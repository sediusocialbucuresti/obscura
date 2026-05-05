use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use obscura_browser::{lifecycle::WaitUntil, BrowserContext, Page};
use serde::Serialize;
use tokio::task::LocalSet;
use url::Url;

use crate::export::export_outputs;
use crate::extract::{discover_company_links, extract_company_profile, page_html};
use crate::models::{DirectorySource, ScrapeJob};
use crate::seed::load_sources;
use crate::storage::{host_from_url, now_epoch, slugify, JsonlWriter, StorageLayout};
use crate::validator::validate_profile;

#[derive(Debug, Clone)]
pub struct PipelineOptions {
    pub seeds_path: PathBuf,
    pub output_dir: PathBuf,
    pub concurrency: usize,
    pub max_pages: usize,
    pub timeout_secs: u64,
    pub delay_ms: u64,
    pub obey_robots: bool,
    pub user_agent: Option<String>,
    pub export_after_run: bool,
    pub include_personal_contacts: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RunSummary {
    pub sources: usize,
    pub jobs_planned: usize,
    pub jobs_completed: usize,
    pub jobs_failed: usize,
    pub profiles_exported: usize,
    pub output_dir: String,
}

#[derive(Clone)]
struct DomainLimiter {
    delay: Duration,
    last_seen: Rc<RefCell<HashMap<String, Instant>>>,
}

impl DomainLimiter {
    fn new(delay: Duration) -> Self {
        Self {
            delay,
            last_seen: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    async fn wait_turn(&self, url: &str) {
        if self.delay.is_zero() {
            return;
        }
        let host = host_from_url(url).unwrap_or_else(|| "unknown".to_string());
        let sleep_for = {
            let mut last_seen = self.last_seen.borrow_mut();
            let now = Instant::now();
            let sleep_for = last_seen
                .get(&host)
                .and_then(|last| self.delay.checked_sub(now.saturating_duration_since(*last)));
            last_seen.insert(host, now + sleep_for.unwrap_or_default());
            sleep_for
        };
        if let Some(sleep_for) = sleep_for {
            tokio::time::sleep(sleep_for).await;
        }
    }
}

#[derive(Default)]
struct WorkerStats {
    completed: usize,
    failed: usize,
}

pub async fn run_once(options: PipelineOptions) -> anyhow::Result<RunSummary> {
    let local = LocalSet::new();
    local.run_until(run_once_local(options)).await
}

async fn run_once_local(options: PipelineOptions) -> anyhow::Result<RunSummary> {
    let layout = StorageLayout::prepare(&options.output_dir).await?;
    let sources = load_sources(&options.seeds_path).await?;
    let jobs = plan_jobs(&sources, &options, &layout).await?;
    let jobs_planned = jobs.len();

    let jobs_writer = Rc::new(JsonlWriter::create(&layout.jobs_jsonl).await?);
    let profiles_writer = Rc::new(JsonlWriter::create(&layout.profiles_jsonl).await?);
    let attempts_writer = Rc::new(JsonlWriter::create(&layout.attempts_jsonl).await?);

    for job in &jobs {
        jobs_writer.append(job).await?;
    }

    let queue = Rc::new(tokio::sync::Mutex::new(VecDeque::from(jobs)));
    let limiter = DomainLimiter::new(Duration::from_millis(options.delay_ms));
    let worker_count = options.concurrency.max(1);
    let mut handles = Vec::new();

    for worker_id in 0..worker_count {
        let queue = queue.clone();
        let limiter = limiter.clone();
        let profiles_writer = profiles_writer.clone();
        let attempts_writer = attempts_writer.clone();
        let layout = layout.clone_for_worker();
        let options = options.clone();

        handles.push(tokio::task::spawn_local(async move {
            let mut stats = WorkerStats::default();
            loop {
                let job = {
                    let mut queue = queue.lock().await;
                    queue.pop_front()
                };
                let Some(job) = job else {
                    break;
                };

                limiter.wait_turn(&job.url).await;
                let attempt =
                    process_job(worker_id, &job, &options, &layout, profiles_writer.as_ref()).await;
                match attempt {
                    Ok(()) => {
                        stats.completed += 1;
                        let record = AttemptRecord::success(worker_id, &job);
                        let _ = attempts_writer.append(&record).await;
                    }
                    Err(error) => {
                        stats.failed += 1;
                        let error_message = error.to_string();
                        let record = AttemptRecord::failure(worker_id, &job, &error_message);
                        let _ = attempts_writer.append(&record).await;
                    }
                }
            }
            stats
        }));
    }

    let mut summary = RunSummary {
        sources: sources.len(),
        jobs_planned,
        output_dir: options.output_dir.display().to_string(),
        ..RunSummary::default()
    };

    for handle in handles {
        let stats = handle.await?;
        summary.jobs_completed += stats.completed;
        summary.jobs_failed += stats.failed;
    }

    if options.export_after_run {
        summary.profiles_exported =
            export_outputs(&options.output_dir, options.include_personal_contacts).await?;
    }

    Ok(summary)
}

async fn plan_jobs(
    sources: &[DirectorySource],
    options: &PipelineOptions,
    layout: &StorageLayout,
) -> anyhow::Result<Vec<ScrapeJob>> {
    let mut jobs = Vec::new();
    let mut seen = BTreeSet::new();

    for source in sources {
        if jobs.len() >= options.max_pages {
            break;
        }
        let remaining = options.max_pages.saturating_sub(jobs.len());
        let max_for_source = source.max_pages.min(remaining).max(1);
        let source_type = source.source_type.to_ascii_lowercase();

        let mut discovered_for_source = 0usize;
        if source_type == "directory" {
            let discovered =
                match discover_source_jobs(source, options, layout, max_for_source).await {
                    Ok(jobs) => jobs,
                    Err(error) => {
                        tracing::warn!("source discovery failed for {}: {}", source.url, error);
                        Vec::new()
                    }
                };
            for job in discovered {
                if jobs.len() >= options.max_pages {
                    break;
                }
                if seen.insert(job.url.clone()) {
                    discovered_for_source += 1;
                    jobs.push(job);
                }
            }
        }

        if (source_type != "directory" || discovered_for_source == 0)
            && seen.insert(source.url.clone())
        {
            jobs.push(source_job(source, &source.url, "company_profile"));
        }
    }

    Ok(jobs)
}

async fn discover_source_jobs(
    source: &DirectorySource,
    options: &PipelineOptions,
    layout: &StorageLayout,
    limit: usize,
) -> anyhow::Result<Vec<ScrapeJob>> {
    let mut context = BrowserContext::with_full_options(
        format!("b2b-discovery-{}", slugify(&source.name)),
        None,
        false,
        options.user_agent.clone(),
    );
    context.obey_robots = options.obey_robots;
    let context = Arc::new(context);
    let mut page = Page::new(format!("discovery-{}", slugify(&source.name)), context);

    tokio::time::timeout(
        Duration::from_secs(options.timeout_secs),
        page.navigate_with_wait(&source.url, WaitUntil::DomContentLoaded),
    )
    .await
    .with_context(|| format!("discovery timed out for {}", source.url))?
    .with_context(|| format!("discovery navigation failed for {}", source.url))?;

    if let Some(html) = page_html(&page) {
        let filename = format!("source-{}.html", slugify(&source.name));
        tokio::fs::write(layout.raw_pages_dir.join(filename), html).await?;
    }

    let links = discover_company_links(&page, &source.url, &source.allowed_domains, limit);
    let jobs = links
        .into_iter()
        .map(|url| source_job(source, &url, "company_profile"))
        .collect::<Vec<_>>();

    Ok(jobs)
}

async fn process_job(
    worker_id: usize,
    job: &ScrapeJob,
    options: &PipelineOptions,
    layout: &StorageLayout,
    profiles_writer: &JsonlWriter,
) -> anyhow::Result<()> {
    let mut context = BrowserContext::with_full_options(
        format!("b2b-worker-{}", worker_id),
        None,
        false,
        options.user_agent.clone(),
    );
    context.obey_robots = options.obey_robots;
    let context = Arc::new(context);
    let mut page = Page::new(
        format!("b2b-page-{}-{}", worker_id, slugify(&job.id)),
        context,
    );

    tokio::time::timeout(
        Duration::from_secs(options.timeout_secs),
        page.navigate_with_wait(&job.url, WaitUntil::DomContentLoaded),
    )
    .await
    .with_context(|| format!("timed out navigating to {}", job.url))?
    .with_context(|| format!("failed to navigate to {}", job.url))?;

    let mut profile = extract_company_profile(&page, job);
    profile.validation = validate_profile(&profile, options.obey_robots, &job.compliance_basis);

    if let Some(html) = page_html(&page) {
        tokio::fs::write(
            layout.raw_pages_dir.join(format!("{}.html", profile.id)),
            html,
        )
        .await?;
    }

    profiles_writer.append(&profile).await?;
    Ok(())
}

fn source_job(source: &DirectorySource, url: &str, job_type: &str) -> ScrapeJob {
    let now = now_epoch();
    let id_basis = host_from_url(url).unwrap_or_else(|| url.to_string());
    ScrapeJob {
        id: slugify(&format!("{} {}", source.name, id_basis)),
        source_name: source.name.clone(),
        source_url: source.url.clone(),
        url: normalize_url(url),
        job_type: job_type.to_string(),
        region: source.region.clone(),
        country: source.country.clone(),
        industry: source.industry.clone(),
        company_type: source.company_type.clone(),
        tags: source.tags.clone(),
        compliance_basis: source.compliance_basis.clone(),
        scheduled_at_epoch: now,
        refresh_interval_days: source.refresh_interval_days,
    }
}

fn normalize_url(value: &str) -> String {
    Url::parse(value)
        .map(|mut url| {
            url.set_fragment(None);
            url.to_string()
        })
        .unwrap_or_else(|_| value.to_string())
}

#[derive(Debug, Serialize)]
struct AttemptRecord<'a> {
    worker_id: usize,
    job_id: &'a str,
    url: &'a str,
    status: &'a str,
    error: Option<&'a str>,
    attempted_at_epoch: u64,
}

impl<'a> AttemptRecord<'a> {
    fn success(worker_id: usize, job: &'a ScrapeJob) -> Self {
        Self {
            worker_id,
            job_id: &job.id,
            url: &job.url,
            status: "success",
            error: None,
            attempted_at_epoch: now_epoch(),
        }
    }

    fn failure(worker_id: usize, job: &'a ScrapeJob, error: &'a str) -> Self {
        Self {
            worker_id,
            job_id: &job.id,
            url: &job.url,
            status: "failure",
            error: Some(error),
            attempted_at_epoch: now_epoch(),
        }
    }
}

trait CloneForWorker {
    fn clone_for_worker(&self) -> StorageLayout;
}

impl CloneForWorker for StorageLayout {
    fn clone_for_worker(&self) -> StorageLayout {
        StorageLayout {
            root: self.root.clone(),
            raw_pages_dir: self.raw_pages_dir.clone(),
            directory_dir: self.directory_dir.clone(),
            companies_dir: self.companies_dir.clone(),
            mautic_dir: self.mautic_dir.clone(),
            templates_dir: self.templates_dir.clone(),
            jobs_jsonl: self.jobs_jsonl.clone(),
            profiles_jsonl: self.profiles_jsonl.clone(),
            attempts_jsonl: self.attempts_jsonl.clone(),
        }
    }
}
