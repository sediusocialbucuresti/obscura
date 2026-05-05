use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};
use obscura_b2b::export::export_outputs;
use obscura_b2b::orchestrator::{run_once, PipelineOptions};
use obscura_b2b::seed::write_example_sources;

#[derive(Parser)]
#[command(name = "obscura-b2b", about = "B2B data extraction orchestrator built on Obscura")]
struct Args {
    #[arg(short, long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Init {
        #[arg(long, default_value = "b2b-seeds.json")]
        seeds: PathBuf,
        #[arg(long, default_value = "data/b2b")]
        out: PathBuf,
    },
    Run {
        #[arg(long, default_value = "b2b-seeds.json")]
        seeds: PathBuf,
        #[arg(long, default_value = "data/b2b")]
        out: PathBuf,
        #[arg(long, default_value_t = 10)]
        concurrency: usize,
        #[arg(long, default_value_t = 1000)]
        max_pages: usize,
        #[arg(long, default_value_t = 45)]
        timeout: u64,
        #[arg(long, default_value_t = 500)]
        delay_ms: u64,
        #[arg(long)]
        obey_robots: bool,
        #[arg(long)]
        user_agent: Option<String>,
        #[arg(long)]
        export: bool,
        #[arg(long)]
        include_personal_contacts: bool,
        #[arg(long)]
        r#loop: bool,
        #[arg(long, default_value_t = 86_400)]
        interval_seconds: u64,
    },
    Export {
        #[arg(long, default_value = "data/b2b")]
        out: PathBuf,
        #[arg(long)]
        include_personal_contacts: bool,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let filter = if args.verbose { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(filter)),
        )
        .with_writer(std::io::stderr)
        .init();

    match args.command {
        Command::Init { seeds, out } => {
            write_example_sources(&seeds).await?;
            tokio::fs::create_dir_all(&out).await?;
            println!(
                "Created seed template at {} and output directory {}",
                seeds.display(),
                out.display()
            );
        }
        Command::Run {
            seeds,
            out,
            concurrency,
            max_pages,
            timeout,
            delay_ms,
            obey_robots,
            user_agent,
            export,
            include_personal_contacts,
            r#loop,
            interval_seconds,
        } => {
            loop {
                let summary = run_once(PipelineOptions {
                    seeds_path: seeds.clone(),
                    output_dir: out.clone(),
                    concurrency,
                    max_pages,
                    timeout_secs: timeout,
                    delay_ms,
                    obey_robots,
                    user_agent: user_agent.clone(),
                    export_after_run: export,
                    include_personal_contacts,
                })
                .await?;
                println!("{}", serde_json::to_string_pretty(&summary)?);

                if !r#loop {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(interval_seconds)).await;
            }
        }
        Command::Export {
            out,
            include_personal_contacts,
        } => {
            let count = export_outputs(&out, include_personal_contacts).await?;
            println!("Exported {} profiles from {}", count, out.display());
        }
    }

    Ok(())
}
