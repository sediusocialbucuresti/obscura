use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::Path;

use serde::Serialize;

use crate::models::{CompanyProfile, ContactPoint};
use crate::storage::{
    csv_escape, read_jsonl, slugify, write_json_pretty, write_text, StorageLayout,
};

#[derive(Debug, Clone, Serialize)]
struct DirectoryIndexEntry {
    id: String,
    company_name: String,
    profile_url: String,
    directory_profile: String,
    lei: Option<String>,
    lei_status: Option<String>,
    region: Option<String>,
    country: Option<String>,
    company_type: Option<String>,
    industries: Vec<String>,
    tags: Vec<String>,
    validation_status: String,
    validation_score: u8,
}

#[derive(Debug, Clone, Serialize)]
struct SearchDocument {
    id: String,
    title: String,
    body: String,
    lei: Option<String>,
    region: Option<String>,
    country: Option<String>,
    company_type: Option<String>,
    industries: Vec<String>,
    products: Vec<String>,
    services: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SegmentSummary {
    by_region: BTreeMap<String, Vec<String>>,
    by_country: BTreeMap<String, Vec<String>>,
    by_industry: BTreeMap<String, Vec<String>>,
    by_company_type: BTreeMap<String, Vec<String>>,
    by_source: BTreeMap<String, Vec<String>>,
    by_status: BTreeMap<String, Vec<String>>,
    by_source_basis: BTreeMap<String, Vec<String>>,
}

pub async fn export_outputs(
    root: impl AsRef<Path>,
    include_personal_contacts: bool,
) -> anyhow::Result<usize> {
    let layout = StorageLayout::prepare(root).await?;
    let profiles_raw: Vec<CompanyProfile> = read_jsonl(&layout.profiles_jsonl).await?;
    let mut latest_by_id = BTreeMap::new();
    for profile in profiles_raw {
        latest_by_id.insert(profile.id.clone(), profile);
    }
    let mut profiles = latest_by_id.into_values().collect::<Vec<_>>();
    profiles.sort_by(|a, b| {
        a.company_name
            .to_ascii_lowercase()
            .cmp(&b.company_name.to_ascii_lowercase())
    });
    reset_dir(&layout.companies_dir).await?;

    let mut index = Vec::new();
    let mut search = Vec::new();
    let mut segments = SegmentSummary {
        by_region: BTreeMap::new(),
        by_country: BTreeMap::new(),
        by_industry: BTreeMap::new(),
        by_company_type: BTreeMap::new(),
        by_source: BTreeMap::new(),
        by_status: BTreeMap::new(),
        by_source_basis: BTreeMap::new(),
    };

    for profile in &profiles {
        let file_stem = safe_file_stem(&profile.id);
        let filename = format!("{}.json", file_stem);
        write_json_pretty(layout.companies_dir.join(&filename), profile).await?;

        index.push(DirectoryIndexEntry {
            id: profile.id.clone(),
            company_name: profile.company_name.clone(),
            profile_url: profile.profile_url.clone(),
            directory_profile: format!("companies/{}", filename),
            lei: evidence_value(profile, "lei"),
            lei_status: evidence_value(profile, "gleif_entity_status"),
            region: profile.region.clone(),
            country: profile.country.clone(),
            company_type: profile.company_type.clone(),
            industries: profile.industries.clone(),
            tags: profile.tags.clone(),
            validation_status: profile.validation.status.clone(),
            validation_score: profile.validation.score,
        });

        search.push(SearchDocument {
            id: profile.id.clone(),
            title: profile.company_name.clone(),
            body: search_body(profile),
            lei: evidence_value(profile, "lei"),
            region: profile.region.clone(),
            country: profile.country.clone(),
            company_type: profile.company_type.clone(),
            industries: profile.industries.clone(),
            products: profile
                .products
                .iter()
                .map(|item| item.name.clone())
                .collect(),
            services: profile
                .services
                .iter()
                .map(|item| item.name.clone())
                .collect(),
        });

        push_segment(
            &mut segments.by_region,
            profile.region.as_deref(),
            &profile.id,
        );
        push_segment(
            &mut segments.by_country,
            profile.country.as_deref(),
            &profile.id,
        );
        push_segment(
            &mut segments.by_company_type,
            profile.company_type.as_deref(),
            &profile.id,
        );
        push_segment(
            &mut segments.by_source,
            Some(&profile.source_name),
            &profile.id,
        );
        push_segment(
            &mut segments.by_status,
            Some(&profile.validation.status),
            &profile.id,
        );
        for source_basis in profile
            .validation
            .compliance_flags
            .iter()
            .filter_map(|flag| flag.strip_prefix("source_basis:"))
        {
            push_segment(
                &mut segments.by_source_basis,
                Some(source_basis),
                &profile.id,
            );
        }
        for industry in &profile.industries {
            push_segment(&mut segments.by_industry, Some(industry), &profile.id);
        }
    }

    write_json_pretty(layout.directory_dir.join("index.json"), &index).await?;
    write_json_pretty(layout.directory_dir.join("search.json"), &search).await?;
    write_json_pretty(layout.directory_dir.join("segments.json"), &segments).await?;
    write_json_pretty(layout.mautic_dir.join("segments.json"), &segments).await?;
    write_text(
        layout.mautic_dir.join("contacts.csv"),
        &mautic_contacts_csv(&profiles, include_personal_contacts),
    )
    .await?;
    write_text(
        layout.templates_dir.join("claim-your-profile.md"),
        claim_profile_template(),
    )
    .await?;
    write_static_site(&layout, &profiles).await?;

    Ok(profiles.len())
}

const LIST_PAGE_SIZE: usize = 200;
const SITEMAP_URL_LIMIT: usize = 50_000;

async fn write_static_site(
    layout: &StorageLayout,
    profiles: &[CompanyProfile],
) -> anyhow::Result<()> {
    let site_dir = layout.root.join("site");
    let site_companies_dir = site_dir.join("companies");
    let sitemap_dir = site_dir.join("sitemaps");
    let base_url = site_base_url();
    reset_dir(&site_companies_dir).await?;
    reset_dir(&sitemap_dir).await?;

    write_text(site_dir.join("styles.css"), site_css()).await?;
    write_text(site_dir.join("robots.txt"), &robots_txt(&base_url)).await?;
    write_text(site_dir.join("index.html"), &home_html(profiles, &base_url)).await?;
    write_company_listing_pages(&site_dir, profiles, &base_url).await?;

    for profile in profiles {
        let file_stem = safe_file_stem(&profile.id);
        let page = company_html(profile, &base_url);
        write_text(
            site_companies_dir.join(format!("{}.html", file_stem)),
            &page,
        )
        .await?;
    }

    write_sitemaps(&site_dir, profiles, &base_url).await?;

    Ok(())
}

async fn write_company_listing_pages(
    site_dir: &Path,
    profiles: &[CompanyProfile],
    base_url: &str,
) -> anyhow::Result<()> {
    let page_count = listing_page_count(profiles.len());
    for page_index in 0..page_count {
        let start = page_index * LIST_PAGE_SIZE;
        let end = profiles.len().min(start + LIST_PAGE_SIZE);
        let page = listing_html(
            &profiles[start..end],
            page_index,
            page_count,
            profiles.len(),
            base_url,
        );
        let path = if page_index == 0 {
            site_dir.join("companies").join("index.html")
        } else {
            site_dir
                .join("companies")
                .join(format!("page-{}.html", page_index + 1))
        };
        write_text(path, &page).await?;
    }
    Ok(())
}

fn home_html(profiles: &[CompanyProfile], base_url: &str) -> String {
    let mut by_region = BTreeMap::<String, usize>::new();
    let mut by_country = BTreeMap::<String, usize>::new();
    for profile in profiles {
        *by_region
            .entry(
                profile
                    .region
                    .clone()
                    .unwrap_or_else(|| "Unknown".to_string()),
            )
            .or_default() += 1;
        *by_country
            .entry(
                profile
                    .country
                    .clone()
                    .unwrap_or_else(|| "Unknown".to_string()),
            )
            .or_default() += 1;
    }

    let mut out = String::new();
    out.push_str("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">");
    out.push_str("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">");
    out.push_str("<title>B2B Company Directory</title>");
    out.push_str("<meta name=\"description\" content=\"Search public B2B company profiles across Europe and MENA prepared for review and enrichment.\">");
    let _ = write!(
        out,
        "<link rel=\"canonical\" href=\"{}\">",
        html_attr(base_url)
    );
    out.push_str("<link rel=\"stylesheet\" href=\"styles.css\"></head><body>");
    out.push_str("<header><h1>B2B Company Directory</h1>");
    let _ = write!(
        out,
        "<p>{} public company profiles across Europe and MENA prepared for review and enrichment.</p>",
        profiles.len()
    );
    out.push_str("<nav><a href=\"companies/index.html\">Browse companies</a><a href=\"sitemap-index.xml\">Sitemap</a></nav></header>");
    out.push_str("<main><section><h2>Directory Coverage</h2><div class=\"stats\">");
    for (region, count) in by_region {
        let _ = write!(
            out,
            "<div><strong>{}</strong><span>{} profiles</span></div>",
            html(&region),
            count
        );
    }
    out.push_str("</div></section>");
    out.push_str("<section><h2>Top Countries</h2><div class=\"chips\">");
    let mut countries = by_country.into_iter().collect::<Vec<_>>();
    countries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    for (country, count) in countries.iter().take(40) {
        let _ = write!(
            out,
            "<span>{} <strong>{}</strong></span>",
            html(country),
            count
        );
    }
    out.push_str("</div></section></main></body></html>");
    out
}

fn listing_html(
    profiles: &[CompanyProfile],
    page_index: usize,
    page_count: usize,
    total_count: usize,
    base_url: &str,
) -> String {
    let page_number = page_index + 1;
    let canonical = absolute_site_url(base_url, &listing_page_href(page_index));
    let mut out = String::new();
    out.push_str("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">");
    out.push_str("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">");
    let _ = write!(
        out,
        "<title>B2B Companies - Page {}</title><meta name=\"description\" content=\"Browse page {} of public B2B company profiles across Europe and MENA.\">",
        page_number, page_number
    );
    let _ = write!(
        out,
        "<link rel=\"canonical\" href=\"{}\">",
        html_attr(&canonical)
    );
    if page_index > 0 {
        let prev = absolute_site_url(base_url, &listing_page_href(page_index - 1));
        let _ = write!(out, "<link rel=\"prev\" href=\"{}\">", html_attr(&prev));
    }
    if page_index + 1 < page_count {
        let next = absolute_site_url(base_url, &listing_page_href(page_index + 1));
        let _ = write!(out, "<link rel=\"next\" href=\"{}\">", html_attr(&next));
    }
    out.push_str("<link rel=\"stylesheet\" href=\"../styles.css\"></head><body>");
    out.push_str("<header><a href=\"../index.html\">Directory</a>");
    let _ = write!(
        out,
        "<h1>B2B Companies</h1><p>Page {} of {}. {} total profiles.</p>",
        page_number, page_count, total_count
    );
    out.push_str("<input id=\"search\" type=\"search\" placeholder=\"Filter this page\" aria-label=\"Filter this page\"></header>");
    out.push_str("<main><nav class=\"pager\">");
    if page_index > 0 {
        let _ = write!(
            out,
            "<a href=\"{}\">Previous</a>",
            html_attr(&listing_page_local_href(page_index - 1))
        );
    }
    if page_index + 1 < page_count {
        let _ = write!(
            out,
            "<a href=\"{}\">Next</a>",
            html_attr(&listing_page_local_href(page_index + 1))
        );
    }
    out.push_str("</nav><section class=\"grid\" id=\"companies\">");

    for profile in profiles {
        push_company_card(
            &mut out,
            profile,
            &format!("{}.html", safe_file_stem(&profile.id)),
        );
    }

    out.push_str("</section></main>");
    out.push_str(site_search_script());
    out.push_str("</body></html>");
    out
}

fn push_company_card(out: &mut String, profile: &CompanyProfile, href: &str) {
    let description = meta_description(profile);
    let country = profile.country.as_deref().unwrap_or("Unknown country");
    let company_type = profile.company_type.as_deref().unwrap_or("B2B entity");
    let lei = evidence_value(profile, "lei");
    let industries = if profile.industries.is_empty() {
        "Unclassified".to_string()
    } else {
        profile.industries.join(", ")
    };
    let lei_badge = if lei.is_some() {
        "<span class=\"badge\">LEI verified</span>"
    } else {
        ""
    };
    let _ = write!(
        out,
        "<article class=\"card\" data-search=\"{}\"><a href=\"{}\"><h2>{}</h2></a><p class=\"meta\">{} · {} · {}</p>{}<p>{}</p></article>",
        html_attr(&format!(
            "{} {} {} {} {} {}",
            profile.company_name,
            country,
            company_type,
            industries,
            description,
            lei.as_deref().unwrap_or("")
        )),
        html_attr(href),
        html(&profile.company_name),
        html(country),
        html(company_type),
        html(&industries),
        lei_badge,
        html(&description)
    );
}

fn company_html(profile: &CompanyProfile, base_url: &str) -> String {
    let canonical = absolute_site_url(
        base_url,
        &format!("companies/{}.html", safe_file_stem(&profile.id)),
    );
    let description = meta_description(profile);
    let mut out = String::new();
    out.push_str("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">");
    out.push_str("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">");
    let _ = write!(
        out,
        "<title>{}</title><meta name=\"description\" content=\"{}\">",
        html(&profile.company_name),
        html_attr(&description)
    );
    let _ = write!(
        out,
        "<link rel=\"canonical\" href=\"{}\"><meta name=\"robots\" content=\"{}\">",
        html_attr(&canonical),
        html_attr(profile_robots(profile))
    );
    out.push_str("<script type=\"application/ld+json\">");
    out.push_str(&organization_json_ld(profile, &canonical));
    out.push_str("</script>");
    out.push_str("<link rel=\"stylesheet\" href=\"../styles.css\"></head><body>");
    out.push_str("<header class=\"profile-header\"><a href=\"index.html\">Companies</a><a href=\"../index.html\">Directory</a>");
    let _ = write!(out, "<h1>{}</h1>", html(&profile.company_name));
    let _ = write!(out, "<p>{}</p>", html(&description));
    out.push_str("</header><main class=\"profile\">");

    out.push_str("<section><h2>Company</h2><dl>");
    field(&mut out, "Region", profile.region.as_deref());
    field(&mut out, "Country", profile.country.as_deref());
    field(&mut out, "Type", profile.company_type.as_deref());
    field(&mut out, "Domain", profile.canonical_domain.as_deref());
    field(&mut out, "Source", Some(&profile.source_name));
    field(&mut out, "Source URL", Some(&profile.profile_url));
    out.push_str("</dl></section>");

    lei_verification_section(&mut out, profile);
    list_section(&mut out, "Industries", &profile.industries);
    list_section(&mut out, "Specializations", &profile.specializations);
    list_section(&mut out, "Addresses", &profile.addresses);

    out.push_str("<section><h2>Contacts</h2>");
    if profile.contacts.emails.is_empty()
        && profile.contacts.phones.is_empty()
        && profile.contacts.websites.is_empty()
    {
        out.push_str("<p>No public contact points captured yet.</p>");
    } else {
        out.push_str("<ul>");
        for email in &profile.contacts.emails {
            let _ = write!(out, "<li>{}</li>", html(&email.value));
        }
        for phone in &profile.contacts.phones {
            let _ = write!(out, "<li>{}</li>", html(&phone.value));
        }
        for website in &profile.contacts.websites {
            let _ = write!(
                out,
                "<li><a href=\"{}\">{}</a></li>",
                html_attr(website),
                html(website)
            );
        }
        out.push_str("</ul>");
    }
    out.push_str("</section>");

    catalog_section(&mut out, "Products", &profile.products);
    catalog_section(&mut out, "Services", &profile.services);

    out.push_str("<section><h2>Validation</h2><dl>");
    field(&mut out, "Status", Some(&profile.validation.status));
    let score = profile.validation.score.to_string();
    field(&mut out, "Score", Some(&score));
    out.push_str("</dl></section>");

    out.push_str("</main></body></html>");
    out
}

async fn write_sitemaps(
    site_dir: &Path,
    profiles: &[CompanyProfile],
    base_url: &str,
) -> anyhow::Result<()> {
    let mut urls = Vec::with_capacity(profiles.len() + listing_page_count(profiles.len()) + 1);
    urls.push(base_url.to_string());
    for page_index in 0..listing_page_count(profiles.len()) {
        urls.push(absolute_site_url(base_url, &listing_page_href(page_index)));
    }
    for profile in profiles
        .iter()
        .filter(|profile| is_indexable_profile(profile))
    {
        urls.push(absolute_site_url(
            base_url,
            &format!("companies/{}.html", safe_file_stem(&profile.id)),
        ));
    }

    let sitemap_dir = site_dir.join("sitemaps");
    let mut sitemap_files = Vec::new();
    for (idx, chunk) in urls.chunks(SITEMAP_URL_LIMIT).enumerate() {
        let filename = format!("sitemap-{:04}.xml", idx + 1);
        let path = sitemap_dir.join(&filename);
        write_text(path, &sitemap_xml(chunk)).await?;
        sitemap_files.push(filename);
    }

    write_text(
        site_dir.join("sitemap-index.xml"),
        &sitemap_index_xml(&sitemap_files, base_url),
    )
    .await?;

    Ok(())
}

fn field(out: &mut String, label: &str, value: Option<&str>) {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        let _ = write!(
            out,
            "<dt>{}</dt><dd>{}</dd>",
            html(label),
            linked_text(value)
        );
    }
}

fn lei_verification_section(out: &mut String, profile: &CompanyProfile) {
    if evidence_value(profile, "lei").is_none() {
        return;
    }

    out.push_str("<section><h2>LEI Verification</h2><dl>");
    for (label, field_name) in [
        ("LEI", "lei"),
        ("Legal name", "gleif_legal_name"),
        ("Entity status", "gleif_entity_status"),
        ("Registration status", "gleif_registration_status"),
        ("Entity category", "gleif_entity_category"),
        ("Legal form", "gleif_legal_form"),
        ("Registered as", "gleif_registered_as"),
        ("Registration authority", "gleif_registered_at"),
        ("Jurisdiction", "gleif_jurisdiction"),
        ("Creation date", "gleif_creation_date"),
        ("Initial registration", "gleif_initial_registration_date"),
        ("Last update", "gleif_last_update_date"),
        ("Next renewal", "gleif_next_renewal_date"),
        ("Corroboration", "gleif_corroboration_level"),
        ("Conformity", "gleif_conformity_flag"),
        ("Validated at", "gleif_validated_at"),
        ("Validated as", "gleif_validated_as"),
        ("Associated entity", "gleif_associated_entity"),
        ("Direct parent", "gleif_direct_parent"),
        ("Ultimate parent", "gleif_ultimate_parent"),
        (
            "Direct parent relationship",
            "gleif_direct_parent_relationship_url",
        ),
        (
            "Ultimate parent relationship",
            "gleif_ultimate_parent_relationship_url",
        ),
        (
            "Direct parent reporting exception",
            "gleif_direct_parent_reporting_exception_url",
        ),
        (
            "Ultimate parent reporting exception",
            "gleif_ultimate_parent_reporting_exception_url",
        ),
    ] {
        let value = evidence_value(profile, field_name);
        field(out, label, value.as_deref());
    }
    out.push_str("</dl></section>");
}

fn list_section(out: &mut String, title: &str, items: &[String]) {
    out.push_str("<section>");
    let _ = write!(out, "<h2>{}</h2>", html(title));
    if items.is_empty() {
        out.push_str("<p>Not captured yet.</p>");
    } else {
        out.push_str("<ul>");
        for item in items {
            let _ = write!(out, "<li>{}</li>", html(item));
        }
        out.push_str("</ul>");
    }
    out.push_str("</section>");
}

fn catalog_section(out: &mut String, title: &str, items: &[crate::models::CatalogItem]) {
    out.push_str("<section>");
    let _ = write!(out, "<h2>{}</h2>", html(title));
    if items.is_empty() {
        out.push_str("<p>Not captured yet.</p>");
    } else {
        out.push_str("<ul>");
        for item in items {
            if let Some(url) = &item.url {
                let _ = write!(
                    out,
                    "<li><a href=\"{}\">{}</a></li>",
                    html_attr(url),
                    html(&item.name)
                );
            } else {
                let _ = write!(out, "<li>{}</li>", html(&item.name));
            }
        }
        out.push_str("</ul>");
    }
    out.push_str("</section>");
}

fn listing_page_count(profile_count: usize) -> usize {
    profile_count.div_ceil(LIST_PAGE_SIZE).max(1)
}

fn listing_page_href(page_index: usize) -> String {
    if page_index == 0 {
        "companies/index.html".to_string()
    } else {
        format!("companies/page-{}.html", page_index + 1)
    }
}

fn listing_page_local_href(page_index: usize) -> String {
    if page_index == 0 {
        "index.html".to_string()
    } else {
        format!("page-{}.html", page_index + 1)
    }
}

fn site_base_url() -> String {
    std::env::var("OBSCURA_B2B_SITE_BASE_URL")
        .ok()
        .map(|value| value.trim().trim_end_matches('/').to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "http://127.0.0.1:8080".to_string())
}

fn absolute_site_url(base_url: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

fn meta_description(profile: &CompanyProfile) -> String {
    let description = profile.description.as_deref().unwrap_or("").trim();
    let fallback = format!(
        "{} B2B company profile{}{}.",
        profile.company_name,
        profile
            .country
            .as_ref()
            .map(|country| format!(" in {country}"))
            .unwrap_or_default(),
        profile
            .company_type
            .as_ref()
            .map(|company_type| format!(" classified as {company_type}"))
            .unwrap_or_default()
    );
    truncate_text(if description.is_empty() {
        &fallback
    } else {
        description
    })
}

fn truncate_text(value: &str) -> String {
    const MAX_CHARS: usize = 220;
    let mut out = value.chars().take(MAX_CHARS).collect::<String>();
    if value.chars().count() > MAX_CHARS {
        out.push_str("...");
    }
    out
}

fn is_indexable_profile(profile: &CompanyProfile) -> bool {
    !matches!(
        profile.validation.status.as_str(),
        "hold" | "blocked" | "rejected"
    ) && profile.validation.score >= 40
}

fn profile_robots(profile: &CompanyProfile) -> &'static str {
    if is_indexable_profile(profile) {
        "index,follow"
    } else {
        "noindex,nofollow"
    }
}

fn organization_json_ld(profile: &CompanyProfile, canonical: &str) -> String {
    #[derive(Serialize)]
    struct OrganizationJsonLd<'a> {
        #[serde(rename = "@context")]
        context: &'static str,
        #[serde(rename = "@type")]
        kind: &'static str,
        #[serde(rename = "@id")]
        id: &'a str,
        name: &'a str,
        description: String,
        url: String,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        address: Vec<&'a String>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        same_as: Vec<&'a String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        identifier: Option<PropertyValueJsonLd>,
    }

    #[derive(Serialize)]
    struct PropertyValueJsonLd {
        #[serde(rename = "@type")]
        kind: &'static str,
        #[serde(rename = "propertyID")]
        property_id: &'static str,
        value: String,
    }

    let identifier = evidence_value(profile, "lei").map(|lei| PropertyValueJsonLd {
        kind: "PropertyValue",
        property_id: "LEI",
        value: lei,
    });

    let data = OrganizationJsonLd {
        context: "https://schema.org",
        kind: "Organization",
        id: canonical,
        name: &profile.company_name,
        description: meta_description(profile),
        url: profile
            .canonical_domain
            .clone()
            .unwrap_or_else(|| canonical.to_string()),
        address: profile.addresses.iter().collect(),
        same_as: profile.contacts.social_links.iter().collect(),
        identifier,
    };
    serde_json::to_string(&data).unwrap_or_else(|_| "{}".to_string())
}

fn sitemap_xml(urls: &[String]) -> String {
    let mut out = String::new();
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    out.push_str("<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n");
    for url in urls {
        let _ = writeln!(out, "  <url><loc>{}</loc></url>", xml(url));
    }
    out.push_str("</urlset>\n");
    out
}

fn sitemap_index_xml(files: &[String], base_url: &str) -> String {
    let mut out = String::new();
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    out.push_str("<sitemapindex xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n");
    for file in files {
        let url = absolute_site_url(base_url, &format!("sitemaps/{file}"));
        let _ = writeln!(out, "  <sitemap><loc>{}</loc></sitemap>", xml(&url));
    }
    out.push_str("</sitemapindex>\n");
    out
}

fn robots_txt(base_url: &str) -> String {
    format!(
        "User-agent: *\nAllow: /\nDisallow: /directory/\nDisallow: /mautic/\nSitemap: {}/sitemap-index.xml\n",
        base_url.trim_end_matches('/')
    )
}

fn html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn html_attr(value: &str) -> String {
    html(value).replace('"', "&quot;")
}

fn linked_text(value: &str) -> String {
    if value.starts_with("https://") || value.starts_with("http://") {
        format!(
            "<a href=\"{}\" rel=\"nofollow noopener\">{}</a>",
            html_attr(value),
            html(value)
        )
    } else {
        html(value)
    }
}

fn xml(value: &str) -> String {
    html_attr(value)
}

fn site_css() -> &'static str {
    r#"body{margin:0;font-family:Inter,Arial,sans-serif;background:#f6f7f9;color:#18202a}header{padding:24px 32px;background:#fff;border-bottom:1px solid #d9dde3}h1{margin:0 0 8px;font-size:28px}h2{font-size:18px;margin:0 0 8px}p{line-height:1.45}main{padding:24px 32px}nav{display:flex;flex-wrap:wrap;gap:10px;margin-top:14px}.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:14px}.card,section{background:#fff;border:1px solid #d9dde3;border-radius:8px;padding:16px}.card a,nav a,.profile-header a{color:#0b5cab;text-decoration:none}.meta{color:#5b6675;font-size:13px}.badge{display:inline-block;margin:2px 0 6px;padding:4px 7px;border:1px solid #9eb8d7;border-radius:999px;background:#eef6ff;color:#194b7d;font-size:12px;font-weight:700}.profile{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:16px}.profile-header{display:flex;flex-wrap:wrap;gap:10px;align-items:baseline}.profile-header h1,.profile-header p{flex-basis:100%}dl{display:grid;grid-template-columns:140px 1fr;gap:8px 12px}dt{font-weight:700;color:#394454}dd{margin:0;overflow-wrap:anywhere}input[type=search]{width:100%;max-width:560px;margin-top:12px;padding:10px 12px;border:1px solid #bdc5d1;border-radius:6px;font-size:15px}.pager{justify-content:space-between;margin:0 0 18px}.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px}.stats div{border:1px solid #d9dde3;border-radius:8px;padding:12px}.stats strong{display:block;font-size:24px}.chips{display:flex;flex-wrap:wrap;gap:8px}.chips span{background:#eef1f5;border:1px solid #d9dde3;border-radius:999px;padding:6px 10px}@media(max-width:700px){header,main{padding:18px}.grid{grid-template-columns:1fr}dl{grid-template-columns:1fr}}"#
}

fn site_search_script() -> &'static str {
    r#"<script>const s=document.getElementById('search');const cards=[...document.querySelectorAll('.card')];if(s){s.addEventListener('input',()=>{const q=s.value.toLowerCase().trim();for(const c of cards){c.style.display=!q||c.dataset.search.toLowerCase().includes(q)?'':'none';}});}</script>"#
}

async fn reset_dir(path: &Path) -> anyhow::Result<()> {
    if path.exists() {
        tokio::fs::remove_dir_all(path).await?;
    }
    tokio::fs::create_dir_all(path).await?;
    Ok(())
}

fn safe_file_stem(value: &str) -> String {
    const MAX_STEM_BYTES: usize = 120;

    let slug = slugify(value);
    if slug.len() <= MAX_STEM_BYTES {
        return slug;
    }

    let hash = fnv1a64(value.as_bytes());
    let suffix = format!("-{hash:016x}");
    let prefix_len = MAX_STEM_BYTES - suffix.len();
    let prefix = slug
        .char_indices()
        .take_while(|(idx, ch)| idx + ch.len_utf8() <= prefix_len)
        .map(|(_, ch)| ch)
        .collect::<String>()
        .trim_matches('-')
        .to_string();

    format!("{prefix}{suffix}")
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn search_body(profile: &CompanyProfile) -> String {
    let mut parts = Vec::new();
    if let Some(description) = &profile.description {
        parts.push(description.clone());
    }
    parts.extend(profile.industries.clone());
    parts.extend(profile.specializations.clone());
    parts.extend(profile.products.iter().map(|item| item.name.clone()));
    parts.extend(profile.services.iter().map(|item| item.name.clone()));
    parts.extend(profile.evidence.iter().map(|item| item.value.clone()));
    parts.join(" ")
}

fn evidence_value(profile: &CompanyProfile, field: &str) -> Option<String> {
    profile
        .evidence
        .iter()
        .find(|item| item.field == field && !item.value.trim().is_empty())
        .map(|item| item.value.trim().to_string())
}

fn push_segment(map: &mut BTreeMap<String, Vec<String>>, key: Option<&str>, id: &str) {
    let Some(key) = key.map(str::trim).filter(|key| !key.is_empty()) else {
        return;
    };
    let key = slugify(key);
    let ids = map.entry(key).or_default();
    if !ids.iter().any(|existing| existing == id) {
        ids.push(id.to_string());
    }
}

fn mautic_contacts_csv(profiles: &[CompanyProfile], include_personal_contacts: bool) -> String {
    let mut out = String::new();
    out.push_str("email,firstname,lastname,company,phone,website,region,country,industry,company_type,tags,profile_url,claim_url\n");

    let mut seen = BTreeSet::new();
    for profile in profiles.iter().filter(|profile| is_campaign_ready(profile)) {
        for email in profile
            .contacts
            .emails
            .iter()
            .filter(|email| should_export_email(email, include_personal_contacts))
        {
            if !seen.insert(email.value.clone()) {
                continue;
            }

            let (firstname, lastname) = if email.personal {
                split_email_name(&email.value)
            } else {
                (String::new(), String::new())
            };
            let phone = profile
                .contacts
                .phones
                .first()
                .map(|phone| phone.value.as_str())
                .unwrap_or("");
            let website = profile
                .canonical_domain
                .as_deref()
                .unwrap_or(profile.profile_url.as_str());
            let industry = profile.industries.join("|");
            let tags = mautic_tags(profile);
            let claim_url = format!("{{{{profile_claim_base_url}}}}/claim/{}", profile.id);

            let row = [
                email.value.as_str(),
                firstname.as_str(),
                lastname.as_str(),
                profile.company_name.as_str(),
                phone,
                website,
                profile.region.as_deref().unwrap_or(""),
                profile.country.as_deref().unwrap_or(""),
                industry.as_str(),
                profile.company_type.as_deref().unwrap_or(""),
                tags.as_str(),
                profile.profile_url.as_str(),
                claim_url.as_str(),
            ]
            .iter()
            .map(|value| csv_escape(value))
            .collect::<Vec<_>>()
            .join(",");
            out.push_str(&row);
            out.push('\n');
        }
    }

    out
}

fn is_campaign_ready(profile: &CompanyProfile) -> bool {
    profile.validation.status == "ready"
        && !profile
            .validation
            .compliance_flags
            .iter()
            .any(|flag| flag == "mautic_export_not_campaign_ready")
}

fn should_export_email(email: &ContactPoint, include_personal_contacts: bool) -> bool {
    !email.personal || include_personal_contacts
}

fn split_email_name(email: &str) -> (String, String) {
    let local = email.split('@').next().unwrap_or("");
    let parts = local
        .split(['.', '_', '-'])
        .filter(|part| !part.is_empty())
        .map(title_case)
        .collect::<Vec<_>>();
    match parts.as_slice() {
        [first, last, ..] => (first.clone(), last.clone()),
        [first] => (first.clone(), String::new()),
        _ => (String::new(), String::new()),
    }
}

fn title_case(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => format!(
            "{}{}",
            first.to_ascii_uppercase(),
            chars.as_str().to_ascii_lowercase()
        ),
        None => String::new(),
    }
}

fn mautic_tags(profile: &CompanyProfile) -> String {
    let mut tags = BTreeSet::new();
    tags.insert("claim-profile".to_string());
    if let Some(region) = &profile.region {
        tags.insert(format!("region:{}", slugify(region)));
    }
    if let Some(country) = &profile.country {
        tags.insert(format!("country:{}", slugify(country)));
    }
    if let Some(company_type) = &profile.company_type {
        tags.insert(format!("type:{}", slugify(company_type)));
    }
    tags.insert(format!("status:{}", slugify(&profile.validation.status)));
    tags.insert(format!("source:{}", slugify(&profile.source_name)));
    for industry in &profile.industries {
        tags.insert(format!("industry:{}", slugify(industry)));
    }
    for tag in &profile.tags {
        tags.insert(slugify(tag));
    }
    tags.into_iter().collect::<Vec<_>>().join("|")
}

fn claim_profile_template() -> &'static str {
    r#"Subject: Confirm your company profile on {{directory_name}}

Hello,

We maintain {{directory_name}}, a B2B directory for manufacturers, wholesalers, distributors, and related suppliers.

We found publicly listed business information for {{company}} and prepared this draft profile:

{{profile_url}}

Please use this link to claim, update, or request removal of the profile:

{{claim_url}}

This message is sent to a public business contact address for profile verification. If you are not the right contact, please forward it to the appropriate team or use the removal option above.

Regards,
{{sender_name}}
"#
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skips_personal_contacts_by_default() {
        let email = ContactPoint {
            value: "jane@example.com".to_string(),
            kind: "personal_email".to_string(),
            source_url: "https://example.com".to_string(),
            confidence: 1.0,
            personal: true,
        };
        assert!(!should_export_email(&email, false));
        assert!(should_export_email(&email, true));
    }

    #[test]
    fn safe_file_stem_bounds_long_ids() {
        let id = "very-long-company-name-".repeat(30);
        let stem = safe_file_stem(&id);

        assert!(stem.len() <= 120);
        assert!(stem.starts_with("very-long-company-name"));
        assert!(stem.rsplit_once('-').is_some());
    }
}
