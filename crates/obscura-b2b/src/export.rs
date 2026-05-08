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

#[derive(Debug, Clone, Default)]
struct FilterOptions {
    countries: BTreeMap<String, String>,
    industries: BTreeMap<String, String>,
    product_categories: BTreeMap<String, String>,
    sources: BTreeMap<String, String>,
    company_types: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize)]
struct StaticCompanySearchRecord {
    title: String,
    url: String,
    description: String,
    country: String,
    country_token: String,
    company_type: String,
    company_type_token: String,
    industries: String,
    industry_tokens: Vec<String>,
    product_category_tokens: Vec<String>,
    source: String,
    source_token: String,
    image_url: Option<String>,
    lei: Option<String>,
    has_website: bool,
    has_email: bool,
    has_phone: bool,
    has_products: bool,
    has_services: bool,
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
    let mut profiles = latest_by_id
        .into_values()
        .filter(is_displayable_profile)
        .collect::<Vec<_>>();
    profiles.sort_by_key(profile_sort_key);
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

    let write_company_json = !skip_company_json_files();
    for profile in &profiles {
        let file_stem = safe_file_stem(&profile.id);
        let filename = format!("{}.json", file_stem);
        if write_company_json {
            write_json_pretty(layout.companies_dir.join(&filename), profile).await?;
        }

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
    write_company_search_data(&site_companies_dir, profiles).await?;
    let seo_segment_hrefs =
        write_seo_segment_pages(&site_companies_dir, profiles, &base_url).await?;

    for profile in profiles {
        let file_stem = safe_file_stem(&profile.id);
        let page = company_html(profile, &base_url);
        write_text(
            site_companies_dir.join(format!("{}.html", file_stem)),
            &page,
        )
        .await?;
    }

    write_sitemaps(&site_dir, profiles, &base_url, &seo_segment_hrefs).await?;

    Ok(())
}

async fn write_company_listing_pages(
    site_dir: &Path,
    profiles: &[CompanyProfile],
    base_url: &str,
) -> anyhow::Result<()> {
    let page_count = listing_page_count(profiles.len());
    let filters = filter_options(profiles);
    for page_index in 0..page_count {
        let start = page_index * LIST_PAGE_SIZE;
        let end = profiles.len().min(start + LIST_PAGE_SIZE);
        let page = listing_html(
            &profiles[start..end],
            page_index,
            page_count,
            profiles.len(),
            base_url,
            &filters,
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

async fn write_company_search_data(
    site_companies_dir: &Path,
    profiles: &[CompanyProfile],
) -> anyhow::Result<()> {
    let records = profiles
        .iter()
        .map(static_search_record)
        .collect::<Vec<_>>();
    let json = serde_json::to_string(&records)?;
    write_text(site_companies_dir.join("search-data.json"), &json).await
}

async fn write_seo_segment_pages(
    site_companies_dir: &Path,
    profiles: &[CompanyProfile],
    base_url: &str,
) -> anyhow::Result<Vec<String>> {
    let mut hrefs = Vec::new();
    let mut countries = BTreeMap::<String, Vec<&CompanyProfile>>::new();
    for profile in profiles {
        if let Some(country) = profile
            .country
            .as_deref()
            .filter(|value| !value.trim().is_empty())
        {
            countries
                .entry(clean_public_text(country))
                .or_default()
                .push(profile);
        }
    }

    for (country, mut country_profiles) in countries {
        country_profiles.sort_by_key(|profile| profile_sort_key(profile));
        let filename = format!("country-{}.html", slugify(&country));
        let href = format!("companies/{filename}");
        let title = format!("B2B Companies in {country}");
        let description = format!(
            "Browse {} SaharaIndex company profiles in {} with company-level contact signals, activity classification, supplier type, and RFQ routing.",
            format_count(country_profiles.len()),
            country
        );
        let page = seo_segment_html(
            &title,
            &description,
            &country_profiles,
            country_profiles.len(),
            base_url,
            &href,
        );
        write_text(site_companies_dir.join(&filename), &page).await?;
        hrefs.push(href);
    }

    for (filename, title, description, segment_profiles) in [
        (
            "manufacturers.html",
            "B2B Manufacturers",
            "Browse SaharaIndex manufacturer profiles with activity classification, country coverage, contact signals, and RFQ routing.",
            profiles
                .iter()
                .filter(|profile| profile.company_type.as_deref() == Some("manufacturer"))
                .collect::<Vec<_>>(),
        ),
        (
            "wholesalers.html",
            "B2B Wholesalers",
            "Browse SaharaIndex wholesaler profiles with country coverage, contact signals, product/service links, and RFQ routing.",
            profiles
                .iter()
                .filter(|profile| profile.company_type.as_deref() == Some("wholesaler"))
                .collect::<Vec<_>>(),
        ),
        (
            "distributors.html",
            "B2B Distributors",
            "Browse SaharaIndex distributor profiles with countries, activity categories, public company contacts, and RFQ routing.",
            profiles
                .iter()
                .filter(|profile| profile.company_type.as_deref() == Some("distributor"))
                .collect::<Vec<_>>(),
        ),
        (
            "exporters.html",
            "B2B Exporters",
            "Browse SaharaIndex exporter profiles with product evidence, country coverage, contact signals, and RFQ routing.",
            profiles
                .iter()
                .filter(|profile| profile.company_type.as_deref() == Some("exporter"))
                .collect::<Vec<_>>(),
        ),
        (
            "with-website.html",
            "Companies With Websites",
            "Browse SaharaIndex company profiles that include an official or registry-supplied company website.",
            profiles
                .iter()
                .filter(|profile| !profile.contacts.websites.is_empty())
                .collect::<Vec<_>>(),
        ),
        (
            "with-email.html",
            "Companies With Public Email",
            "Browse SaharaIndex company profiles that include a public role or generic company email for buyer contact.",
            profiles
                .iter()
                .filter(|profile| has_public_email(profile))
                .collect::<Vec<_>>(),
        ),
        (
            "with-phone.html",
            "Companies With Phone Numbers",
            "Browse SaharaIndex company profiles that include a public company-level phone number.",
            profiles
                .iter()
                .filter(|profile| !profile.contacts.phones.is_empty())
                .collect::<Vec<_>>(),
        ),
        (
            "with-products.html",
            "Companies With Product Links",
            "Browse SaharaIndex company profiles enriched with product links from official company websites.",
            profiles
                .iter()
                .filter(|profile| !profile.products.is_empty())
                .collect::<Vec<_>>(),
        ),
        (
            "with-photos.html",
            "Companies With Photos",
            "Browse SaharaIndex company profiles enriched with public company or product photos.",
            profiles
                .iter()
                .filter(|profile| !profile.images.is_empty())
                .collect::<Vec<_>>(),
        ),
    ] {
        if segment_profiles.is_empty() {
            continue;
        }
        let href = format!("companies/{filename}");
        let page = seo_segment_html(
            title,
            description,
            &segment_profiles,
            segment_profiles.len(),
            base_url,
            &href,
        );
        write_text(site_companies_dir.join(filename), &page).await?;
        hrefs.push(href);
    }

    Ok(hrefs)
}

fn site_nav(local_home: &str, companies_href: &str, sitemap_href: &str) -> String {
    format!(
        concat!(
            "<nav class=\"topbar\">",
            "<a class=\"brand\" href=\"{}\">SaharaIndex</a>",
            "<div class=\"nav-links\">",
            "<a href=\"/search\">Search</a>",
            "<a href=\"/suppliers\">Suppliers</a>",
            "<a href=\"{}\">Companies</a>",
            "<a href=\"/categories\">Categories</a>",
            "<a href=\"/countries\">Countries</a>",
            "<a href=\"/rfq\">RFQ</a>",
            "<a href=\"/pricing\">Pricing</a>",
            "<a href=\"{}\">Sitemap</a>",
            "</div>",
            "<div class=\"nav-actions\">",
            "<a class=\"button secondary nav-cta\" href=\"/list-your-company\">List Your Company</a>",
            "<a class=\"login-link\" href=\"/login\">Log in</a>",
            "<button class=\"theme-toggle\" type=\"button\" aria-label=\"Toggle dark mode\" title=\"Toggle dark mode\"><span class=\"theme-label\">Dark Mode</span></button>",
            "</div>",
            "</nav>"
        ),
        html_attr(local_home),
        html_attr(companies_href),
        html_attr(sitemap_href)
    )
}

fn seo_segment_html(
    title: &str,
    description: &str,
    profiles: &[&CompanyProfile],
    total_count: usize,
    base_url: &str,
    href: &str,
) -> String {
    let canonical = absolute_site_url(base_url, href);
    let mut out = String::new();
    out.push_str("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">");
    out.push_str("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">");
    let _ = write!(
        out,
        "<title>{}</title><meta name=\"description\" content=\"{}\">",
        html(title),
        html_attr(description)
    );
    let _ = write!(
        out,
        "<link rel=\"canonical\" href=\"{}\">{}<link rel=\"stylesheet\" href=\"../styles.css\"></head><body>",
        html_attr(&canonical),
        theme_boot_script()
    );
    let _ = write!(
        out,
        "<header class=\"site-header directory-header\">{}<div class=\"hero compact\">",
        site_nav("../index.html", "index.html", "../sitemap-index.xml")
    );
    let _ = write!(
        out,
        "<p class=\"eyebrow\">SEO supplier segment</p><h1>{}</h1><p class=\"lead\">{} Showing the first {} profiles in this segment.</p><p class=\"actions\"><a class=\"button\" href=\"index.html\">Open full directory</a><a class=\"button secondary\" href=\"../index.html\">Directory home</a></p>",
        html(title),
        html(description),
        format_count(profiles.len().min(LIST_PAGE_SIZE))
    );
    out.push_str("</div></header><main class=\"page-shell\"><section class=\"panel\"><dl>");
    let count = total_count.to_string();
    field(&mut out, "Indexed profiles", Some(&count));
    out.push_str("</dl></section><section class=\"company-grid\">");
    for profile in profiles.iter().take(LIST_PAGE_SIZE) {
        push_company_card(
            &mut out,
            profile,
            &format!("{}.html", safe_file_stem(&profile.id)),
        );
    }
    out.push_str("</section></main>");
    out.push_str(theme_toggle_script());
    out.push_str("</body></html>");
    out
}

fn home_html(profiles: &[CompanyProfile], base_url: &str) -> String {
    let mut by_region = BTreeMap::<String, usize>::new();
    let mut by_country = BTreeMap::<String, usize>::new();
    let mut lei_count = 0usize;
    let mut contact_count = 0usize;
    for profile in profiles {
        if evidence_value(profile, "lei").is_some() {
            lei_count += 1;
        }
        if has_public_contact(profile) {
            contact_count += 1;
        }
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
    out.push_str("<title>SaharaIndex B2B Company Directory</title>");
    out.push_str("<meta name=\"description\" content=\"Search verified B2B company profiles, legal identifiers, countries, activities, and buyer RFQ routes on SaharaIndex.\">");
    let _ = write!(
        out,
        "<link rel=\"canonical\" href=\"{}\">",
        html_attr(base_url)
    );
    out.push_str(theme_boot_script());
    out.push_str("<link rel=\"stylesheet\" href=\"styles.css\"></head><body>");
    let _ = write!(
        out,
        "<header class=\"site-header\">{}<div class=\"hero\">",
        site_nav("index.html", "companies/index.html", "sitemap-index.xml")
    );
    out.push_str(
        "<p class=\"eyebrow\">Verified supplier discovery</p><h1>B2B Company Directory</h1>",
    );
    let _ = write!(
        out,
        "<p class=\"lead\">Search {} company profiles across Europe and MENA. Each profile keeps registry evidence separate from commercial contact enrichment and gives buyers a direct RFQ route through SaharaIndex.</p>",
        format_count(profiles.len())
    );
    out.push_str("<p class=\"actions\"><a class=\"button\" href=\"companies/index.html\">Browse companies</a><a class=\"button secondary\" href=\"sitemap-index.xml\">View sitemap</a></p></div></header>");
    out.push_str("<main class=\"page-shell\"><section class=\"stats\">");
    stat_card(
        &mut out,
        "Profiles",
        &format_count(profiles.len()),
        "indexed company pages",
    );
    stat_card(
        &mut out,
        "LEI verified",
        &format_count(lei_count),
        "matched to GLEIF records",
    );
    stat_card(
        &mut out,
        "With contacts",
        &format_count(contact_count),
        "public company-level contact points",
    );
    stat_card(
        &mut out,
        "Countries",
        &format_count(by_country.len()),
        "covered by current corpus",
    );
    out.push_str(
        "</section><section class=\"panel\"><h2>Directory Coverage</h2><div class=\"metric-grid\">",
    );
    for (region, count) in by_region {
        let _ = write!(
            out,
            "<div><strong>{}</strong><span>{} profiles</span></div>",
            html(&region),
            format_count(count)
        );
    }
    out.push_str("</div></section>");
    out.push_str("<section class=\"panel\"><h2>Top Countries</h2><div class=\"chips\">");
    let mut countries = by_country.into_iter().collect::<Vec<_>>();
    countries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    for (country, count) in countries.iter().take(40) {
        let _ = write!(
            out,
            "<a href=\"companies/country-{}.html\">{} <strong>{}</strong></a>",
            html_attr(&slugify(country)),
            html(country),
            format_count(*count)
        );
    }
    out.push_str("</div></section></main>");
    out.push_str(theme_toggle_script());
    out.push_str("</body></html>");
    out
}

fn listing_html(
    profiles: &[CompanyProfile],
    page_index: usize,
    page_count: usize,
    total_count: usize,
    base_url: &str,
    filters: &FilterOptions,
) -> String {
    let page_number = page_index + 1;
    let canonical = absolute_site_url(base_url, &listing_page_href(page_index));
    let mut out = String::new();
    out.push_str("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">");
    out.push_str("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">");
    let _ = write!(
        out,
        "<title>B2B Companies - Page {}</title><meta name=\"description\" content=\"Browse SaharaIndex company profiles with legal identifiers, country filters, public contacts when available, and RFQ routing.\">",
        page_number
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
    out.push_str(theme_boot_script());
    out.push_str("<link rel=\"stylesheet\" href=\"../styles.css\"></head><body>");
    let _ = write!(
        out,
        "<header class=\"site-header directory-header\">{}<div class=\"hero compact\">",
        site_nav("../index.html", "index.html", "../sitemap-index.xml")
    );
    let _ = write!(
        out,
        "<p class=\"eyebrow\">Company directory</p><h1>B2B Companies</h1><p class=\"lead\">Page {} of {} with {} indexed profiles. Use the search box and filters to narrow results by country, activity, source, company type, and contact/product signals.</p>",
        page_number,
        page_count,
        format_count(total_count)
    );
    out.push_str("<div class=\"toolbar\"><input id=\"search\" type=\"search\" placeholder=\"Filter this page by company, country, LEI, activity, or source\" aria-label=\"Filter this page\"><a class=\"button\" href=\"../index.html\">Directory home</a></div></div></header>");
    out.push_str("<main class=\"page-shell\"><section class=\"panel filter-panel\"><p class=\"filter-summary\" id=\"filter-summary\">Showing all ");
    let _ = write!(
        out,
        "{}</p><div class=\"filter-controls\">",
        format_count(profiles.len())
    );
    write_filter_select(
        &mut out,
        "filter-country",
        "Country",
        "All countries",
        &filters.countries,
    );
    write_filter_select(
        &mut out,
        "filter-industry",
        "Industry / Activity",
        "All activities",
        &filters.industries,
    );
    write_filter_select(
        &mut out,
        "filter-product-category",
        "Product category",
        "All categories",
        &filters.product_categories,
    );
    write_filter_select(
        &mut out,
        "filter-source",
        "Source",
        "All sources",
        &filters.sources,
    );
    write_filter_select(
        &mut out,
        "filter-type",
        "Company type",
        "All types",
        &filters.company_types,
    );
    out.push_str(
        "<div class=\"filter-item\"><span>Contact availability</span><div class=\"checkboxes\">",
    );
    out.push_str("<label><input id=\"filter-contact-website\" type=\"checkbox\">Website</label>");
    out.push_str("<label><input id=\"filter-contact-email\" type=\"checkbox\">Email</label>");
    out.push_str("<label><input id=\"filter-contact-phone\" type=\"checkbox\">Phone</label>");
    out.push_str("</div></div><div class=\"filter-item\"><span>Catalog availability</span><div class=\"checkboxes\">");
    out.push_str("<label><input id=\"filter-product\" type=\"checkbox\">Products</label>");
    out.push_str("<label><input id=\"filter-service\" type=\"checkbox\">Services</label>");
    out.push_str("</div></div><div class=\"filter-item filter-actions\"><button type=\"button\" id=\"filter-reset\" class=\"button secondary\">Reset filters</button></div></div>");
    out.push_str("</section><nav class=\"pager\">");
    if page_index > 0 {
        let _ = write!(
            out,
            "<a class=\"button secondary\" href=\"{}\">Previous</a>",
            html_attr(&listing_page_local_href(page_index - 1))
        );
    }
    if page_index + 1 < page_count {
        let _ = write!(
            out,
            "<a class=\"button secondary\" href=\"{}\">Next</a>",
            html_attr(&listing_page_local_href(page_index + 1))
        );
    }
    out.push_str("</nav><section class=\"company-grid\" id=\"companies\">");

    for profile in profiles {
        push_company_card(
            &mut out,
            profile,
            &format!("{}.html", safe_file_stem(&profile.id)),
        );
    }

    out.push_str("</section><p id=\"filter-empty\" class=\"filter-empty\" role=\"status\" hidden>No companies match the selected filters.</p></main>");
    out.push_str(site_search_script());
    out.push_str(theme_toggle_script());
    out.push_str("</body></html>");
    out
}

fn write_filter_select(
    out: &mut String,
    id: &str,
    label: &str,
    all_label: &str,
    options: &BTreeMap<String, String>,
) {
    let _ = write!(
        out,
        "<div class=\"filter-item\"><label for=\"{}\">{}</label><select id=\"{}\"><option value=\"\">{}</option>",
        html_attr(id),
        html(label),
        html_attr(id),
        html(all_label)
    );
    for (value, label) in options {
        let _ = write!(
            out,
            "<option value=\"{}\">{}</option>",
            html_attr(value),
            html(label)
        );
    }
    out.push_str("</select></div>");
}

fn filter_options(profiles: &[CompanyProfile]) -> FilterOptions {
    let mut options = FilterOptions::default();
    for profile in profiles {
        let country = profile.country.as_deref().unwrap_or("Unknown country");
        upsert_filter_option(&mut options.countries, country);

        for industry in &profile.industries {
            let normalized = normalized_filter_value(industry);
            if !normalized.is_empty() && normalized != "unclassified b2b entity" {
                upsert_filter_option(&mut options.industries, industry);
            }
        }

        for category in product_category_labels(profile) {
            upsert_filter_option(&mut options.product_categories, &category);
        }

        upsert_filter_option(&mut options.sources, &profile.source_name);
        upsert_filter_option(&mut options.company_types, &display_company_type(profile));
    }
    options
}

fn upsert_filter_option(options: &mut BTreeMap<String, String>, value: &str) {
    let label = clean_public_text(value);
    let key = normalized_filter_value(&label);
    if key.is_empty() {
        return;
    }
    options.entry(key).or_insert(label);
}

fn normalized_filter_value(value: &str) -> String {
    clean_public_text(value).to_ascii_lowercase()
}

fn static_search_record(profile: &CompanyProfile) -> StaticCompanySearchRecord {
    let title = display_company_name(profile);
    let country = profile
        .country
        .as_deref()
        .unwrap_or("Unknown country")
        .to_string();
    let company_type = display_company_type(profile);
    let industries = display_industries(profile);
    let description = meta_description(profile);
    let lei = evidence_value(profile, "lei");
    let industry_tokens = industry_tokens(profile);
    let product_category_tokens = product_category_labels(profile)
        .iter()
        .map(|value| normalized_filter_value(value))
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    let source = profile.source_name.clone();
    StaticCompanySearchRecord {
        title,
        url: format!("{}.html", safe_file_stem(&profile.id)),
        description,
        country_token: normalized_filter_value(&country),
        country,
        company_type_token: normalized_filter_value(&company_type),
        company_type,
        industries,
        industry_tokens,
        product_category_tokens,
        source_token: normalized_filter_value(&source),
        source,
        image_url: first_image_url(profile),
        lei,
        has_website: !profile.contacts.websites.is_empty(),
        has_email: has_public_email(profile),
        has_phone: !profile.contacts.phones.is_empty(),
        has_products: !profile.products.is_empty(),
        has_services: !profile.services.is_empty(),
    }
}

fn industry_tokens(profile: &CompanyProfile) -> Vec<String> {
    profile
        .industries
        .iter()
        .filter_map(|industry| {
            let normalized = normalized_filter_value(industry);
            if normalized.is_empty() || normalized == "unclassified b2b entity" {
                None
            } else {
                Some(normalized)
            }
        })
        .collect()
}

fn product_category_labels(profile: &CompanyProfile) -> Vec<String> {
    let mut labels = BTreeSet::new();
    for item in profile.products.iter().chain(profile.services.iter()) {
        if let Some(category) = item.category.as_deref().and_then(public_catalog_label) {
            labels.insert(category);
        }
    }
    labels.into_iter().collect()
}

fn public_catalog_label(value: &str) -> Option<String> {
    let label = clean_public_text(value);
    let normalized = label.to_ascii_lowercase();
    let blocked = [
        "product",
        "products",
        "service",
        "services",
        "skip to content",
        "read more",
        "view",
        "view fullsize",
        "click here",
        "new",
        "new!",
        "vaata toodet",
        "vaata veel",
    ];
    if label.len() < 3 || blocked.iter().any(|blocked| normalized == *blocked) {
        None
    } else {
        Some(label)
    }
}

fn push_company_card(out: &mut String, profile: &CompanyProfile, href: &str) {
    let description = meta_description(profile);
    let display_name = display_company_name(profile);
    let country = profile
        .country
        .as_deref()
        .unwrap_or("Unknown country")
        .to_string();
    let country_token = normalized_filter_value(&country);
    let source_token = normalized_filter_value(&profile.source_name);
    let company_type = display_company_type(profile);
    let company_type_token = normalized_filter_value(&company_type);
    let industries_for_filter = industry_tokens(profile).join("|");
    let product_categories_for_filter = product_category_labels(profile)
        .iter()
        .map(|category| normalized_filter_value(category))
        .filter(|category| !category.is_empty())
        .collect::<Vec<_>>()
        .join("|");
    let lei = evidence_value(profile, "lei");
    let industries = display_industries(profile);
    let has_website = !profile.contacts.websites.is_empty();
    let has_email = has_public_email(profile);
    let has_phone = !profile.contacts.phones.is_empty();
    let has_products = !profile.products.is_empty();
    let has_services = !profile.services.is_empty();
    let image_html = first_image_url(profile)
        .map(|url| {
            format!(
                "<a class=\"card-media\" href=\"{}\"><img src=\"{}\" alt=\"{}\" loading=\"lazy\"></a>",
                html_attr(href),
                html_attr(&url),
                html_attr(&format!("{display_name} profile image"))
            )
        })
        .unwrap_or_default();
    let lei_badge = if lei.is_some() {
        "<span class=\"badge ok\">LEI verified</span>"
    } else {
        "<span class=\"badge muted\">Registry record</span>"
    };
    let contact_badge = if has_public_contact(profile) {
        "<span class=\"badge ok\">Company contact</span>"
    } else {
        "<span class=\"badge muted\">RFQ routing</span>"
    };
    let _ = write!(
        out,
        "<article class=\"company-card\" data-search=\"{}\" data-country=\"{}\" data-industries=\"{}\" data-product-categories=\"{}\" data-source=\"{}\" data-company-type=\"{}\" data-has-website=\"{}\" data-has-email=\"{}\" data-has-phone=\"{}\" data-has-products=\"{}\" data-has-services=\"{}\">{}<div class=\"card-badges\">{}{}</div><a class=\"card-title\" href=\"{}\"><h2>{}</h2></a><p class=\"meta\">{} | {} | {}</p><p>{}</p><dl class=\"mini-facts\">{}{}{}{}{}</dl><a class=\"text-link\" href=\"{}\">Open profile</a></article>",
        html_attr(&format!(
            "{} {} {} {} {} {} {}",
            display_name,
            country,
            company_type,
            industries,
            description,
            lei.as_deref().unwrap_or(""),
            profile.source_name
        )),
        html_attr(&country_token),
        html_attr(&industries_for_filter),
        html_attr(&product_categories_for_filter),
        html_attr(&source_token),
        html_attr(&company_type_token),
        if has_website { "1" } else { "0" },
        if has_email { "1" } else { "0" },
        if has_phone { "1" } else { "0" },
        if has_products { "1" } else { "0" },
        if has_services { "1" } else { "0" },
        image_html,
        lei_badge,
        contact_badge,
        html_attr(href),
        html(&display_name),
        html(&country),
        html(&company_type),
        html(&industries),
        html(&description),
        mini_fact("LEI", lei.as_deref()),
        mini_fact("Source", Some(&profile.source_name)),
        mini_fact("Contact", Some(contact_status(profile))),
        mini_fact(
            "Products",
            Some(if has_products { "Available" } else { "Not listed" }),
        ),
        mini_fact(
            "Services",
            Some(if has_services { "Available" } else { "Not listed" }),
        ),
        html_attr(href)
    );
}

fn company_html(profile: &CompanyProfile, base_url: &str) -> String {
    let canonical = absolute_site_url(
        base_url,
        &format!("companies/{}.html", safe_file_stem(&profile.id)),
    );
    let description = meta_description(profile);
    let display_name = display_company_name(profile);
    let rfq_url = rfq_url(base_url, profile);
    let lei = evidence_value(profile, "lei").unwrap_or_default();
    let mut out = String::new();
    out.push_str("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">");
    out.push_str("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">");
    let _ = write!(
        out,
        "<title>{}</title><meta name=\"description\" content=\"{}\">",
        html(&display_name),
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
    out.push_str(theme_boot_script());
    out.push_str("<link rel=\"stylesheet\" href=\"../styles.css\"></head><body>");
    let _ = write!(
        out,
        "<header class=\"site-header profile-header\">{}<div class=\"hero compact\">",
        site_nav("../index.html", "index.html", "../sitemap-index.xml")
    );
    out.push_str("<p class=\"eyebrow\">Company profile</p>");
    let _ = write!(out, "<h1>{}</h1>", html(&display_name));
    let _ = write!(out, "<p class=\"lead\">{}</p>", html(&description));
    out.push_str("<div class=\"card-badges\">");
    if !lei.is_empty() {
        out.push_str("<span class=\"badge ok\">LEI verified</span>");
    }
    let _ = write!(
        out,
        "<span class=\"badge muted\">{}</span><span class=\"badge muted\">{}</span>",
        html(profile.country.as_deref().unwrap_or("Country pending")),
        html(&display_company_type(profile))
    );
    out.push_str("</div>");
    let _ = write!(
        out,
        "<p class=\"actions\"><a class=\"button\" href=\"{}\">Request quote</a><a class=\"button secondary\" href=\"index.html\">Back to companies</a></p>",
        html_attr("#rfq")
    );
    out.push_str("</div></header><main class=\"profile-layout\">");

    out.push_str("<section class=\"panel profile-summary\"><h2>Company</h2><dl>");
    field(&mut out, "Region", profile.region.as_deref());
    field(&mut out, "Country", profile.country.as_deref());
    let company_type = display_company_type(profile);
    field(&mut out, "Type", Some(&company_type));
    field(&mut out, "Domain", profile.canonical_domain.as_deref());
    field(&mut out, "Source", Some(&profile.source_name));
    field_link(
        &mut out,
        "Source record",
        &profile.profile_url,
        "Open source record",
    );
    if !lei.is_empty() {
        field(&mut out, "LEI", Some(&lei));
    }
    out.push_str("</dl></section>");

    lei_verification_section(&mut out, profile);
    list_section(&mut out, "Industries", &profile.industries);
    list_section(&mut out, "Specializations", &profile.specializations);
    list_section(&mut out, "Addresses", &profile.addresses);
    media_section(&mut out, profile);

    out.push_str("<section class=\"panel\"><h2>Buyer Contact</h2>");
    let public_emails = profile
        .contacts
        .emails
        .iter()
        .filter(|email| !email.personal)
        .collect::<Vec<_>>();
    if public_emails.is_empty()
        && profile.contacts.phones.is_empty()
        && profile.contacts.websites.is_empty()
    {
        let _ = write!(
            out,
            "<p>No verified company-level contact point is published for this profile yet. Send an RFQ through SaharaIndex so the request can be routed and supplier details can be verified.</p><p><a class=\"button\" href=\"{}\">Request quote</a></p>",
            html_attr("#rfq")
        );
    } else {
        out.push_str("<ul class=\"contact-list\">");
        for email in public_emails {
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
        let _ = write!(
            out,
            "<p><a class=\"button\" href=\"{}\">Request quote through SaharaIndex</a></p>",
            html_attr("#rfq")
        );
    }
    out.push_str("</section>");

    rfq_section(&mut out, profile, &lei, &canonical, &rfq_url);
    catalog_section(&mut out, "Products", &profile.products);
    catalog_section(&mut out, "Services", &profile.services);

    out.push_str("<section class=\"panel\"><h2>Validation</h2><dl>");
    field(&mut out, "Status", Some(&profile.validation.status));
    let score = profile.validation.score.to_string();
    field(&mut out, "Score", Some(&score));
    out.push_str("</dl></section>");

    out.push_str("</main>");
    out.push_str(rfq_form_script());
    out.push_str(theme_toggle_script());
    out.push_str("</body></html>");
    out
}

async fn write_sitemaps(
    site_dir: &Path,
    profiles: &[CompanyProfile],
    base_url: &str,
    seo_segment_hrefs: &[String],
) -> anyhow::Result<()> {
    let mut urls = Vec::with_capacity(profiles.len() + listing_page_count(profiles.len()) + 1);
    urls.push(base_url.to_string());
    for page_index in 0..listing_page_count(profiles.len()) {
        urls.push(absolute_site_url(base_url, &listing_page_href(page_index)));
    }
    for href in seo_segment_hrefs {
        urls.push(absolute_site_url(base_url, href));
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
            linked_text(&clean_public_text(value))
        );
    }
}

fn field_link(out: &mut String, label: &str, url: &str, text: &str) {
    if !url.trim().is_empty() {
        let _ = write!(
            out,
            "<dt>{}</dt><dd><a href=\"{}\" rel=\"nofollow noopener\">{}</a></dd>",
            html(label),
            html_attr(url),
            html(text)
        );
    }
}

fn lei_verification_section(out: &mut String, profile: &CompanyProfile) {
    if evidence_value(profile, "lei").is_none() {
        return;
    }

    out.push_str("<section class=\"panel lei-panel\"><h2>LEI Verification</h2><dl>");
    for (label, field_name) in [
        ("LEI", "lei"),
        ("Legal name", "gleif_legal_name"),
        ("Entity status", "gleif_entity_status"),
        ("Registration status", "gleif_registration_status"),
        ("Registered as", "gleif_registered_as"),
        ("Jurisdiction", "gleif_jurisdiction"),
        ("Last update", "gleif_last_update_date"),
        ("Next renewal", "gleif_next_renewal_date"),
    ] {
        let value = evidence_value(profile, field_name);
        field(out, label, value.as_deref());
    }
    out.push_str("</dl><details><summary>Registry details</summary><dl>");
    for (label, field_name) in [
        ("Entity category", "gleif_entity_category"),
        ("Legal form", "gleif_legal_form"),
        ("Registration authority", "gleif_registered_at"),
        ("Creation date", "gleif_creation_date"),
        ("Initial registration", "gleif_initial_registration_date"),
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
    out.push_str("</dl></details></section>");
}

fn rfq_section(
    out: &mut String,
    profile: &CompanyProfile,
    lei: &str,
    canonical: &str,
    rfq_url: &str,
) {
    let display_name = display_company_name(profile);
    out.push_str("<section id=\"rfq\" class=\"panel rfq-panel\"><h2>Request Quote</h2>");
    let _ = write!(
        out,
        "<p>Send a buyer request to SaharaIndex for routing and supplier verification for {}.</p>",
        html(&display_name)
    );
    let _ = write!(
        out,
        "<form class=\"rfq-form\" data-company=\"{}\" data-lei=\"{}\" data-profile=\"{}\" data-api=\"{}\" data-rfq-url=\"{}\">",
        html_attr(&display_name),
        html_attr(lei),
        html_attr(canonical),
        html_attr(&rfq_api_url()),
        html_attr(rfq_url)
    );
    out.push_str(
        "<label>Buyer name<input name=\"buyerName\" required autocomplete=\"name\"></label>",
    );
    out.push_str("<label>Buyer email<input name=\"buyerEmail\" type=\"email\" required autocomplete=\"email\"></label>");
    out.push_str(
        "<label>Buyer company<input name=\"buyerCompany\" autocomplete=\"organization\"></label>",
    );
    out.push_str("<label>Buyer phone<input name=\"buyerPhone\" autocomplete=\"tel\"></label>");
    out.push_str("<label>Product needed<input name=\"productNeeded\" required placeholder=\"Product, quantity, destination\"></label>");
    out.push_str("<label>Quantity<input name=\"quantity\" placeholder=\"e.g. 1 container, 10 tonnes, 500 units\"></label>");
    out.push_str("<label>Destination country<input name=\"destinationCountry\" autocomplete=\"country-name\"></label>");
    out.push_str("<label>Message<textarea name=\"message\" rows=\"4\" placeholder=\"Incoterms, specs, certifications, delivery timeline\"></textarea></label>");
    out.push_str("<button class=\"button\" type=\"submit\">Send RFQ</button><p class=\"rfq-status\" role=\"status\"></p></form>");
    out.push_str("</section>");
}

fn list_section(out: &mut String, title: &str, items: &[String]) {
    let cleaned_items = items
        .iter()
        .map(|item| clean_public_text(item))
        .filter(|item| !item.is_empty())
        .filter(|item| !item.eq_ignore_ascii_case("unclassified b2b entity"))
        .collect::<Vec<_>>();
    if cleaned_items.is_empty() {
        return;
    }
    out.push_str("<section class=\"panel\">");
    let _ = write!(out, "<h2>{}</h2>", html(title));
    out.push_str("<ul class=\"clean-list\">");
    for item in cleaned_items {
        let _ = write!(out, "<li>{}</li>", html(&item));
    }
    out.push_str("</ul>");
    out.push_str("</section>");
}

fn media_section(out: &mut String, profile: &CompanyProfile) {
    if profile.images.is_empty() {
        return;
    }
    out.push_str("<section class=\"panel media-panel\"><h2>Photos</h2><div class=\"media-grid\">");
    for image in profile.images.iter().take(8) {
        if image.url.trim().is_empty() {
            continue;
        }
        let alt = image
            .alt
            .as_deref()
            .map(clean_public_text)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| display_company_name(profile));
        let _ = write!(
            out,
            "<figure><img src=\"{}\" alt=\"{}\" loading=\"lazy\"><figcaption>{}</figcaption></figure>",
            html_attr(&image.url),
            html_attr(&alt),
            html(&alt)
        );
    }
    out.push_str("</div></section>");
}

fn catalog_section(out: &mut String, title: &str, items: &[crate::models::CatalogItem]) {
    if items.is_empty() {
        return;
    }
    out.push_str("<section class=\"panel\">");
    let _ = write!(out, "<h2>{}</h2>", html(title));
    out.push_str("<ul class=\"catalog-list\">");
    for item in items {
        let name = clean_public_text(&item.name);
        if name.is_empty() {
            continue;
        }
        out.push_str("<li>");
        if let Some(url) = &item.url {
            let _ = write!(
                out,
                "<a href=\"{}\" rel=\"nofollow noopener\">{}</a>",
                html_attr(url),
                html(&name)
            );
        } else {
            let _ = write!(out, "<strong>{}</strong>", html(&name));
        }
        if let Some(category) = item.category.as_deref().and_then(public_catalog_label) {
            let _ = write!(out, "<span>{}</span>", html(&category));
        }
        if let Some(description) = item
            .description
            .as_deref()
            .map(clean_public_text)
            .filter(|value| !value.is_empty())
        {
            let _ = write!(out, "<p>{}</p>", html(&truncate_text_to(&description, 160)));
        }
        out.push_str("</li>");
    }
    out.push_str("</ul>");
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

fn rfq_url(base_url: &str, profile: &CompanyProfile) -> String {
    absolute_site_url(
        base_url,
        &format!(
            "rfq?company={}&lei={}",
            url_component(&profile.id),
            url_component(evidence_value(profile, "lei").as_deref().unwrap_or(""))
        ),
    )
}

fn rfq_api_url() -> String {
    std::env::var("OBSCURA_B2B_RFQ_API_URL")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "https://api.saharaindex.com/api/rfqs".to_string())
}

fn skip_company_json_files() -> bool {
    std::env::var("OBSCURA_B2B_SKIP_COMPANY_JSON")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

fn display_company_name(profile: &CompanyProfile) -> String {
    clean_public_text(&profile.company_name)
}

fn profile_sort_key(profile: &CompanyProfile) -> (u8, String) {
    (
        profile_sort_priority(profile),
        display_company_name(profile).to_ascii_lowercase(),
    )
}

fn profile_sort_priority(profile: &CompanyProfile) -> u8 {
    let company_type = profile.company_type.as_deref().unwrap_or_default();
    let is_target = matches!(
        company_type,
        "manufacturer" | "wholesaler" | "distributor" | "exporter"
    );
    match (is_target, has_public_contact(profile)) {
        (true, true) => 0,
        (true, false) => 1,
        (false, true) => 2,
        (false, false) => 3,
    }
}

fn display_company_type(profile: &CompanyProfile) -> String {
    profile
        .company_type
        .as_deref()
        .map(clean_public_text)
        .map(|value| {
            if value.eq_ignore_ascii_case("legal entity") {
                "Legal entity".to_string()
            } else {
                value
            }
        })
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "B2B entity".to_string())
}

fn display_industries(profile: &CompanyProfile) -> String {
    let industries = profile
        .industries
        .iter()
        .map(|value| clean_public_text(value))
        .filter(|value| !value.is_empty())
        .filter(|value| !value.eq_ignore_ascii_case("unclassified b2b entity"))
        .collect::<Vec<_>>();

    if industries.is_empty() {
        "Activity not classified yet".to_string()
    } else {
        industries.join(", ")
    }
}

fn has_public_contact(profile: &CompanyProfile) -> bool {
    has_public_email(profile)
        || !profile.contacts.phones.is_empty()
        || !profile.contacts.websites.is_empty()
}

fn has_public_email(profile: &CompanyProfile) -> bool {
    profile.contacts.emails.iter().any(|email| !email.personal)
}

fn first_image_url(profile: &CompanyProfile) -> Option<String> {
    profile
        .images
        .iter()
        .map(|image| image.url.trim())
        .find(|url| !url.is_empty())
        .map(ToOwned::to_owned)
}

fn contact_status(profile: &CompanyProfile) -> &'static str {
    if has_public_contact(profile) {
        "Public company contact"
    } else {
        "RFQ routing"
    }
}

fn stat_card(out: &mut String, label: &str, value: &str, note: &str) {
    let _ = write!(
        out,
        "<div class=\"stat-card\"><span>{}</span><strong>{}</strong><small>{}</small></div>",
        html(label),
        html(value),
        html(note)
    );
}

fn mini_fact(label: &str, value: Option<&str>) -> String {
    value
        .map(clean_public_text)
        .filter(|value| !value.is_empty())
        .map(|value| {
            format!(
                "<dt>{}</dt><dd>{}</dd>",
                html(label),
                html(&truncate_text_to(&value, 64))
            )
        })
        .unwrap_or_default()
}

fn format_count(value: usize) -> String {
    let digits = value.to_string();
    let mut out = String::new();
    for (idx, ch) in digits.chars().rev().enumerate() {
        if idx > 0 && idx % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

fn meta_description(profile: &CompanyProfile) -> String {
    let description = profile
        .description
        .as_deref()
        .map(clean_public_text)
        .unwrap_or_default();
    if !description.is_empty()
        && !description
            .to_ascii_lowercase()
            .contains("still requires official website")
    {
        return truncate_text(&description);
    }

    let name = display_company_name(profile);
    let country = profile
        .country
        .as_deref()
        .unwrap_or("its registered market");
    let company_type = display_company_type(profile);
    let activity = display_industries(profile);
    let lei = evidence_value(profile, "lei");
    let activity_sentence = if activity == "Activity not classified yet" {
        "Activity classification is pending.".to_string()
    } else {
        format!("Activity: {activity}.")
    };
    let fallback = format!(
        "{} is a {} in {}. {}{} Buyers can send an RFQ through SaharaIndex while company-level contact and catalog enrichment is verified.",
        name,
        company_type,
        country,
        activity_sentence,
        lei.map(|value| format!(" LEI: {value}.")).unwrap_or_default()
    );
    truncate_text(&fallback)
}

fn truncate_text(value: &str) -> String {
    truncate_text_to(value, 220)
}

fn truncate_text_to(value: &str, max_chars: usize) -> String {
    const MAX_CHARS: usize = 220;
    let max_chars = max_chars.min(MAX_CHARS);
    let mut out = value.chars().take(max_chars).collect::<String>();
    if value.chars().count() > max_chars {
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

fn is_displayable_profile(profile: &CompanyProfile) -> bool {
    if matches!(
        profile.validation.status.as_str(),
        "hold" | "blocked" | "rejected"
    ) {
        return false;
    }
    display_company_name(profile)
        .chars()
        .filter(|ch| ch.is_alphanumeric())
        .count()
        >= 2
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
        image: Option<String>,
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

    let display_name = display_company_name(profile);
    let data = OrganizationJsonLd {
        context: "https://schema.org",
        kind: "Organization",
        id: canonical,
        name: &display_name,
        description: meta_description(profile),
        url: profile
            .canonical_domain
            .clone()
            .unwrap_or_else(|| canonical.to_string()),
        address: profile.addresses.iter().collect(),
        same_as: profile.contacts.social_links.iter().collect(),
        image: first_image_url(profile),
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

fn clean_public_text(value: &str) -> String {
    let without_entities = value.replace("&quot;", "").replace("&#34;", "");
    let mut out = String::new();
    let mut last_space = false;

    for ch in without_entities.chars() {
        if ch == '"' {
            continue;
        }
        if ch.is_whitespace() {
            if !last_space && !out.is_empty() {
                out.push(' ');
                last_space = true;
            }
            continue;
        }
        out.push(ch);
        last_space = false;
    }

    let mut cleaned = out
        .trim()
        .trim_matches(|ch| {
            matches!(
                ch,
                '"' | '\'' | '.' | '&' | '+' | '*' | '-' | ',' | ';' | ':'
            )
        })
        .to_string();
    for (from, to) in [
        (" ,", ","),
        (" .", "."),
        (" ;", ";"),
        (" :", ":"),
        ("( ", "("),
        (" )", ")"),
        (" - ", " - "),
    ] {
        cleaned = cleaned.replace(from, to);
    }
    while cleaned.contains("  ") {
        cleaned = cleaned.replace("  ", " ");
    }

    if cleaned.is_empty() {
        value.trim().to_string()
    } else {
        cleaned
    }
}

fn url_component(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(char::from(byte));
            }
            _ => {
                let _ = write!(out, "%{byte:02X}");
            }
        }
    }
    out
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
    r#"*{box-sizing:border-box}:root{color-scheme:light;--bg:#f5f7fa;--surface:#fff;--surface-2:#eef2f6;--text:#17202a;--muted:#526070;--line:#d8dee8;--line-strong:#b8c3d1;--brand:#0b203c;--brand-2:#0b5cab;--accent:#d6a64c;--ok-bg:#e8f6ee;--ok-line:#a9d8bc;--ok-text:#146c2e;--danger:#b42318;--shadow:0 8px 24px rgba(17,31,48,.07)}html.dark{color-scheme:dark;--bg:#07111f;--surface:#0e1a2a;--surface-2:#13243a;--text:#f7f7f7;--muted:#b8c3d1;--line:#263952;--line-strong:#3b506b;--brand:#f7f7f7;--brand-2:#d6a64c;--accent:#d6a64c;--ok-bg:#123622;--ok-line:#2d7d4a;--ok-text:#8ee0aa;--danger:#ff9b8f;--shadow:0 10px 30px rgba(0,0,0,.28)}body{margin:0;font-family:Inter,Arial,sans-serif;background:var(--bg);color:var(--text)}a{color:var(--brand-2);text-decoration:none}a:hover{text-decoration:underline}p{line-height:1.5}h1,h2{letter-spacing:0}.site-header{background:var(--surface);border-bottom:1px solid var(--line)}.topbar{display:flex;align-items:center;gap:18px;max-width:1180px;margin:0 auto;padding:14px 24px}.brand{font-weight:900;color:var(--brand);font-size:19px;white-space:nowrap}.nav-links,.nav-actions{display:flex;align-items:center;flex-wrap:wrap;gap:14px}.nav-links{flex:1}.nav-links a,.login-link{color:var(--text);font-weight:800;font-size:14px}.nav-actions{justify-content:flex-end}.theme-toggle{border:1px solid var(--line-strong);background:var(--surface-2);color:var(--text);border-radius:6px;padding:9px 11px;font:inherit;font-weight:800;cursor:pointer}.nav-cta{padding:9px 11px}.hero{max-width:1180px;margin:0 auto;padding:34px 24px 38px}.hero.compact{padding-top:24px}.eyebrow{margin:0 0 8px;color:var(--muted);font-size:13px;font-weight:900;text-transform:uppercase}.lead{max-width:860px;color:var(--muted);font-size:17px}.actions,.toolbar,.pager,.card-badges{display:flex;flex-wrap:wrap;gap:10px;align-items:center}.button,button.button{display:inline-block;background:var(--brand-2);color:#fff!important;border:0;border-radius:6px;padding:10px 13px;font-weight:900;text-decoration:none;cursor:pointer}.button.secondary{background:var(--surface-2);color:var(--text)!important;border:1px solid var(--line-strong)}.page-shell{max-width:1180px;margin:0 auto;padding:24px}.stats,.metric-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:14px}.stat-card,.panel,.company-card{background:var(--surface);border:1px solid var(--line);border-radius:8px;box-shadow:var(--shadow)}.stat-card{padding:16px}.stat-card span,.stat-card small,.meta{color:var(--muted)}.stat-card strong{display:block;font-size:30px;margin:4px 0;color:var(--text)}.panel{padding:18px;margin-bottom:16px}.panel h2,.company-card h2{margin:0 0 10px}.metric-grid div{padding:12px;border:1px solid var(--line);border-radius:6px}.metric-grid strong{display:block;font-size:20px}.chips{display:flex;flex-wrap:wrap;gap:8px}.chips span,.chips a,.badge{border-radius:999px;padding:6px 10px;font-size:12px;font-weight:900}.chips span,.chips a{background:var(--surface-2);border:1px solid var(--line)}.badge.ok{background:var(--ok-bg);border:1px solid var(--ok-line);color:var(--ok-text)}.badge.muted{background:var(--surface-2);border:1px solid var(--line);color:var(--muted)}.filter-panel{display:block}.filter-summary{margin:0 0 8px;color:var(--muted);font-weight:900}.filter-controls{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px}.filter-item{display:flex;flex-direction:column;gap:6px}.filter-item span{font-size:13px;font-weight:900;color:var(--text)}.checkboxes{display:flex;flex-wrap:wrap;gap:8px}.checkboxes label{display:flex;align-items:center;gap:6px;font-size:13px;font-weight:700;color:var(--text);cursor:pointer}.checkboxes input{width:auto;margin:0}.filter-actions{align-self:flex-end}.filter-empty{margin:8px 0 0;color:var(--danger);font-weight:900}.company-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:14px}.company-card{padding:16px;display:flex;flex-direction:column;min-height:300px;overflow:hidden}.card-media{display:block;margin:-16px -16px 14px;border-bottom:1px solid var(--line);background:var(--surface-2);height:150px}.card-media img{width:100%;height:100%;object-fit:cover;display:block}.card-title h2{font-size:18px;line-height:1.25;margin-top:10px;overflow-wrap:anywhere}.company-card p{margin:8px 0}.mini-facts,dl{display:grid;grid-template-columns:130px 1fr;gap:8px 12px}.mini-facts{margin:12px 0 16px;font-size:13px}.mini-facts dt,dt{font-weight:900;color:var(--text)}.mini-facts dd,dd{margin:0;overflow-wrap:anywhere;color:var(--muted)}.text-link{margin-top:auto;font-weight:900}.profile-layout{max-width:1180px;margin:0 auto;padding:24px;display:grid;grid-template-columns:minmax(0,1fr) minmax(320px,420px);gap:16px}.profile-summary,.lei-panel,.media-panel{grid-column:1/-1}.rfq-panel{grid-column:2}.clean-list,.contact-list,.catalog-list{margin:0;padding-left:18px}.clean-list li,.contact-list li,.catalog-list li{margin:8px 0}.catalog-list span{display:inline-block;margin-left:8px;border:1px solid var(--line);border-radius:999px;padding:3px 8px;color:var(--muted);font-size:12px;font-weight:800}.catalog-list p{margin:4px 0 0;color:var(--muted)}.media-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:12px}.media-grid figure{margin:0;border:1px solid var(--line);border-radius:8px;overflow:hidden;background:var(--surface-2)}.media-grid img{width:100%;height:150px;object-fit:cover;display:block}.media-grid figcaption{padding:8px 10px;color:var(--muted);font-size:12px;font-weight:800}details{margin-top:14px}summary{cursor:pointer;font-weight:900;color:var(--brand-2)}input,textarea,select{width:100%;margin-top:6px;padding:11px 12px;border:1px solid var(--line-strong);border-radius:6px;font:inherit;background:var(--surface);color:var(--text)}input[type=search]{max-width:620px;margin-top:0}label{display:block;font-weight:900;color:var(--text)}.rfq-form{display:grid;gap:12px}.rfq-status{font-weight:900}.rfq-status.ok{color:var(--ok-text)}.rfq-status.err{color:var(--danger)}@media(max-width:980px){.topbar{align-items:flex-start;flex-direction:column}.nav-links,.nav-actions{width:100%;justify-content:flex-start}.nav-links a{font-size:13px}}@media(max-width:820px){.topbar,.hero,.page-shell,.profile-layout{padding-left:16px;padding-right:16px}.company-grid,.profile-layout{grid-template-columns:1fr}.rfq-panel{grid-column:auto}dl,.mini-facts{grid-template-columns:1fr}.toolbar input[type=search]{max-width:none}.filter-controls{grid-template-columns:1fr}.card-media{height:130px}}"#
}

fn theme_boot_script() -> &'static str {
    r#"<script>(()=>{try{const saved=localStorage.getItem('sahara_theme');const dark=saved?saved==='dark':matchMedia('(prefers-color-scheme: dark)').matches;document.documentElement.classList.toggle('dark',dark);}catch(err){}})();</script>"#
}

fn theme_toggle_script() -> &'static str {
    r#"<script>(()=>{const sync=()=>{const dark=document.documentElement.classList.contains('dark');document.querySelectorAll('.theme-toggle .theme-label').forEach(node=>{node.textContent=dark?'Light Mode':'Dark Mode';});};document.querySelectorAll('.theme-toggle').forEach(button=>{button.addEventListener('click',()=>{const dark=!document.documentElement.classList.contains('dark');document.documentElement.classList.toggle('dark',dark);try{localStorage.setItem('sahara_theme',dark?'dark':'light');}catch(err){}sync();});});sync();})();</script>"#
}

fn rfq_form_script() -> &'static str {
    r#"<script>for(const form of document.querySelectorAll('.rfq-form')){form.addEventListener('submit',async e=>{e.preventDefault();const status=form.querySelector('.rfq-status');status.className='rfq-status';status.textContent='Sending RFQ...';const data=Object.fromEntries(new FormData(form).entries());const company=form.dataset.company||'';const lei=form.dataset.lei||'';const profile=form.dataset.profile||'';const extra=[`Requested company: ${company}`,lei?`LEI: ${lei}`:'',`Profile: ${profile}`,data.message||''].filter(Boolean).join('\n');const payload={buyerName:data.buyerName,buyerCompany:data.buyerCompany||undefined,buyerEmail:data.buyerEmail,buyerPhone:data.buyerPhone||undefined,productNeeded:data.productNeeded,quantity:data.quantity||undefined,destinationCountry:data.destinationCountry||undefined,message:extra,preferredCountry:''};try{const res=await fetch(form.dataset.api,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});if(!res.ok){throw new Error(await res.text());}status.className='rfq-status ok';status.textContent='RFQ sent. SaharaIndex will route this request and follow up by email.';form.reset();}catch(err){status.className='rfq-status err';status.textContent='Could not send RFQ. Please use the main SaharaIndex RFQ page.';}})}</script>"#
}

fn site_search_script() -> &'static str {
    r#"<script>(()=>{const MAX_RESULTS=200;const controls={search:document.getElementById('search'),country:document.getElementById('filter-country'),industry:document.getElementById('filter-industry'),productCategory:document.getElementById('filter-product-category'),source:document.getElementById('filter-source'),typeFilter:document.getElementById('filter-type'),contactWebsite:document.getElementById('filter-contact-website'),contactEmail:document.getElementById('filter-contact-email'),contactPhone:document.getElementById('filter-contact-phone'),product:document.getElementById('filter-product'),service:document.getElementById('filter-service'),reset:document.getElementById('filter-reset'),summary:document.getElementById('filter-summary'),empty:document.getElementById('filter-empty')};const grid=document.getElementById('companies');const initialCards=[...document.querySelectorAll('.company-card')];let records=[];let loaded=false;if(!grid||!initialCards.length){return;}const splitTokens=(value)=>value?value.split('|'):[];const wants=()=>({query:(controls.search&&controls.search.value.toLowerCase().trim())||'',country:(controls.country&&controls.country.value)||'',industry:(controls.industry&&controls.industry.value)||'',productCategory:(controls.productCategory&&controls.productCategory.value)||'',source:(controls.source&&controls.source.value)||'',type:(controls.typeFilter&&controls.typeFilter.value)||'',website:!!(controls.contactWebsite&&controls.contactWebsite.checked),email:!!(controls.contactEmail&&controls.contactEmail.checked),phone:!!(controls.contactPhone&&controls.contactPhone.checked),product:!!(controls.product&&controls.product.checked),service:!!(controls.service&&controls.service.checked)});const cardMatches=(card,w)=>{if(w.query&&!card.dataset.search.toLowerCase().includes(w.query)){return false;}if(w.country&&card.dataset.country!==w.country){return false;}if(w.industry&&!splitTokens(card.dataset.industries).includes(w.industry)){return false;}if(w.productCategory&&!splitTokens(card.dataset.productCategories).includes(w.productCategory)){return false;}if(w.source&&card.dataset.source!==w.source){return false;}if(w.type&&card.dataset.companyType!==w.type){return false;}if(w.website&&card.dataset.hasWebsite!=='1'){return false;}if(w.email&&card.dataset.hasEmail!=='1'){return false;}if(w.phone&&card.dataset.hasPhone!=='1'){return false;}if(w.product&&card.dataset.hasProducts!=='1'){return false;}if(w.service&&card.dataset.hasServices!=='1'){return false;}return true;};const recordText=(record)=>[record.title,record.country,record.company_type,record.industries,record.description,record.lei||'',record.source].join(' ').toLowerCase();const recordMatches=(record,w)=>{if(w.query&&!record.fulltext.includes(w.query)){return false;}if(w.country&&record.country_token!==w.country){return false;}if(w.industry&&!record.industry_tokens.includes(w.industry)){return false;}if(w.productCategory&&!record.product_category_tokens.includes(w.productCategory)){return false;}if(w.source&&record.source_token!==w.source){return false;}if(w.type&&record.company_type_token!==w.type){return false;}if(w.website&&!record.has_website){return false;}if(w.email&&!record.has_email){return false;}if(w.phone&&!record.has_phone){return false;}if(w.product&&!record.has_products){return false;}if(w.service&&!record.has_services){return false;}return true;};const addText=(parent,tag,cls,text)=>{const node=document.createElement(tag);if(cls){node.className=cls;}node.textContent=text;parent.appendChild(node);return node;};const addFact=(dl,label,value)=>{if(!value){return;}addText(dl,'dt','',label);addText(dl,'dd','',value);};const renderRecord=(record)=>{const article=document.createElement('article');article.className='company-card';if(record.image_url){const media=document.createElement('a');media.className='card-media';media.href=record.url;const img=document.createElement('img');img.src=record.image_url;img.alt=`${record.title} profile image`;img.loading='lazy';media.appendChild(img);article.appendChild(media);}const badges=addText(article,'div','card-badges','');addText(badges,'span',record.lei?'badge ok':'badge muted',record.lei?'LEI verified':'Registry record');addText(badges,'span',(record.has_website||record.has_email||record.has_phone)?'badge ok':'badge muted',(record.has_website||record.has_email||record.has_phone)?'Company contact':'RFQ routing');const link=document.createElement('a');link.className='card-title';link.href=record.url;const h2=document.createElement('h2');h2.textContent=record.title;link.appendChild(h2);article.appendChild(link);addText(article,'p','meta',`${record.country} | ${record.company_type} | ${record.industries}`);addText(article,'p','',record.description);const dl=document.createElement('dl');dl.className='mini-facts';addFact(dl,'LEI',record.lei);addFact(dl,'Source',record.source);addFact(dl,'Contact',(record.has_website||record.has_email||record.has_phone)?'Public company contact':'RFQ routing');addFact(dl,'Products',record.has_products?'Available':'Not listed');addFact(dl,'Services',record.has_services?'Available':'Not listed');article.appendChild(dl);const open=document.createElement('a');open.className='text-link';open.href=record.url;open.textContent='Open profile';article.appendChild(open);return article;};const renderRecords=(matches)=>{const visible=matches.slice(0,MAX_RESULTS);const fragment=document.createDocumentFragment();for(const record of visible){fragment.appendChild(renderRecord(record));}grid.replaceChildren(fragment);if(controls.summary){controls.summary.textContent=`${matches.length} companies match directory filters. Showing first ${visible.length}.`;}if(controls.empty){controls.empty.hidden=matches.length>0;}};const applyFilters=()=>{const w=wants();if(loaded){renderRecords(records.filter(record=>recordMatches(record,w)));return;}let visible=0;for(const card of initialCards){const show=cardMatches(card,w);card.style.display=show?'':'none';if(show){visible+=1;}}if(controls.summary){controls.summary.textContent=`${visible} of ${initialCards.length} companies match this page filters. Loading full directory index...`;}if(controls.empty){controls.empty.hidden=visible>0;}};const onReset=()=>{if(controls.search){controls.search.value='';}[controls.country,controls.industry,controls.productCategory,controls.source,controls.typeFilter].forEach(filter=>{if(filter){filter.value='';}});[controls.contactWebsite,controls.contactEmail,controls.contactPhone,controls.product,controls.service].forEach(filter=>{if(filter){filter.checked=false;}});applyFilters();};for(const [control,event] of [[controls.search,'input'],[controls.country,'change'],[controls.industry,'change'],[controls.productCategory,'change'],[controls.source,'change'],[controls.typeFilter,'change'],[controls.contactWebsite,'change'],[controls.contactEmail,'change'],[controls.contactPhone,'change'],[controls.product,'change'],[controls.service,'change']]){if(control){control.addEventListener(event,applyFilters);}}if(controls.reset){controls.reset.addEventListener('click',onReset);}applyFilters();fetch('search-data.json',{headers:{Accept:'application/json'}}).then(response=>response.ok?response.json():Promise.reject(new Error('search-data unavailable'))).then(data=>{records=Array.isArray(data)?data.map(record=>({...record,fulltext:recordText(record)})):[];loaded=records.length>0;applyFilters();}).catch(()=>{if(controls.summary){controls.summary.textContent=`Full directory index unavailable. ${initialCards.length} current page companies can be filtered.`;}});})();</script>"#
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
    parts.push(display_company_name(profile));
    if let Some(description) = &profile.description {
        parts.push(clean_public_text(description));
    }
    parts.extend(
        profile
            .industries
            .iter()
            .map(|value| clean_public_text(value)),
    );
    parts.extend(
        profile
            .specializations
            .iter()
            .map(|value| clean_public_text(value)),
    );
    parts.extend(
        profile
            .products
            .iter()
            .map(|item| clean_public_text(&item.name)),
    );
    parts.extend(
        profile
            .services
            .iter()
            .map(|item| clean_public_text(&item.name)),
    );
    parts.extend(
        profile
            .evidence
            .iter()
            .map(|item| clean_public_text(&item.value)),
    );
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
