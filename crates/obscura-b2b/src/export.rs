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
}

pub async fn export_outputs(
    root: impl AsRef<Path>,
    include_personal_contacts: bool,
) -> anyhow::Result<usize> {
    let layout = StorageLayout::prepare(root).await?;
    let mut profiles: Vec<CompanyProfile> = read_jsonl(&layout.profiles_jsonl).await?;
    profiles.sort_by(|a, b| {
        a.company_name
            .to_ascii_lowercase()
            .cmp(&b.company_name.to_ascii_lowercase())
    });
    profiles.dedup_by(|a, b| a.id == b.id);
    reset_dir(&layout.companies_dir).await?;

    let mut index = Vec::new();
    let mut search = Vec::new();
    let mut segments = SegmentSummary {
        by_region: BTreeMap::new(),
        by_country: BTreeMap::new(),
        by_industry: BTreeMap::new(),
        by_company_type: BTreeMap::new(),
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

async fn write_static_site(
    layout: &StorageLayout,
    profiles: &[CompanyProfile],
) -> anyhow::Result<()> {
    let site_dir = layout.root.join("site");
    let site_companies_dir = site_dir.join("companies");
    reset_dir(&site_companies_dir).await?;

    write_text(site_dir.join("styles.css"), site_css()).await?;

    let mut index = String::new();
    index.push_str("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">");
    index.push_str("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">");
    index.push_str("<title>B2B Company Directory</title>");
    index.push_str("<link rel=\"stylesheet\" href=\"styles.css\"></head><body>");
    index.push_str("<header><h1>B2B Company Directory</h1>");
    let _ = write!(
        index,
        "<p>{} public company profiles prepared for review and enrichment.</p>",
        profiles.len()
    );
    index.push_str("<input id=\"search\" type=\"search\" placeholder=\"Search companies, countries, industries\" aria-label=\"Search companies\"></header>");
    index.push_str("<main><section class=\"grid\" id=\"companies\">");

    for profile in profiles {
        let file_stem = safe_file_stem(&profile.id);
        let href = format!("companies/{}.html", file_stem);
        let description = profile.description.as_deref().unwrap_or("");
        let country = profile.country.as_deref().unwrap_or("Unknown country");
        let company_type = profile.company_type.as_deref().unwrap_or("B2B entity");
        let industries = if profile.industries.is_empty() {
            "Unclassified".to_string()
        } else {
            profile.industries.join(", ")
        };
        let _ = write!(
            index,
            "<article class=\"card\" data-search=\"{}\"><a href=\"{}\"><h2>{}</h2></a><p class=\"meta\">{} · {} · {}</p><p>{}</p></article>",
            html_attr(&format!(
                "{} {} {} {} {}",
                profile.company_name,
                country,
                company_type,
                industries,
                description
            )),
            html_attr(&href),
            html(&profile.company_name),
            html(country),
            html(company_type),
            html(&industries),
            html(description)
        );
    }

    index.push_str("</section></main>");
    index.push_str(site_search_script());
    index.push_str("</body></html>");
    write_text(site_dir.join("index.html"), &index).await?;

    for profile in profiles {
        let file_stem = safe_file_stem(&profile.id);
        let page = company_html(profile);
        write_text(
            site_companies_dir.join(format!("{}.html", file_stem)),
            &page,
        )
        .await?;
    }

    Ok(())
}

fn company_html(profile: &CompanyProfile) -> String {
    let mut out = String::new();
    out.push_str("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">");
    out.push_str("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">");
    let _ = write!(out, "<title>{}</title>", html(&profile.company_name));
    out.push_str("<link rel=\"stylesheet\" href=\"../styles.css\"></head><body>");
    out.push_str("<header class=\"profile-header\"><a href=\"../index.html\">Directory</a>");
    let _ = write!(out, "<h1>{}</h1>", html(&profile.company_name));
    if let Some(description) = &profile.description {
        let _ = write!(out, "<p>{}</p>", html(description));
    }
    out.push_str("</header><main class=\"profile\">");

    out.push_str("<section><h2>Company</h2><dl>");
    field(&mut out, "Region", profile.region.as_deref());
    field(&mut out, "Country", profile.country.as_deref());
    field(&mut out, "Type", profile.company_type.as_deref());
    field(&mut out, "Domain", profile.canonical_domain.as_deref());
    field(&mut out, "Source", Some(&profile.source_name));
    field(&mut out, "Source URL", Some(&profile.profile_url));
    out.push_str("</dl></section>");

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

fn field(out: &mut String, label: &str, value: Option<&str>) {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        let _ = write!(out, "<dt>{}</dt><dd>{}</dd>", html(label), html(value));
    }
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

fn html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn html_attr(value: &str) -> String {
    html(value).replace('"', "&quot;")
}

fn site_css() -> &'static str {
    r#"body{margin:0;font-family:Inter,Arial,sans-serif;background:#f6f7f9;color:#18202a}header{padding:24px 32px;background:#fff;border-bottom:1px solid #d9dde3}h1{margin:0 0 8px;font-size:28px}h2{font-size:18px;margin:0 0 8px}p{line-height:1.45}main{padding:24px 32px}.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:14px}.card,section{background:#fff;border:1px solid #d9dde3;border-radius:8px;padding:16px}.card a{color:#0b5cab;text-decoration:none}.meta{color:#5b6675;font-size:13px}.profile{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:16px}.profile-header a{display:inline-block;margin-bottom:12px;color:#0b5cab}dl{display:grid;grid-template-columns:120px 1fr;gap:8px 12px}dt{font-weight:700;color:#394454}dd{margin:0}input[type=search]{width:100%;max-width:560px;margin-top:12px;padding:10px 12px;border:1px solid #bdc5d1;border-radius:6px;font-size:15px}@media(max-width:700px){header,main{padding:18px}.grid{grid-template-columns:1fr}dl{grid-template-columns:1fr}}"#
}

fn site_search_script() -> &'static str {
    r#"<script>const s=document.getElementById('search');const cards=[...document.querySelectorAll('.card')];s.addEventListener('input',()=>{const q=s.value.toLowerCase().trim();for(const c of cards){c.style.display=!q||c.dataset.search.toLowerCase().includes(q)?'':'none';}});</script>"#
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
    parts.join(" ")
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
    for profile in profiles {
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
