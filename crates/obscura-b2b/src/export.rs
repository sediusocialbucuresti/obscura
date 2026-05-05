use std::collections::{BTreeMap, BTreeSet};
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

    let mut index = Vec::new();
    let mut search = Vec::new();
    let mut segments = SegmentSummary {
        by_region: BTreeMap::new(),
        by_country: BTreeMap::new(),
        by_industry: BTreeMap::new(),
        by_company_type: BTreeMap::new(),
    };

    for profile in &profiles {
        let filename = format!("{}.json", profile.id);
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

    Ok(profiles.len())
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
}
