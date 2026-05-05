use std::collections::BTreeSet;

use obscura_browser::Page;
use obscura_dom::{DomTree, NodeData, NodeId};
use url::Url;

use crate::models::{
    CatalogItem, CompanyProfile, ContactPoint, ContactSet, Evidence, Personnel, ScrapeJob,
    ValidationReport,
};
use crate::storage::{host_from_url, now_epoch, slugify};

pub fn extract_company_profile(page: &Page, job: &ScrapeJob) -> CompanyProfile {
    let profile_url = page.url_string();
    let canonical_domain = host_from_url(&profile_url);
    let scraped_at_epoch = now_epoch();
    let refresh_due_epoch = scraped_at_epoch + job.refresh_interval_days.saturating_mul(86_400);

    let extracted = page.with_dom(|dom| {
        let title = clean(&page.title);
        let meta_description = meta_content(
            dom,
            &["description", "og:description", "twitter:description"],
        );
        let site_name = meta_content(dom, &["og:site_name", "application-name"]);
        let h1 = first_text(dom, "h1");
        let body_text = readable_page_text(dom);
        let links = collect_links(dom, &profile_url);

        let company_name = first_non_empty(&[
            site_name,
            h1,
            Some(title.clone()),
            Some(job.source_name.clone()),
        ])
        .unwrap_or_else(|| job.source_name.clone());

        let description = meta_description.or_else(|| first_meaningful_line(&body_text, 180));
        let contacts = extract_contacts(dom, &body_text, &links, &profile_url);
        let addresses = extract_addresses(dom, &body_text);
        let products = extract_catalog_items(&links, &body_text, "product");
        let services = extract_catalog_items(&links, &body_text, "service");
        let company_size = find_line_with_any(
            &body_text,
            &["employees", "staff", "workforce", "team size"],
        );
        let revenue = find_line_with_any(&body_text, &["revenue", "turnover", "annual sales"]);
        let personnel = extract_personnel(&body_text, &profile_url);
        let industries = infer_industries(job.industry.as_deref(), &body_text);
        let company_type = infer_company_type(job.company_type.as_deref(), &body_text);
        let specializations = infer_specializations(&body_text);

        ExtractedPage {
            company_name,
            description,
            contacts,
            addresses,
            products,
            services,
            company_size,
            revenue,
            personnel,
            industries,
            company_type,
            specializations,
        }
    });

    let extracted = extracted.unwrap_or_else(|| ExtractedPage {
        company_name: job.source_name.clone(),
        description: None,
        contacts: ContactSet::default(),
        addresses: Vec::new(),
        products: Vec::new(),
        services: Vec::new(),
        company_size: None,
        revenue: None,
        personnel: Vec::new(),
        industries: job.industry.clone().into_iter().collect(),
        company_type: job.company_type.clone(),
        specializations: Vec::new(),
    });

    let mut evidence = Vec::new();
    evidence.push(Evidence {
        field: "profile_url".to_string(),
        value: profile_url.clone(),
        source_url: profile_url.clone(),
    });
    if let Some(description) = &extracted.description {
        evidence.push(Evidence {
            field: "description".to_string(),
            value: description.clone(),
            source_url: profile_url.clone(),
        });
    }

    let id_basis = if extracted.company_name.is_empty() {
        canonical_domain
            .clone()
            .unwrap_or_else(|| profile_url.clone())
    } else {
        extracted.company_name.clone()
    };

    CompanyProfile {
        id: slugify(&format!(
            "{} {}",
            id_basis,
            canonical_domain.clone().unwrap_or_default()
        )),
        source_name: job.source_name.clone(),
        source_url: job.source_url.clone(),
        profile_url,
        canonical_domain,
        company_name: extracted.company_name,
        description: extracted.description,
        region: job.region.clone(),
        country: job.country.clone(),
        company_type: extracted.company_type,
        industries: extracted.industries,
        specializations: extracted.specializations,
        products: extracted.products,
        services: extracted.services,
        contacts: extracted.contacts,
        addresses: extracted.addresses,
        company_size: extracted.company_size,
        revenue: extracted.revenue,
        personnel: extracted.personnel,
        evidence,
        validation: ValidationReport::default(),
        tags: job.tags.clone(),
        scraped_at_epoch,
        refresh_due_epoch,
    }
}

pub fn discover_company_links(
    page: &Page,
    source_url: &str,
    allowed_domains: &[String],
    limit: usize,
) -> Vec<String> {
    page.with_dom(|dom| {
        let links = collect_links(dom, source_url);
        let mut seen = BTreeSet::new();
        let mut out = Vec::new();

        for (href, text) in links {
            if out.len() >= limit {
                break;
            }
            if !is_allowed_domain(&href, source_url, allowed_domains) {
                continue;
            }
            if !is_company_candidate_link(&href, &text) {
                continue;
            }
            if seen.insert(href.clone()) {
                out.push(href);
            }
        }

        out
    })
    .unwrap_or_default()
}

pub fn page_html(page: &Page) -> Option<String> {
    page.with_dom(|dom| {
        if let Ok(Some(html)) = dom.query_selector("html") {
            dom.outer_html(html)
        } else {
            dom.inner_html(dom.document())
        }
    })
}

struct ExtractedPage {
    company_name: String,
    description: Option<String>,
    contacts: ContactSet,
    addresses: Vec<String>,
    products: Vec<CatalogItem>,
    services: Vec<CatalogItem>,
    company_size: Option<String>,
    revenue: Option<String>,
    personnel: Vec<Personnel>,
    industries: Vec<String>,
    company_type: Option<String>,
    specializations: Vec<String>,
}

fn meta_content(dom: &DomTree, names: &[&str]) -> Option<String> {
    let wanted: BTreeSet<String> = names.iter().map(|name| name.to_ascii_lowercase()).collect();
    for node_id in dom.query_selector_all("meta").unwrap_or_default() {
        let Some(node) = dom.get_node(node_id) else {
            continue;
        };
        let name = node
            .get_attribute("name")
            .or_else(|| node.get_attribute("property"))
            .unwrap_or("")
            .to_ascii_lowercase();
        if !wanted.contains(&name) {
            continue;
        }
        if let Some(value) = node
            .get_attribute("content")
            .map(clean)
            .filter(|s| !s.is_empty())
        {
            return Some(value);
        }
    }
    None
}

fn first_text(dom: &DomTree, selector: &str) -> Option<String> {
    dom.query_selector(selector)
        .ok()
        .flatten()
        .map(|id| clean(&dom.text_content(id)))
        .filter(|s| !s.is_empty())
}

fn readable_page_text(dom: &DomTree) -> String {
    if let Ok(Some(body)) = dom.query_selector("body") {
        let mut out = String::new();
        collect_readable_text(dom, body, &mut out);
        normalize_space(&out)
    } else {
        normalize_space(&dom.text_content(dom.document()))
    }
}

fn collect_readable_text(dom: &DomTree, node_id: NodeId, out: &mut String) {
    let Some(node) = dom.get_node(node_id) else {
        return;
    };
    match &node.data {
        NodeData::Text { contents } => {
            let text = clean(contents);
            if !text.is_empty() {
                if !out.ends_with(' ') && !out.ends_with('\n') && !out.is_empty() {
                    out.push(' ');
                }
                out.push_str(&text);
            }
        }
        NodeData::Element { name, .. } => {
            let tag = name.local.as_ref();
            if matches!(tag, "script" | "style" | "noscript" | "svg") {
                return;
            }
            let block = matches!(
                tag,
                "address"
                    | "article"
                    | "aside"
                    | "br"
                    | "dd"
                    | "div"
                    | "dl"
                    | "dt"
                    | "figcaption"
                    | "footer"
                    | "h1"
                    | "h2"
                    | "h3"
                    | "h4"
                    | "h5"
                    | "h6"
                    | "header"
                    | "hr"
                    | "li"
                    | "main"
                    | "nav"
                    | "ol"
                    | "p"
                    | "section"
                    | "table"
                    | "tbody"
                    | "td"
                    | "tfoot"
                    | "th"
                    | "thead"
                    | "tr"
                    | "ul"
            );
            if block && !out.ends_with('\n') {
                out.push('\n');
            }
            for child in dom.children(node_id) {
                collect_readable_text(dom, child, out);
            }
            if block && !out.ends_with('\n') {
                out.push('\n');
            }
        }
        _ => {
            for child in dom.children(node_id) {
                collect_readable_text(dom, child, out);
            }
        }
    }
}

fn collect_links(dom: &DomTree, base_url: &str) -> Vec<(String, String)> {
    let base = Url::parse(base_url).ok();
    let mut out = Vec::new();

    for node_id in dom.query_selector_all("a").unwrap_or_default() {
        let Some(node) = dom.get_node(node_id) else {
            continue;
        };
        let Some(href) = node.get_attribute("href") else {
            continue;
        };
        let href = href.trim();
        if href.is_empty() || href.starts_with('#') || href.starts_with("javascript:") {
            continue;
        }

        let full_url = if href.starts_with("mailto:") || href.starts_with("tel:") {
            href.to_string()
        } else if let Ok(url) = Url::parse(href) {
            url.to_string()
        } else if let Some(base) = &base {
            match base.join(href) {
                Ok(url) => url.to_string(),
                Err(_) => continue,
            }
        } else {
            continue;
        };
        let text = clean(&dom.text_content(node_id));
        out.push((full_url, text));
    }

    out
}

fn extract_contacts(
    dom: &DomTree,
    body_text: &str,
    links: &[(String, String)],
    source_url: &str,
) -> ContactSet {
    let mut contacts = ContactSet::default();
    let mut email_seen = BTreeSet::new();
    let mut phone_seen = BTreeSet::new();
    let mut website_seen = BTreeSet::new();
    let mut social_seen = BTreeSet::new();

    for (href, _) in links {
        if let Some(value) = href.strip_prefix("mailto:") {
            let email = value
                .split('?')
                .next()
                .unwrap_or("")
                .trim()
                .to_ascii_lowercase();
            if is_valid_email(&email) && email_seen.insert(email.clone()) {
                contacts.emails.push(contact_point(email, source_url, 0.95));
            }
            continue;
        }
        if let Some(value) = href.strip_prefix("tel:") {
            let phone = normalize_phone(value);
            if is_valid_phone(&phone) && phone_seen.insert(phone.clone()) {
                contacts.phones.push(ContactPoint {
                    value: phone,
                    kind: "phone".to_string(),
                    source_url: source_url.to_string(),
                    confidence: 0.95,
                    personal: false,
                });
            }
            continue;
        }
        if href.starts_with("http://") || href.starts_with("https://") {
            if is_social_link(href) {
                if social_seen.insert(href.clone()) {
                    contacts.social_links.push(href.clone());
                }
            } else if website_seen.insert(href.clone()) {
                contacts.websites.push(href.clone());
            }
        }
    }

    for email in extract_emails_from_text(body_text) {
        if email_seen.insert(email.clone()) {
            contacts.emails.push(contact_point(email, source_url, 0.75));
        }
    }

    for node_id in dom.query_selector_all("address").unwrap_or_default() {
        let text = clean(&dom.text_content(node_id));
        for phone in extract_phones_from_line(&text) {
            if phone_seen.insert(phone.clone()) {
                contacts.phones.push(ContactPoint {
                    value: phone,
                    kind: "phone".to_string(),
                    source_url: source_url.to_string(),
                    confidence: 0.8,
                    personal: false,
                });
            }
        }
    }

    for line in body_text
        .lines()
        .filter(|line| has_any(line, &["tel", "phone", "mobile", "+"]))
    {
        for phone in extract_phones_from_line(line) {
            if phone_seen.insert(phone.clone()) {
                contacts.phones.push(ContactPoint {
                    value: phone,
                    kind: "phone".to_string(),
                    source_url: source_url.to_string(),
                    confidence: 0.65,
                    personal: false,
                });
            }
        }
    }

    contacts
}

fn contact_point(email: String, source_url: &str, confidence: f32) -> ContactPoint {
    let local = email.split('@').next().unwrap_or("");
    ContactPoint {
        kind: if is_role_email(local) {
            "role_email"
        } else {
            "personal_email"
        }
        .to_string(),
        personal: !is_role_email(local),
        value: email,
        source_url: source_url.to_string(),
        confidence,
    }
}

fn extract_addresses(dom: &DomTree, body_text: &str) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();

    for node_id in dom.query_selector_all("address").unwrap_or_default() {
        let text = clean(&dom.text_content(node_id));
        if text.len() > 12 && seen.insert(text.clone()) {
            out.push(text);
        }
    }

    for line in body_text.lines().filter(|line| {
        has_any(
            line,
            &[
                "address",
                "head office",
                "registered office",
                "industrial zone",
            ],
        )
    }) {
        let value = clean(line);
        if value.len() > 20 && value.len() < 260 && seen.insert(value.clone()) {
            out.push(value);
        }
        if out.len() >= 5 {
            break;
        }
    }

    out
}

fn extract_catalog_items(
    links: &[(String, String)],
    body_text: &str,
    category: &str,
) -> Vec<CatalogItem> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    let words = if category == "product" {
        [
            "product",
            "catalog",
            "catalogue",
            "brand",
            "range",
            "collection",
            "category",
        ]
    } else {
        [
            "service",
            "solution",
            "capability",
            "offering",
            "logistics",
            "support",
            "consulting",
        ]
    };

    for (href, text) in links {
        let haystack = format!(
            "{} {}",
            href.to_ascii_lowercase(),
            text.to_ascii_lowercase()
        );
        if !words.iter().any(|word| haystack.contains(word)) {
            continue;
        }
        let name = if text.is_empty() {
            href.rsplit('/')
                .find(|part| !part.is_empty())
                .unwrap_or(category)
        } else {
            text
        };
        let name = clean(name);
        if name.len() < 3 || !seen.insert(name.to_ascii_lowercase()) {
            continue;
        }
        out.push(CatalogItem {
            name,
            description: None,
            url: Some(href.clone()),
            category: Some(category.to_string()),
        });
        if out.len() >= 25 {
            return out;
        }
    }

    for line in body_text.lines().filter(|line| has_any(line, &words)) {
        let value = clean(line);
        if value.len() < 8 || value.len() > 180 || !seen.insert(value.to_ascii_lowercase()) {
            continue;
        }
        out.push(CatalogItem {
            name: value,
            description: None,
            url: None,
            category: Some(category.to_string()),
        });
        if out.len() >= 25 {
            break;
        }
    }

    out
}

fn extract_personnel(body_text: &str, source_url: &str) -> Vec<Personnel> {
    let titles = [
        "ceo",
        "chief executive",
        "managing director",
        "general manager",
        "founder",
        "owner",
        "sales director",
        "export manager",
        "procurement manager",
        "commercial director",
    ];
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();

    for line in body_text.lines() {
        let clean_line = clean(line);
        let lower = clean_line.to_ascii_lowercase();
        let Some(title) = titles.iter().find(|title| lower.contains(**title)) else {
            continue;
        };
        if clean_line.len() < 8 || clean_line.len() > 220 || !seen.insert(clean_line.clone()) {
            continue;
        }
        out.push(Personnel {
            name: guess_name_near_title(&clean_line, title),
            title: title.to_string(),
            source_text: clean_line,
            source_url: source_url.to_string(),
        });
        if out.len() >= 10 {
            break;
        }
    }

    out
}

fn infer_industries(seed: Option<&str>, body_text: &str) -> Vec<String> {
    let mut out = BTreeSet::new();
    if let Some(seed) = seed.map(clean).filter(|s| !s.is_empty()) {
        out.insert(seed);
    }
    let mapping = [
        ("food", "Food and beverage"),
        ("textile", "Textiles"),
        ("machinery", "Machinery"),
        ("chemical", "Chemicals"),
        ("packaging", "Packaging"),
        ("construction", "Construction"),
        ("electronics", "Electronics"),
        ("medical", "Medical supplies"),
        ("automotive", "Automotive"),
        ("logistics", "Logistics"),
        ("agriculture", "Agriculture"),
        ("metal", "Metals"),
        ("plastic", "Plastics"),
        ("pharmaceutical", "Pharmaceuticals"),
    ];
    let lower = body_text.to_ascii_lowercase();
    for (needle, label) in mapping {
        if lower.contains(needle) {
            out.insert(label.to_string());
        }
    }
    out.into_iter().take(8).collect()
}

fn infer_company_type(seed: Option<&str>, body_text: &str) -> Option<String> {
    if let Some(seed) = seed.map(clean).filter(|s| !s.is_empty()) {
        return Some(seed.to_ascii_lowercase());
    }
    let lower = body_text.to_ascii_lowercase();
    for (needle, label) in [
        ("manufacturer", "manufacturer"),
        ("factory", "manufacturer"),
        ("producer", "manufacturer"),
        ("wholesaler", "wholesaler"),
        ("wholesale", "wholesaler"),
        ("distributor", "distributor"),
        ("distribution", "distributor"),
        ("supplier", "supplier"),
        ("importer", "importer"),
        ("exporter", "exporter"),
        ("trading", "trading company"),
    ] {
        if lower.contains(needle) {
            return Some(label.to_string());
        }
    }
    None
}

fn infer_specializations(body_text: &str) -> Vec<String> {
    let mut out = BTreeSet::new();
    for line in body_text.lines() {
        let lower = line.to_ascii_lowercase();
        if !has_any(
            &lower,
            &[
                "specializ",
                "expertise",
                "certified",
                "iso ",
                "oem",
                "private label",
            ],
        ) {
            continue;
        }
        let value = clean(line);
        if value.len() >= 8 && value.len() <= 160 {
            out.insert(value);
        }
        if out.len() >= 12 {
            break;
        }
    }
    out.into_iter().collect()
}

fn find_line_with_any(body_text: &str, needles: &[&str]) -> Option<String> {
    body_text
        .lines()
        .map(clean)
        .find(|line| line.len() <= 180 && has_any(line, needles))
}

fn first_meaningful_line(body_text: &str, max_len: usize) -> Option<String> {
    body_text.lines().map(clean).find(|line| {
        line.len() >= 40 && line.len() <= max_len && !has_any(line, &["cookie", "privacy", "terms"])
    })
}

fn first_non_empty(values: &[Option<String>]) -> Option<String> {
    values
        .iter()
        .flatten()
        .map(|s| clean(s))
        .find(|s| !s.is_empty())
}

fn is_company_candidate_link(href: &str, text: &str) -> bool {
    let haystack = format!(
        "{} {}",
        href.to_ascii_lowercase(),
        text.to_ascii_lowercase()
    );
    has_any(
        &haystack,
        &[
            "company",
            "manufacturer",
            "producer",
            "supplier",
            "distributor",
            "wholesaler",
            "factory",
            "profile",
            "member",
            "exporter",
            "importer",
            "products",
            "catalog",
            "contact",
            "about",
        ],
    )
}

fn is_allowed_domain(href: &str, source_url: &str, allowed_domains: &[String]) -> bool {
    let Some(host) = host_from_url(href) else {
        return false;
    };
    if allowed_domains.is_empty() {
        return host_from_url(source_url)
            .map(|source_host| source_host == host)
            .unwrap_or(false);
    }
    allowed_domains.iter().any(|allowed| {
        let allowed = allowed.trim_start_matches("www.").to_ascii_lowercase();
        host == allowed || host.ends_with(&format!(".{}", allowed))
    })
}

fn extract_emails_from_text(text: &str) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();

    for token in text.split(|ch: char| {
        !(ch.is_ascii_alphanumeric() || matches!(ch, '@' | '.' | '_' | '%' | '+' | '-'))
    }) {
        let email = token
            .trim_matches(|ch: char| matches!(ch, '.' | ',' | ';' | ':' | ')' | '(' | '[' | ']'))
            .to_ascii_lowercase();
        if is_valid_email(&email) && seen.insert(email.clone()) {
            out.push(email);
        }
    }

    out
}

fn extract_phones_from_line(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();

    for ch in line.chars() {
        if ch.is_ascii_digit() || matches!(ch, '+' | ' ' | '-' | '(' | ')') {
            current.push(ch);
        } else {
            push_phone(&mut out, &mut current);
        }
    }
    push_phone(&mut out, &mut current);
    out
}

fn push_phone(out: &mut Vec<String>, current: &mut String) {
    let phone = normalize_phone(current);
    current.clear();
    if is_valid_phone(&phone) && !out.contains(&phone) {
        out.push(phone);
    }
}

fn normalize_phone(value: &str) -> String {
    clean(
        &value
            .chars()
            .filter(|ch| ch.is_ascii_digit() || matches!(ch, '+' | ' ' | '-' | '(' | ')'))
            .collect::<String>(),
    )
}

fn is_valid_phone(value: &str) -> bool {
    let digits = value.chars().filter(|ch| ch.is_ascii_digit()).count();
    (7..=18).contains(&digits)
}

fn is_valid_email(value: &str) -> bool {
    let Some((local, domain)) = value.split_once('@') else {
        return false;
    };
    !local.is_empty()
        && local.len() <= 64
        && domain.contains('.')
        && !domain.starts_with('.')
        && !domain.ends_with('.')
        && !value.ends_with(".png")
        && !value.ends_with(".jpg")
        && !value.ends_with(".jpeg")
        && !value.ends_with(".gif")
        && value.is_ascii()
}

fn is_role_email(local: &str) -> bool {
    matches!(
        local,
        "info"
            | "sales"
            | "contact"
            | "hello"
            | "office"
            | "support"
            | "service"
            | "customerservice"
            | "customer.service"
            | "marketing"
            | "export"
            | "exports"
            | "wholesale"
            | "distribution"
            | "distributor"
            | "admin"
            | "inquiries"
            | "enquiry"
            | "enquiries"
    )
}

fn is_social_link(href: &str) -> bool {
    has_any(
        href,
        &[
            "linkedin.com",
            "facebook.com",
            "instagram.com",
            "x.com",
            "twitter.com",
            "youtube.com",
        ],
    )
}

fn guess_name_near_title(line: &str, title: &str) -> Option<String> {
    let lower = line.to_ascii_lowercase();
    let idx = lower.find(title)?;
    let before = clean(&line[..idx]);
    let before = before
        .trim_matches(|ch: char| matches!(ch, '-' | ':' | '|' | ',' | ';'))
        .trim();
    if before.split_whitespace().count() >= 2 && before.len() <= 80 {
        Some(before.to_string())
    } else {
        None
    }
}

fn has_any(value: &str, needles: &[&str]) -> bool {
    let lower = value.to_ascii_lowercase();
    needles
        .iter()
        .any(|needle| lower.contains(&needle.to_ascii_lowercase()))
}

fn clean(value: &str) -> String {
    normalize_space(value).trim().to_string()
}

fn normalize_space(value: &str) -> String {
    let mut out = String::new();
    let mut last_space = false;
    let mut last_newline = false;

    for ch in value.chars() {
        if ch == '\n' || ch == '\r' {
            if !last_newline && !out.is_empty() {
                out.push('\n');
            }
            last_newline = true;
            last_space = false;
        } else if ch.is_whitespace() {
            if !last_space && !last_newline && !out.is_empty() {
                out.push(' ');
            }
            last_space = true;
        } else {
            out.push(ch);
            last_space = false;
            last_newline = false;
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_basic_emails() {
        assert_eq!(
            extract_emails_from_text("Contact sales@example.com now"),
            vec!["sales@example.com"]
        );
    }

    #[test]
    fn classifies_role_email() {
        let point = contact_point("info@example.com".to_string(), "https://example.com", 1.0);
        assert!(!point.personal);
        assert_eq!(point.kind, "role_email");
    }

    #[test]
    fn normalizes_phone_candidates() {
        assert!(extract_phones_from_line("Tel: +49 (0) 30 1234567")
            .contains(&"+49 (0) 30 1234567".to_string()));
    }
}
