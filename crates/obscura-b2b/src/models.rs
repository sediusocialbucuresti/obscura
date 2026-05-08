use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceFile {
    pub sources: Vec<DirectorySource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectorySource {
    pub name: String,
    pub url: String,
    #[serde(default = "default_source_type")]
    pub source_type: String,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub country: Option<String>,
    #[serde(default)]
    pub industry: Option<String>,
    #[serde(default)]
    pub company_type: Option<String>,
    #[serde(default)]
    pub allowed_domains: Vec<String>,
    #[serde(default = "default_refresh_interval_days")]
    pub refresh_interval_days: u64,
    #[serde(default = "default_source_max_pages")]
    pub max_pages: usize,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default = "default_compliance_basis")]
    pub compliance_basis: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrapeJob {
    pub id: String,
    pub source_name: String,
    pub source_url: String,
    pub url: String,
    pub job_type: String,
    pub region: Option<String>,
    pub country: Option<String>,
    pub industry: Option<String>,
    pub company_type: Option<String>,
    pub tags: Vec<String>,
    pub compliance_basis: String,
    pub scheduled_at_epoch: u64,
    pub refresh_interval_days: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompanyProfile {
    pub id: String,
    pub source_name: String,
    pub source_url: String,
    pub profile_url: String,
    pub canonical_domain: Option<String>,
    pub company_name: String,
    pub description: Option<String>,
    pub region: Option<String>,
    pub country: Option<String>,
    pub company_type: Option<String>,
    pub industries: Vec<String>,
    pub specializations: Vec<String>,
    pub products: Vec<CatalogItem>,
    pub services: Vec<CatalogItem>,
    #[serde(default)]
    pub images: Vec<MediaAsset>,
    pub contacts: ContactSet,
    pub addresses: Vec<String>,
    pub company_size: Option<String>,
    pub revenue: Option<String>,
    pub personnel: Vec<Personnel>,
    pub evidence: Vec<Evidence>,
    pub validation: ValidationReport,
    pub tags: Vec<String>,
    pub scraped_at_epoch: u64,
    pub refresh_due_epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogItem {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub category: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaAsset {
    pub url: String,
    #[serde(default)]
    pub alt: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub source_url: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ContactSet {
    pub emails: Vec<ContactPoint>,
    pub phones: Vec<ContactPoint>,
    pub websites: Vec<String>,
    pub social_links: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContactPoint {
    pub value: String,
    pub kind: String,
    pub source_url: String,
    pub confidence: f32,
    pub personal: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Personnel {
    pub name: Option<String>,
    pub title: String,
    pub source_text: String,
    pub source_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Evidence {
    pub field: String,
    pub value: String,
    pub source_url: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ValidationReport {
    pub status: String,
    pub score: u8,
    pub issues: Vec<String>,
    pub compliance_flags: Vec<String>,
    pub field_coverage: Vec<String>,
}

pub fn default_source_type() -> String {
    "company_site".to_string()
}

pub fn default_refresh_interval_days() -> u64 {
    30
}

pub fn default_source_max_pages() -> usize {
    250
}

pub fn default_compliance_basis() -> String {
    "public_business_directory_review_required".to_string()
}
