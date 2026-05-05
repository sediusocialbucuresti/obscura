use std::path::Path;

use anyhow::Context;

use crate::models::{DirectorySource, SourceFile};

pub async fn load_sources(path: impl AsRef<Path>) -> anyhow::Result<Vec<DirectorySource>> {
    let raw = tokio::fs::read_to_string(path.as_ref())
        .await
        .with_context(|| format!("failed to read seed file {}", path.as_ref().display()))?;
    let trimmed = raw.trim_start();

    if trimmed.starts_with('[') {
        Ok(serde_json::from_str::<Vec<DirectorySource>>(&raw)?)
    } else {
        Ok(serde_json::from_str::<SourceFile>(&raw)?.sources)
    }
}

pub async fn write_example_sources(path: impl AsRef<Path>) -> anyhow::Result<()> {
    if let Some(parent) = path.as_ref().parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let sample = SourceFile {
        sources: vec![
            DirectorySource {
                name: "Example Europe Manufacturer".to_string(),
                url: "https://example.com/europe/manufacturers".to_string(),
                source_type: "directory".to_string(),
                region: Some("Europe".to_string()),
                country: None,
                industry: Some("Industrial manufacturing".to_string()),
                company_type: Some("manufacturer".to_string()),
                allowed_domains: vec!["example.com".to_string()],
                refresh_interval_days: 30,
                max_pages: 100,
                tags: vec!["europe".to_string(), "manufacturer".to_string()],
                compliance_basis: "authorized_source_or_public_business_listing".to_string(),
            },
            DirectorySource {
                name: "Example MENA Distributor".to_string(),
                url: "https://example.org/mena/distributors".to_string(),
                source_type: "directory".to_string(),
                region: Some("MENA".to_string()),
                country: None,
                industry: Some("Wholesale distribution".to_string()),
                company_type: Some("distributor".to_string()),
                allowed_domains: vec!["example.org".to_string()],
                refresh_interval_days: 30,
                max_pages: 100,
                tags: vec!["mena".to_string(), "distributor".to_string()],
                compliance_basis: "authorized_source_or_public_business_listing".to_string(),
            },
        ],
    };

    let bytes = serde_json::to_vec_pretty(&sample)?;
    tokio::fs::write(path.as_ref(), bytes).await?;
    Ok(())
}
