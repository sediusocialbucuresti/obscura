use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;
use url::Url;

pub struct StorageLayout {
    pub root: PathBuf,
    pub raw_pages_dir: PathBuf,
    pub directory_dir: PathBuf,
    pub companies_dir: PathBuf,
    pub mautic_dir: PathBuf,
    pub templates_dir: PathBuf,
    pub jobs_jsonl: PathBuf,
    pub profiles_jsonl: PathBuf,
    pub attempts_jsonl: PathBuf,
}

impl StorageLayout {
    pub async fn prepare(root: impl AsRef<Path>) -> anyhow::Result<Self> {
        let root = root.as_ref().to_path_buf();
        let raw_pages_dir = root.join("raw_pages");
        let directory_dir = root.join("directory");
        let companies_dir = directory_dir.join("companies");
        let mautic_dir = root.join("mautic");
        let templates_dir = mautic_dir.join("templates");

        for dir in [
            &root,
            &raw_pages_dir,
            &directory_dir,
            &companies_dir,
            &mautic_dir,
            &templates_dir,
        ] {
            tokio::fs::create_dir_all(dir)
                .await
                .with_context(|| format!("failed to create {}", dir.display()))?;
        }

        Ok(Self {
            jobs_jsonl: root.join("jobs.jsonl"),
            profiles_jsonl: root.join("company_profiles.jsonl"),
            attempts_jsonl: root.join("crawl_attempts.jsonl"),
            root,
            raw_pages_dir,
            directory_dir,
            companies_dir,
            mautic_dir,
            templates_dir,
        })
    }
}

pub struct JsonlWriter {
    file: Mutex<tokio::fs::File>,
}

impl JsonlWriter {
    pub async fn create(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        if let Some(parent) = path.as_ref().parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path.as_ref())
            .await
            .with_context(|| format!("failed to open {}", path.as_ref().display()))?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }

    pub async fn append<T: Serialize>(&self, value: &T) -> anyhow::Result<()> {
        let mut line = serde_json::to_vec(value)?;
        line.push(b'\n');
        let mut file = self.file.lock().await;
        file.write_all(&line).await?;
        file.flush().await?;
        Ok(())
    }
}

pub async fn read_jsonl<T: DeserializeOwned>(path: impl AsRef<Path>) -> anyhow::Result<Vec<T>> {
    if !path.as_ref().exists() {
        return Ok(Vec::new());
    }

    let file = tokio::fs::File::open(path.as_ref()).await?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    let mut out = Vec::new();

    loop {
        line.clear();
        let bytes = reader.read_line(&mut line).await?;
        if bytes == 0 {
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        out.push(serde_json::from_str(trimmed)?);
    }

    Ok(out)
}

pub async fn write_json_pretty<T: Serialize>(
    path: impl AsRef<Path>,
    value: &T,
) -> anyhow::Result<()> {
    if let Some(parent) = path.as_ref().parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let bytes = serde_json::to_vec_pretty(value)?;
    tokio::fs::write(path.as_ref(), bytes).await?;
    Ok(())
}

pub async fn write_text(path: impl AsRef<Path>, value: &str) -> anyhow::Result<()> {
    if let Some(parent) = path.as_ref().parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(path.as_ref(), value).await?;
    Ok(())
}

pub fn now_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub fn host_from_url(value: &str) -> Option<String> {
    Url::parse(value).ok().and_then(|url| {
        url.host_str()
            .map(|host| host.trim_start_matches("www.").to_string())
    })
}

pub fn slugify(value: &str) -> String {
    let mut slug = String::new();
    let mut last_dash = false;

    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
            last_dash = false;
        } else if !last_dash && !slug.is_empty() {
            slug.push('-');
            last_dash = true;
        }
    }

    while slug.ends_with('-') {
        slug.pop();
    }

    if slug.is_empty() {
        "company".to_string()
    } else {
        slug
    }
}

pub fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slugify_keeps_ascii_tokens() {
        assert_eq!(slugify("ACME Export GmbH / MENA"), "acme-export-gmbh-mena");
    }

    #[test]
    fn csv_escape_quotes_values_when_needed() {
        assert_eq!(csv_escape("A, B"), "\"A, B\"");
        assert_eq!(csv_escape("plain"), "plain");
    }
}
