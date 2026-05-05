# B2B Production Runbook

This runbook describes how to operate `obscura-b2b` for MENA and Europe B2B directory extraction.

## 1. Prepare Sources

Create or edit `b2b-seeds.json`.

Each source must be a business directory, company website, manufacturer association, wholesaler listing, distributor listing, marketplace profile index, or another source you are authorized to process.

Minimum fields:

```json
{
  "sources": [
    {
      "name": "Example Authorized Directory",
      "url": "https://example.com/manufacturers",
      "source_type": "directory",
      "region": "Europe",
      "country": "Germany",
      "industry": "Machinery",
      "company_type": "manufacturer",
      "allowed_domains": ["example.com"],
      "refresh_interval_days": 30,
      "max_pages": 500,
      "tags": ["europe", "manufacturer"],
      "compliance_basis": "authorized_source_or_public_business_listing"
    }
  ]
}
```

Use `source_type: "directory"` when the URL is a listing page that links to companies.

Use `source_type: "company_site"` when the URL is already a specific company profile or company website.

## 2. Initialize The Workspace

```bash
cargo run -p obscura-b2b -- init --seeds b2b-seeds.json --out data/b2b
```

This creates a seed template and output directories.

## 3. Run A Small Validation Crawl

Start with a small crawl before scaling:

```bash
cargo run -p obscura-b2b -- run \
  --seeds b2b-seeds.json \
  --out data/b2b \
  --concurrency 3 \
  --max-pages 25 \
  --timeout 45 \
  --delay-ms 1000 \
  --obey-robots \
  --export
```

Review:

```text
data/b2b/crawl_attempts.jsonl
data/b2b/company_profiles.jsonl
data/b2b/directory/index.json
data/b2b/mautic/contacts.csv
```

## 4. Scale The Crawl

After validating the source quality:

```bash
cargo run -p obscura-b2b -- run \
  --seeds b2b-seeds.json \
  --out data/b2b \
  --concurrency 25 \
  --max-pages 10000 \
  --timeout 45 \
  --delay-ms 500 \
  --obey-robots \
  --export
```

Recommended operating limits:

- Use a lower concurrency for small directories.
- Use region-specific seed files for isolation.
- Keep `--delay-ms` high enough to avoid hammering any one domain.
- Keep `--obey-robots` enabled unless there is a documented reason not to.

## 5. Schedule Updates

The simplest schedule is loop mode:

```bash
cargo run -p obscura-b2b -- run \
  --seeds b2b-seeds.json \
  --out data/b2b \
  --loop \
  --interval-seconds 86400 \
  --obey-robots \
  --export
```

For production, prefer systemd, Kubernetes CronJob, Nomad periodic jobs, or another external scheduler so process restarts and logs are managed outside the scraper.

## 6. Publish Directory Data

Consume these files in the website:

```text
data/b2b/directory/index.json
data/b2b/directory/search.json
data/b2b/directory/segments.json
data/b2b/directory/companies/*.json
```

Recommended website behavior:

- Use `index.json` for company listing pages.
- Use `search.json` for search indexing.
- Use `segments.json` for filters by region, country, industry, and company type.
- Use each `companies/*.json` file as the source of truth for individual profile pages.
- Display validation status internally first; only publish `ready` profiles automatically.
- Send `review` and `low_confidence` profiles to a manual curation queue.

## 7. Import Into Mautic

Import:

```text
data/b2b/mautic/contacts.csv
```

Suggested Mautic mapping:

```text
email -> Email
firstname -> First Name
lastname -> Last Name
company -> Company
phone -> Phone
website -> Website
region -> custom field: Region
country -> custom field: Country
industry -> custom field: Industry
company_type -> custom field: Company Type
tags -> Tags
profile_url -> custom field: Profile URL
claim_url -> custom field: Claim URL
```

Use the generated template:

```text
data/b2b/mautic/templates/claim-your-profile.md
```

Default CSV export excludes personal-looking email addresses. Only use:

```bash
--include-personal-contacts
```

after a consent and lawful-basis review.

## 8. Operational Checks

Before each large run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --release --workspace
```

During a run, watch:

```text
data/b2b/crawl_attempts.jsonl
data/b2b/company_profiles.jsonl
```

After a run, inspect:

```text
data/b2b/directory/segments.json
data/b2b/mautic/contacts.csv
```

## 9. Future Hardening

Recommended next steps for very high volume:

- Add Postgres for canonical companies, jobs, attempts, and export batches.
- Add Redis or another queue for distributed workers.
- Add source-specific extractors for known directories.
- Add stricter dedupe by domain, company name, country, phone, and email.
- Add enrichment and verification jobs as separate stages.
- Add CI for `cargo fmt`, `cargo clippy`, and `cargo test`.
