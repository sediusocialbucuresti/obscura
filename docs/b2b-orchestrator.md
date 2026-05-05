# Obscura B2B Orchestrator

`obscura-b2b` is a pipeline layer on top of the Obscura browser engine for building B2B company directories from authorized or public business listing sources.

It is designed for manufacturers, wholesalers, distributors, suppliers, importers, exporters, and related B2B entities across Europe and MENA. The scraper captures company profiles, product/service signals, business contact points, validation metadata, and export files for website directory and Mautic import workflows.

## Compliance Model

Use seed sources that you are allowed to process. The orchestrator records each source's `compliance_basis`, can enforce Obscura's robots.txt handling with `--obey-robots`, and marks personal contact data in validation flags.

Mautic exports include role-based emails by default. Personal-looking addresses are excluded unless `--include-personal-contacts` is passed, which should only be used after a lawful-basis and consent review.

## Seed Format

Create a seed file with directory or company-site sources:

```json
{
  "sources": [
    {
      "name": "Authorized Europe Manufacturers",
      "url": "https://example.com/europe/manufacturers",
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

`source_type` can be `directory` or `company_site`. Directory sources are first crawled for candidate company/profile links, then each candidate is processed as a company profile.

## Commands

```bash
cargo run -p obscura-b2b -- init --seeds b2b-seeds.json --out data/b2b

cargo run -p obscura-b2b -- run \
  --seeds b2b-seeds.json \
  --out data/b2b \
  --concurrency 25 \
  --max-pages 10000 \
  --timeout 45 \
  --delay-ms 500 \
  --obey-robots \
  --export

cargo run -p obscura-b2b -- run \
  --seeds b2b-seeds.json \
  --out data/b2b \
  --loop \
  --interval-seconds 86400 \
  --obey-robots \
  --export

cargo run -p obscura-b2b -- export --out data/b2b
```

## Output Layout

```text
data/b2b/
  jobs.jsonl
  crawl_attempts.jsonl
  company_profiles.jsonl
  raw_pages/
  directory/
    index.json
    search.json
    segments.json
    companies/<company-id>.json
  mautic/
    contacts.csv
    segments.json
    templates/claim-your-profile.md
```

`directory/index.json` powers listing pages. `directory/search.json` is a compact search document set. `directory/companies/*.json` contains individual company profiles. `mautic/contacts.csv` is formatted for Mautic contact import and segmentation.

## Production Notes

For very large runs, keep source lists narrow and explicit by region, industry, and company type. Run multiple orchestrator processes with disjoint seed files if you need region-level isolation. JSONL output is append-only for auditability; downstream jobs should read the latest record per `id` when publishing a website directory.
