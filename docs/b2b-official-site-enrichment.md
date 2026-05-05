# B2B Official-Site Enrichment

Use official company websites to enrich baseline profiles with buyer-facing contact signals.

## Scope

The enrichment tool crawls only verified official company websites supplied in a CSV or already stored on profiles. It does not discover websites through search engine scraping, social networks, or marketplaces.

Extracted fields:

- Role-based sales/commercial/export emails such as `sales@`, `export@`, and `commercial@`.
- Public sales/commercial personnel names and titles when listed on the official website.
- Phones.
- Product, service, catalogue, dealer, and distributor links.
- Official website evidence and source pages.

Personal-looking emails are stored as `personal_email` and are excluded from Mautic by default. Mautic export still requires campaign readiness gates.

## Input CSV

Create a CSV like:

```csv
profile_id,company_name,website
energy-plus-2138009ds9mxxsdyrw46,ENERGY PLUS,https://example-company.example
```

`profile_id` is preferred. If omitted, the tool matches by normalized `company_name`.

Template:

```sh
cp examples/b2b-official-websites.csv data/b2b/enrichment/official-websites.csv
```

## Run

Canary batch:

```sh
python3 tools/b2b_official_site_enricher.py \
  --out data/b2b \
  --websites-csv data/b2b/enrichment/official-websites.csv \
  --limit 250 \
  --pages-per-site 6 \
  --delay 1.5
```

Dry run:

```sh
python3 tools/b2b_official_site_enricher.py \
  --out data/b2b \
  --websites-csv data/b2b/enrichment/official-websites.csv \
  --limit 25 \
  --dry-run
```

Export after enrichment:

```sh
cargo run -p obscura-b2b -- export --out data/b2b
```

## Outputs

```text
data/b2b/enrichment/official-site-enrichment.jsonl
data/b2b/company_profiles.jsonl
```

The tool appends enriched profile revisions to `company_profiles.jsonl`. The exporter now uses latest profile by `id`, so later enrichment records override older baseline records during website export.

## Campaign Readiness

By default, enriched profiles remain blocked from automatic campaign export with `mautic_export_not_campaign_ready`.

To allow a profile to become `ready` when it has a role email and official website, pass:

```sh
--mark-ready-with-role-email
```

Use that only after source and lawful-basis review. Personal contacts remain excluded unless the separate `--include-personal-contacts` export option is used.

## Operating Rules

- Use only official company-owned websites or explicitly permitted source pages.
- Keep robots.txt enabled.
- Keep low concurrency by splitting CSVs and using delays.
- Do not scrape LinkedIn, personal social profiles, or private directories for sales people.
- Publish named personnel only when they are listed publicly on the official company website with a relevant sales/commercial/export title.
