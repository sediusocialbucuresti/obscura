# B2B Orchestrator Implementation Report

Date: 2026-05-05

## 2026-05-10 Additional Company Expansion Batch

Continued company ingestion with a GPT-5.3-Codex-Spark worker used as a parallel source-selection reviewer. The worker recommended staying on already-implemented permitted lanes: official registries, GLEIF legal entities, and later controlled official-site enrichment.

Ingestion executed in this batch:

```text
France Annuaire des Entreprises API: 2,500 additional official profiles appended; 5,525 fetched; 3,025 existing skipped; 0 errors.
Norway Bronnoysund Register Centre API: 1,371 additional official profiles appended; 11,775 fetched; 10,404 existing skipped; 0 errors.
Estonia e-Business Register open data: 0 appended because the upstream response was not a valid zip file in this run.
GLEIF Global LEI Index API: 5,000 additional legal-entity profiles appended across EG, AE, SA, MA, TN, TR, DE, IT, ES, NL, BE, PL, and RO; 20,778 fetched; 15,684 existing skipped; 0 errors.
```

GLEIF country additions in this batch:

```text
AE 784
SA 712
TR 589
ES 505
NL 401
RO 408
DE 400
IT 400
BE 400
PL 400
EG 1
MA 0
TN 0
```

Resulting deployed website state:

```text
company_profiles.jsonl rows: 195,640
Exported public profiles: 133,970
Generated HTML files under companies/: 134,795
Egypt public profiles: 2,163
United Arab Emirates public profiles: 1,059
Arabic-character scan across generated site: 0 files
Public directory URL: https://saharaindex.com/companies/
```

Deployment note:

```text
The first rsync deployment was too slow for the full regenerated static tree. A hard-linked /srv/sahara-b2b-directory.next copy was created on the same filesystem, the previous web root was retained as /srv/sahara-b2b-directory.prev-20260510143357, and the Caddy container was force-recreated so its Docker bind mount pointed at the new web-root inode.
```

## 2026-05-10 English Public Directory And Country Expansion

Implemented the latest SaharaIndex public-directory cleanup and deployment pass:

- Forced generated public pages to English-only presentation for the directory UI, filter labels, industry labels, country pages, profile descriptions, cards, JSON-LD, and static search records.
- Added Arabic-to-English public term mappings for ExpoEgypt sectors and Egyptian governorates/city terms, then stripped any remaining Arabic script from generated public text.
- Normalized Arabic-Indic digits in public company phone fields to Western digits.
- Percent-encoded non-ASCII profile, website, and image URLs so raw Arabic does not leak into HTML attributes or static search JSON.
- Added product category, industry, country, contact-method, source, and company-type filters to the generated directory index.
- Kept only company-level public contact signals for buyer RFQ use: corporate website, role/generic email, switchboard phone, address, public products, and public catalog images where exposed by the source.
- Added display guards for low-quality company names and Estonia person-like registry records so the public site favors company profiles.
- Added an Estonia ingest guard that only accepts records with recognizable business/legal markers such as `OÜ`, `AS`, `MTÜ`, `FIE`, and related legal forms.

Country expansion executed during this pass:

```text
Norway Bronnoysund official API: approximately 500 additional official profiles appended before the combined Finland request stalled.
France Sirene / Annuaire des Entreprises API: 500 additional official profiles appended.
Estonia e-Business Register open data: 300 additional company-level profiles appended after filtering out 52,236 person-like records.
Finland PRH/YTJ API: deferred from this deployment because the request stalled at socket level during the combined run.
```

Final generated/deployed website state:

```text
Public directory URL: https://saharaindex.com/companies/
Egypt country page URL: https://saharaindex.com/companies/country-egypt.html
Exported public profiles: 125,509
Egypt public profiles: 2,163
Generated HTML files under companies/: 126,292
Generated static site size: 1.6 GB
Arabic-character scan across generated site: 0 files
```

Verification and deployment commands used:

```bash
python3 tools/b2b_official_source_ingest.py --out data/b2b --sources france --limit 500 --page-size 25 --delay 0.05
python3 tools/b2b_official_source_ingest.py --out data/b2b --sources estonia --limit 300 --page-size 5 --delay 0.05
cargo build --release -p obscura-b2b
OBSCURA_B2B_SKIP_COMPANY_JSON=1 OBSCURA_B2B_SITE_BASE_URL=https://saharaindex.com OBSCURA_B2B_RFQ_API_URL=https://api.saharaindex.com/api/rfqs ./target/release/obscura-b2b export --out data/b2b
rg -l "[\x{0600}-\x{06FF}]" data/b2b/site
rsync -a --delete data/b2b/site/ /srv/sahara-b2b-directory/
curl -I https://saharaindex.com/companies/
curl -I https://saharaindex.com/companies/country-egypt.html
```

## Repository Access

The prompt referenced `https://github.com/h4ckf4r0day/obscura`, which was not cloneable from this environment. The public repository that resolved successfully is:

```text
https://github.com/h4ckf0r0day/obscura
```

The repository was cloned into:

```text
/root/obscura
```

## What Was Added

Added a new workspace crate:

```text
crates/obscura-b2b/
```

This crate provides a B2B data extraction pipeline on top of Obscura's existing browser engine.

The workspace was updated in:

```text
Cargo.toml
Cargo.lock
```

The new crate builds around existing workspace dependencies so it does not introduce a new database or queue requirement for the first production-ready foundation.

## New Crate Structure

```text
crates/obscura-b2b/
  Cargo.toml
  src/
    lib.rs
    main.rs
    models.rs
    seed.rs
    orchestrator.rs
    extract.rs
    validator.rs
    export.rs
    storage.rs
```

## Main Capabilities Implemented

`obscura-b2b` now supports:

- Seed-file driven source configuration for MENA and Europe B2B targets.
- Directory source discovery for candidate company/profile URLs.
- Company-site source processing for direct company pages.
- Concurrent scraping with per-domain delay controls.
- Optional robots.txt enforcement through Obscura's existing browser context.
- Company profile extraction into normalized JSON records.
- Contact extraction for email, phone, website, and social links.
- Product and service signal extraction from links and visible text.
- Industry and company-type inference for manufacturers, wholesalers, distributors, suppliers, importers, and exporters.
- Address, company size, revenue, and personnel signal extraction when present.
- Validation scoring and issue flags.
- Append-only JSONL persistence for jobs, attempts, and company profiles.
- Raw HTML capture for audit/debugging.
- Website directory exports.
- Mautic-ready contact CSV export.
- "Claim your profile" email template generation.
- Loop mode for scheduled recurring updates.

## CLI Commands Added

Initialize seed/output files:

```bash
cargo run -p obscura-b2b -- init --seeds b2b-seeds.json --out data/b2b
```

Run one scrape pass:

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

Run continuously on an interval:

```bash
cargo run -p obscura-b2b -- run \
  --seeds b2b-seeds.json \
  --out data/b2b \
  --loop \
  --interval-seconds 86400 \
  --obey-robots \
  --export
```

Regenerate website and Mautic exports:

```bash
cargo run -p obscura-b2b -- export --out data/b2b
```

## Output Layout

The orchestrator writes:

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

## Website Integration Outputs

The website directory can consume:

```text
data/b2b/directory/index.json
data/b2b/directory/search.json
data/b2b/directory/segments.json
data/b2b/directory/companies/*.json
```

`index.json` is intended for listing pages.

`search.json` is intended for full-text or client-side search indexing.

`segments.json` groups company IDs by:

- region
- country
- industry
- company type

Each file under `directory/companies/` is an individual company profile.

## Mautic Integration Outputs

The Mautic import file is:

```text
data/b2b/mautic/contacts.csv
```

The generated columns are:

```text
email, firstname, lastname, company, phone, website, region, country, industry, company_type, tags, profile_url, claim_url
```

By default, the export includes role-based business emails and excludes personal-looking addresses. Personal-looking addresses can be included with:

```bash
--include-personal-contacts
```

That flag should only be used after consent/lawful-basis review.

The claim-profile email template is:

```text
data/b2b/mautic/templates/claim-your-profile.md
```

## Compliance Controls

The implementation includes these safeguards:

- Seed sources require an explicit `compliance_basis`.
- `--obey-robots` enables Obscura's robots.txt policy.
- Validation reports include compliance flags.
- Personal-looking emails are flagged.
- Mautic export defaults to role-based business contacts only.
- Raw page HTML is stored for audit/debugging.

## What Was Not Done

This change does not scrape every MENA and Europe company immediately. That would require a verified target source list, legal basis review, rate limits, and a long-running production environment.

This change does not send email campaigns. It prepares Mautic import files and a claim-profile template.

This change does not add Postgres, Redis, or distributed queues yet. The first implementation uses append-only JSONL and filesystem exports so it can run without infrastructure. A future scaling layer can replace or supplement this with Postgres/Redis while preserving the same models.

## Verification Status

Rust tooling and native build dependencies were installed in this environment.

Toolchain:

```text
rustc 1.95.0
cargo 1.95.0
Ubuntu 24.04
```

The following checks were executed successfully:

```bash
cargo fmt --all -- --check
cargo test -p obscura-b2b
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --release --workspace
./target/release/obscura-b2b init --seeds /tmp/obscura-b2b-seeds.json --out /tmp/obscura-b2b-data
./target/release/obscura-b2b export --out /tmp/obscura-b2b-empty-export
```

The release build generated the new `obscura-b2b` binary alongside the existing workspace binaries.

## 2026-05-08 Egypt Enrichment And Website UX Update

Implemented the next SaharaIndex company-directory impact step:

- Added image support to company profiles and static SEO pages.
- Added the original SaharaIndex menu links to generated directory pages.
- Added a localStorage-backed dark mode toggle compatible with the original `sahara_theme` behavior.
- Added SEO segment pages for exporters and companies with photos.
- Added `tools/b2b_made_in_egypt_gate_ingest.py` for permitted public Made in Egypt Gate factory data:
  - public factory profile URL
  - company description
  - sector/category
  - product/service labels
  - public profile photos
  - company website where exposed
  - role/generic public company email where exposed
  - public company phone where exposed
- Added `tools/b2b_egypt_goeic_approved_exporters.py` for the official GOEIC approved-exporter PDF:
  - company name
  - approved item
  - HS code
  - approved exporter code
  - approval date
- Added `tools/b2b_expoegypt_ingest.py` for the public ExpoEgypt exporter/product directory:
  - public exporter profile URL
  - company email, phone, website, and address where published
  - sector/category
  - logo/profile image
  - public product names, product URLs, and product images where the product listing exposes a company relation

ExpoEgypt run summary:

```text
Exporter pages scanned: 488
Product pages scanned: 420
Exporter cards fetched: 4,874
Profiles appended: 4,800
Existing duplicates skipped: 74
Product company relation keys found: 1,016
```

Corpus impact after this pass:

```text
Latest displayable profiles: 130,855
Egypt displayable profiles: 5,313
Egypt profiles with contacts: 5,106
Egypt profiles with photos: 2,504
Egypt profiles with products: 5,141
ExpoEgypt profiles: 4,800
Made in Egypt Gate profiles: 315
GOEIC official exporter profiles: 34
Worldwide profiles with photos: 2,504
Worldwide profiles with products: 5,229
Worldwide profiles with websites: 9,343
Worldwide profiles with role/generic email: 7,711
Worldwide profiles with phones: 15,745
```

Verification executed:

```bash
cargo fmt --all --check
cargo check -q
python3 -m py_compile tools/b2b_made_in_egypt_gate_ingest.py tools/b2b_egypt_goeic_approved_exporters.py
python3 tools/b2b_made_in_egypt_gate_ingest.py --out /tmp/sahara-egypt-test --limit 1 --dry-run
python3 tools/b2b_egypt_goeic_approved_exporters.py --out /tmp/sahara-egypt-test --pdf /tmp/goeic-approved-exporters.pdf --limit 3 --dry-run
python3 tools/b2b_expoegypt_ingest.py --out /tmp/sahara-expo-test --company-pages 1 --product-pages 2 --company-limit 3 --dry-run
python3 tools/b2b_made_in_egypt_gate_ingest.py --out data/b2b --delay 0.2
python3 tools/b2b_egypt_goeic_approved_exporters.py --out data/b2b --pdf /tmp/goeic-approved-exporters.pdf
python3 tools/b2b_expoegypt_ingest.py --out data/b2b --delay 0.12
```

## Files Added Or Modified

Added:

```text
.gitignore
crates/obscura-b2b/Cargo.toml
crates/obscura-b2b/src/lib.rs
crates/obscura-b2b/src/main.rs
crates/obscura-b2b/src/models.rs
crates/obscura-b2b/src/seed.rs
crates/obscura-b2b/src/orchestrator.rs
crates/obscura-b2b/src/extract.rs
crates/obscura-b2b/src/validator.rs
crates/obscura-b2b/src/export.rs
crates/obscura-b2b/src/storage.rs
docs/b2b-orchestrator.md
docs/b2b-implementation-report.md
examples/b2b-seeds.json
```

Modified:

```text
Cargo.toml
Cargo.lock
crates/obscura-browser/*
crates/obscura-cdp/*
crates/obscura-cli/*
crates/obscura-dom/*
crates/obscura-js/*
crates/obscura-net/*
```

The non-B2B crate modifications are formatting and lint/build hardening required to make the full workspace pass `cargo fmt --all -- --check` and strict workspace clippy.
