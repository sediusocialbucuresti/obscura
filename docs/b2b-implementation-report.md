# B2B Orchestrator Implementation Report

Date: 2026-05-05

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
