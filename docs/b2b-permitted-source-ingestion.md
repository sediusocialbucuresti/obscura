# B2B Permitted-Source Ingestion

This workstream uses official APIs, open-data downloads, or licensed data feeds only. It does not scrape private lead databases, personal employee contacts, login-gated directories, or B2B marketplaces that require permission for bulk reuse.

## Implemented Connectors

The connector script is:

```sh
python3 tools/b2b_official_source_ingest.py --help
```

Current implemented sources:

- France Annuaire des Entreprises API: official open company search API.
- Norway Bronnoysund Register Centre API: official open company register API.

Machine-readable source policy:

```text
data/b2b/sources/permitted-source-inventory.json
```

## Run

Dry run:

```sh
python3 tools/b2b_official_source_ingest.py \
  --out data/b2b \
  --sources france,norway \
  --limit 25 \
  --page-size 25 \
  --delay 0.2 \
  --dry-run
```

Append official-source profiles:

```sh
python3 tools/b2b_official_source_ingest.py \
  --out data/b2b \
  --sources france,norway \
  --limit 5000 \
  --page-size 100 \
  --delay 0.2
```

France can rate-limit aggressive runs. If HTTP 429 appears, rerun later with a higher delay such as `--delay 1.0`.

## Data Policy

The connector stores:

- Company name.
- Official registry number.
- Country and address when supplied.
- Activity code and activity description.
- Company website only when supplied by the official API.
- Company size bucket when supplied by the official API.
- Source URL and source-rights evidence.

The connector does not store:

- Personal directors or representatives from registry responses.
- Personal emails.
- Private lead-database contacts.
- Product catalogs inferred from activity codes.

Activity codes are stored as industries and specializations, not product catalogs. Product and catalog fields should come only from official company websites or licensed B2B catalog feeds.

## Latest Run

Run date: 2026-05-06.

```text
France Annuaire des Entreprises API: appended 2,525 profiles, 3 rate-limit errors.
Norway Bronnoysund Register Centre API: appended 4,383 profiles, 17 skipped existing.
Latest profile count after run: 56,908.
Profiles with official websites after run: 616.
```

The live export should be rebuilt after each append:

```sh
OBSCURA_B2B_SITE_BASE_URL=https://saharaindex.com \
OBSCURA_B2B_RFQ_API_URL=https://api.saharaindex.com/api/rfqs \
  cargo run -p obscura-b2b -- export --out data/b2b
```
