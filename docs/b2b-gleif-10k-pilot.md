# B2B GLEIF 10k Pilot

This note documents the first 10,000-company pilot run for the B2B directory pipeline.

## Source Selection

The pilot uses the GLEIF Global LEI Index API as a baseline company identity source.

Source references:

- <https://www.gleif.org/en/lei-data/global-lei-index>
- <https://www.gleif.org/en/lei-data/gleif-api>
- <https://api.gleif.org/api/v1/lei-records>

GLEIF is suitable for the first ingestion pass because it provides structured legal entity names, LEI identifiers, registration status, and address fields with country filters. It is not a complete manufacturer, wholesaler, or distributor catalog. Records imported from GLEIF are marked as `review` and include enrichment-required compliance flags.

## Import Command

Run the pilot importer from the repository root:

```sh
python3 tools/b2b_gleif_pilot.py --out data/b2b --limit 10000 --page-size 200 --sleep 0.05
```

The importer interleaves Europe and MENA country filters so early pilot batches contain both regions. It writes raw GLEIF response pages for traceability and a normalized Obscura B2B JSONL profile file.

Generated files:

```text
data/b2b/company_profiles.jsonl
data/b2b/sources/gleif/*.json
data/b2b/sources/gleif-manifest.json
```

The generated `data/b2b/` directory is intentionally ignored by Git. Recreate it with the command above instead of committing bulk runtime data.

## Export Command

After import, build website, directory, search, and Mautic outputs:

```sh
cargo run -p obscura-b2b -- export --out data/b2b
```

Generated export files:

```text
data/b2b/directory/index.json
data/b2b/directory/search.json
data/b2b/directory/segments.json
data/b2b/directory/companies/*.json
data/b2b/site/index.html
data/b2b/site/companies/*.html
data/b2b/site/styles.css
data/b2b/mautic/contacts.csv
data/b2b/mautic/templates/claim-your-profile.md
```

The exporter resets generated company JSON and static HTML company directories before each run, so stale profile files from failed or partial exports are removed.

## Verified Pilot Result

The first pilot run produced:

```text
10000 data/b2b/company_profiles.jsonl
10000 data/b2b/directory/companies/*.json
10000 data/b2b/site/companies/*.html
```

The Mautic CSV has only a header row for this pilot because GLEIF does not provide email addresses. That is intentional: these records are not campaign-ready until a lawful enrichment step finds public business contact points from official websites or explicitly permitted B2B sources.

## Data Coverage

Each generated profile includes:

- Company legal name.
- LEI source URL.
- Region and country.
- Legal/headquarters address when provided by GLEIF.
- Review validation status and enrichment-required flags.
- Website, contact, product, service, size, revenue, and personnel fields left empty until enrichment.

## Next Enrichment Step

Use the 10k GLEIF records as canonical identity seeds, then enrich in controlled batches:

1. Resolve official websites from registry mappings, company homepages, or vetted directories.
2. Crawl only allowed pages with rate limits and robots.txt checks.
3. Extract role-based business emails, phone numbers, product/service categories, and public catalog links.
4. Re-export the directory and Mautic files.
5. Keep personal contacts excluded unless a separate lawful-basis review approves them.
