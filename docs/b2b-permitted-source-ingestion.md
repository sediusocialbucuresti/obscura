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
- Finland PRH YTJ Open Data API: official PRH company search API (public company name/businessId lookup and official website fields).
- Estonia e-Business Register Open Data: official downloadable open-data JSON (`ettevotja_rekvisiidid__yldandmed`) containing registry-level contact fields.

Deferred sources (not ingested here):

- Bahrain Sijilat: endpoint is discoverable but enforcement is extremely aggressive (`https://api.sijilat.io/search` currently returns 429 with `Maximum 5 requests per hour` and does not expose a usable open endpoint without authenticated limits).
- Greece G.E.MI Open Data: portal requires API key / formal registration (`register`) before API access and does not provide a directly usable open endpoint for this connector path.
- Czech ARES REST: unauthenticated official REST calls work, but the tested manufacturer records did not expose usable contact fields in the returned payload and source reuse terms still need a final legal check before scaling.
- Belgium KBO, Poland REGON/BIR11, UK Companies House, Switzerland Zefix, and Saudi Wathq: useful registry sources, but the tested access paths require API keys, Basic/Bearer credentials, or commercial plan scopes before ingestion.

Machine-readable source policy:

```text
data/b2b/sources/permitted-source-inventory.json
```

## Run

Dry run:

```sh
python3 tools/b2b_official_source_ingest.py \
  --out data/b2b \
  --sources france,norway,finland,estonia \
  --limit 25 \
  --page-size 25 \
  --delay 0.2 \
  --dry-run
```

Append official-source profiles:

```sh
python3 tools/b2b_official_source_ingest.py \
  --out data/b2b \
  --sources france,norway,finland,estonia \
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
- Company phone only when supplied by the official registry API and not tied to a private person.
- Company size bucket when supplied by the official API.
- Company website, role/generic email (registry-level), and phone (registry-level) for Estonia records where available.
- Source URL and source-rights evidence.

The connector does not store:

- Personal directors or representatives from registry responses.
- Personal emails.
- Private lead-database contacts.
- Product catalogs inferred from activity codes.

For Estonia `yldandmed`, only registry-level contact types from `sidevahendid` are ingested:

- `WWW` as website
- `EMAIL` only when it can be linked to the company domain (and is not a public free-mail provider)
- `TEL` and `FAX` as phone values

Activity codes are stored as industries and specializations, not product catalogs. Product and catalog fields should come only from official company websites or licensed B2B catalog feeds.

## Official Website Enrichment

`tools/b2b_official_site_enricher.py` enriches only websites already present in verified profiles. It does not resolve new websites from search engines, scrape marketplaces, or crawl social networks. It obeys `robots.txt`, stays on the same domain, and appends role/generic emails, phones, product links, and service links found on official company websites.

Current safeguards:

- Latest-profile target deduplication by profile id and website host.
- Priority ordering for manufacturers, wholesalers, distributors, and exporters.
- Bounded pages per site and network timeouts.
- Personal emails are not exported to Mautic by default and are not shown as public buyer contacts on generated company pages.

## Latest Run

Run date: 2026-05-06.

```text
France Annuaire des Entreprises API: appended 2,525 profiles, 3 rate-limit errors.
Norway Bronnoysund Register Centre API: appended 5,136 fresh revisions with website/phone handling, then 1,444 additional new profiles.
Finland PRH YTJ Open Data API: appended 2,119 profiles before stopping slow duplicate-heavy prefix scans.
Estonia e-Business Register Open Data: appended 6,000 contact-bearing profiles from the official archive.
Official company website enrichment: appended 360+ profile revisions across bounded runs, including 88 profiles with product links and 66 with service links.
Latest profile count after run: 67,206.
Profiles with websites after run: 4,005.
Profiles with role/generic emails after run: 1,651.
Profiles with phones after run: 7,728.
```

The live export should be rebuilt after each append:

```sh
OBSCURA_B2B_SITE_BASE_URL=https://saharaindex.com \
OBSCURA_B2B_RFQ_API_URL=https://api.saharaindex.com/api/rfqs \
  cargo run -p obscura-b2b -- export --out data/b2b
```
