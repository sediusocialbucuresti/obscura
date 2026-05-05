# B2B Scale Plan

This plan documents the next scale step for Europe and MENA company profile ingestion and website publication.

## Current Scale Step

Run a 50,000-company baseline from GLEIF:

```sh
python3 tools/b2b_gleif_pilot.py --out data/b2b --limit 50000 --page-size 200 --sleep 0.05
cargo run -p obscura-b2b -- export --out data/b2b
```

GLEIF is the identity baseline: company name, LEI, source URL, jurisdiction, and address fields. GLEIF does not provide campaign-ready emails, full websites, products, services, or decision-maker contacts.

## Country Coverage

The current GLEIF pilot importer covers these regions.

Europe:

```text
Albania, Andorra, Austria, Belgium, Bosnia and Herzegovina, Bulgaria, Croatia,
Cyprus, Czechia, Denmark, Estonia, Finland, France, Germany, Greece, Hungary,
Iceland, Ireland, Italy, Latvia, Liechtenstein, Lithuania, Luxembourg, Malta,
Moldova, Monaco, Montenegro, Netherlands, North Macedonia, Norway, Poland,
Portugal, Romania, San Marino, Serbia, Slovakia, Slovenia, Spain, Sweden,
Switzerland, United Kingdom
```

MENA:

```text
Algeria, Bahrain, Egypt, Iran, Iraq, Israel, Jordan, Kuwait, Lebanon, Libya,
Morocco, Oman, Qatar, Saudi Arabia, Syria, Tunisia, Turkey, United Arab Emirates,
Yemen, Palestine
```

## Source Database Ladder

Use a jurisdiction-first adapter layer. Each adapter should record source name, source URL, country, source identifier, license/terms review status, and whether the source is identity-only or enrichment-capable.

### Global Baseline

- GLEIF Global LEI Index API: identity/address baseline for legal entities.
  - <https://www.gleif.org/en/lei-data/global-lei-index>
  - <https://www.gleif.org/en/lei-data/gleif-api>

### Europe Priority Adapters

- United Kingdom: Companies House API.
  - <https://developer.company-information.service.gov.uk/get-started>
  - Identity: company number, status, registered office, officers/filings where allowed.
  - Caveat: API key and terms required.
- France: INSEE/Sirene via API Entreprise and INSEE API catalogue.
  - <https://entreprise.api.gouv.fr/catalogue/insee/etablissements_diffusibles>
  - <https://www.insee.fr/fr/information/8184146>
  - Identity and establishment data with public/diffusion controls.
- Netherlands: KVK APIs.
  - <https://developers.kvk.nl/documentation>
  - Identity and branch profile data.
  - Caveat: licensing/subscription and volume limits.
- Germany: Handelsregister / register portal.
  - <https://www.handelsregister.de/>
  - Identity and filings where official access pattern permits.
  - Caveat: portal/bulk access terms need source-specific implementation.
- Spain: Registro Mercantil and BOE/BORME.
  - <https://www.mjusticia.gob.es/>
  - <https://www.boe.es/diario_borme/>
  - Use as official update/document source, not a uniform company API.
- EU cross-checks:
  - VIES VAT validation: <https://ec.europa.eu/taxation_customs/vies/>
  - EU business register discovery: <https://e-justice.europa.eu/>

### MENA Priority Adapters

- Saudi Arabia: Wathq APIs.
  - <https://developer.wathq.sa/index.php/en/apis>
  - Identity and commercial registration data.
  - Caveat: paid package and API key terms.
- United Arab Emirates: UAE API Marketplace and ministry APIs.
  - <https://u.ae/en/about-the-uae/digital-uae/digital-transformation/platforms-and-apps/uae-api-marketplace>
  - <https://www.moj.gov.ae/en/open-data/apis.aspx>
  - Caveat: service-specific coverage rather than one complete national registry.
- Qatar: Qatar Open Data and MOCI support sources.
  - <https://www.data.gov.qa/explore/dataset/moci-active-certificates-by-municipality-and-business-activity/>
  - Caveat: useful for activity/statistics enrichment, not a full company-detail source.
- Tunisia: Registre National des Entreprises.
  - <https://www.registre-entreprises.tn/>
  - Caveat: confirm endpoint stability and API/bulk terms before production ingestion.

## Website Indexing Requirements

At 50k and above, the static site must avoid one giant listing page. The exporter now generates:

- Paginated company listing pages with 200 profiles per page.
- Individual company profile pages.
- `robots.txt`.
- `sitemap-index.xml` with sitemap shards.
- Canonical tags.
- Per-profile meta descriptions.
- JSON-LD `Organization` payloads.
- `noindex,nofollow` only for blocked/hold/rejected or very low-score profiles.

Set the public canonical host before export:

```sh
OBSCURA_B2B_SITE_BASE_URL=https://your-directory.example cargo run -p obscura-b2b -- export --out data/b2b
```

For local preview, omit the variable and the exporter uses `http://127.0.0.1:8080`.

## Campaign Export Gates

Website indexing and Mautic export are separate gates.

Indexable on the website:

- `ready` and `review` records with validation score at least 40.
- Baseline GLEIF profiles, because they are public identity/address records and visibly marked for enrichment.

Withhold from campaign export:

- Any profile that is not `ready`.
- Any profile with `mautic_export_not_campaign_ready`.
- Any personal-looking email unless `--include-personal-contacts` is explicitly passed after a separate lawful-basis review.

Mautic segments include region, country, industry, company type, source, status, and source-basis.

## Next Enrichment Batches

After the 50k identity baseline:

1. Run a 200-300 company official-website resolution canary across Europe and MENA.
2. Use `--obey-robots`, low concurrency, and per-domain delay.
3. Extract only public business contact points, website URLs, products, services, and catalog links.
4. Promote to 1,000-company enrichment batches after the canary shows stable website resolution and low block/error rates.
5. Promote to 2,500-5,000 company batches only after campaign readiness gates are validated.
