# B2B GLEIF Enrichment

This repo now includes an append-only enrichment command for existing B2B
profiles:

```sh
python3 tools/b2b_gleif_enrich_existing.py --out data/b2b --cached-only
```

The enrichment source is exclusively the GLEIF Global LEI Index API/cache for
this lane. It does not access private databases, bypass restricted systems, or
collect non-public personal contact data.

## What It Adds

For each matched profile, the command appends a newer JSONL revision containing:

- LEI verification tags.
- Legal name, entity status, entity category, legal form, jurisdiction, and registration authority.
- Registered-as number and validation authority.
- Initial registration, last update, renewal, and creation dates.
- Corroboration and conformity flags.
- Associated entity and parent relationship references where GLEIF provides them.

The exporter keeps the latest revision per profile id, so generated website
pages, directory JSON, search JSON, and Mautic segments automatically use the
enriched profile.

## Runbook

Enrich from cached GLEIF pages:

```sh
python3 tools/b2b_gleif_enrich_existing.py --out data/b2b --cached-only
```

Use the live GLEIF API for profiles not present in the local cache:

```sh
python3 tools/b2b_gleif_enrich_existing.py --out data/b2b --api-search --sleep 0.1
```

Re-export the public site with production canonical URLs:

```sh
OBSCURA_B2B_SITE_BASE_URL=https://sediusocialbucuresti.github.io/obscura \
  cargo run -p obscura-b2b -- export --out data/b2b
```

Deploy to GitHub Pages:

```sh
MESSAGE='Deploy GLEIF-enriched B2B directory' tools/b2b_deploy_github_pages.sh
```

## Compliance Boundary

Legal-entity enrichment is restricted to GLEIF for this lane. Contact enrichment
must use official company websites, public role-based business contacts, or
sources where access and reuse are explicitly authorized. Restricted database
bypasses and private contact harvesting are out of scope.
