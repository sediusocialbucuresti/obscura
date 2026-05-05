# B2B Public Website Deploy

## SaharaIndex Live Deployment

The 50k B2B directory is deployed on the production SaharaIndex domain:

```text
https://saharaindex.com/companies/
```

The generated static files are served by the SaharaIndex Caddy container from:

```text
/srv/sahara-b2b-directory
```

The production build uses SaharaIndex canonical URLs and the live RFQ API:

```sh
OBSCURA_B2B_SITE_BASE_URL=https://saharaindex.com \
OBSCURA_B2B_RFQ_API_URL=https://api.saharaindex.com/api/rfqs \
  cargo run -p obscura-b2b -- export --out data/b2b
```

The Caddy route handles:

```text
/companies/
/companies/*.html
/sitemap-index.xml
/sitemaps/*.xml
/styles.css
/robots.txt
```

Buyer requests are handled through the embedded RFQ form on each company profile. The form posts to:

```text
https://api.saharaindex.com/api/rfqs
```

Each RFQ payload includes the requested company name, LEI when available, and the profile URL in the message body so SaharaIndex can route and verify the supplier request.

Verification commands:

```sh
curl -I https://saharaindex.com/companies/
curl -I https://saharaindex.com/sitemap-index.xml
curl -s https://saharaindex.com/robots.txt
curl -sI -X OPTIONS https://api.saharaindex.com/api/rfqs \
  -H 'Origin: https://saharaindex.com' \
  -H 'Access-Control-Request-Method: POST' \
  -H 'Access-Control-Request-Headers: content-type'
```

Deployment status on 2026-05-06:

- 50,250 company HTML files served from `/srv/sahara-b2b-directory/companies`.
- 2 sitemap files served from `/srv/sahara-b2b-directory/sitemaps`.
- Public `robots.txt` advertises both the existing SaharaIndex sitemap and the B2B sitemap index.
- Profile pages include LEI verification and the embedded buyer RFQ form.
- Contact and product fields are still empty unless a later permitted-source enrichment record exists. GLEIF provides legal identity data, not official websites, emails, phones, or product catalogs.

The 50k B2B directory is deployed to GitHub Pages from the fork's `gh-pages` branch.

Public URL:

```text
https://sediusocialbucuresti.github.io/obscura/
```

## Build With Public Canonicals

Set the public base URL before export so canonical tags, robots.txt, and sitemaps point to the real website:

```sh
OBSCURA_B2B_SITE_BASE_URL=https://sediusocialbucuresti.github.io/obscura \
  cargo run -p obscura-b2b -- export --out data/b2b
```

## Deploy

```sh
tools/b2b_deploy_github_pages.sh
```

The deploy script publishes only `data/b2b/site` to `gh-pages`. It does not publish raw GLEIF pages, `directory/*.json`, Mautic files, or enrichment logs.

## Verify

```sh
curl -I https://sediusocialbucuresti.github.io/obscura/
curl -s https://sediusocialbucuresti.github.io/obscura/sitemap-index.xml
curl -s https://sediusocialbucuresti.github.io/obscura/robots.txt
```

GitHub Pages is currently configured as:

```text
source branch: gh-pages
source path: /
https enforced: true
```
