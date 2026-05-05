# B2B Public Website Deploy

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
