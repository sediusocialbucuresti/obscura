#!/usr/bin/env python3
"""Append public ExpoEgypt exporter and product profiles to SaharaIndex.

ExpoEgypt exposes public exporter and product pages with company-level contact
fields, logos, and product images. The connector is rate-limited and stores
only business/profile contact points, not private lead/person databases.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

BASE = "https://www.expoegypt.gov.eg"
EXPORTERS_URL = f"{BASE}/exporters"
PRODUCTS_URL = f"{BASE}/products"
SOURCE_NAME = "ExpoEgypt Exporters Directory"
USER_AGENT = "SaharaIndexBot/1.0 (+https://saharaindex.com/companies/)"
TLS_CONTEXT = ssl._create_unverified_context()

NO_IMAGE_MARKERS = {"/images/no_thumb.jpg", "/images/no-image", "/no_thumb"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="data/b2b")
    parser.add_argument("--company-limit", type=int, default=0, help="Max companies to append; 0 means all")
    parser.add_argument("--product-pages", type=int, default=0, help="Max product pages to scan; 0 means all")
    parser.add_argument("--company-pages", type=int, default=0, help="Max exporter pages to scan; 0 means all")
    parser.add_argument("--delay", type=float, default=0.15)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--update-existing", action="store_true")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    profiles_path = out_dir / "company_profiles.jsonl"
    existing_ids = read_existing_ids(profiles_path)

    product_pages_total = max_page(PRODUCTS_URL)
    exporter_pages_total = max_page(EXPORTERS_URL)
    if args.product_pages:
        product_pages_total = min(product_pages_total, args.product_pages)
    if args.company_pages:
        exporter_pages_total = min(exporter_pages_total, args.company_pages)

    products_by_company = collect_products(product_pages_total, args.delay)
    fetched = 0
    appended = 0
    skipped = 0
    now = int(time.time())
    handle = None if args.dry_run else profiles_path.open("a", encoding="utf-8")

    try:
        for page in range(1, exporter_pages_total + 1):
            source_page = f"{EXPORTERS_URL}?page={page}"
            try:
                content = fetch_text(source_page)
            except urllib.error.URLError:
                continue
            for card in split_blocks(content, "co_node"):
                fetched += 1
                profile = profile_from_card(card, source_page, products_by_company, now)
                if not profile:
                    skipped += 1
                    continue
                if profile["id"] in existing_ids and not args.update_existing:
                    skipped += 1
                    continue
                if args.dry_run:
                    print(json.dumps(profile, ensure_ascii=False))
                else:
                    assert handle is not None
                    handle.write(json.dumps(profile, ensure_ascii=False) + "\n")
                existing_ids.add(profile["id"])
                appended += 1
                if args.company_limit and appended >= args.company_limit:
                    break
            if args.company_limit and appended >= args.company_limit:
                break
            time.sleep(max(0.0, args.delay))
    finally:
        if handle:
            handle.close()

    report = {
        "source": SOURCE_NAME,
        "source_url": EXPORTERS_URL,
        "exporter_pages_seen": exporter_pages_total,
        "product_pages_seen": product_pages_total,
        "fetched": fetched,
        "appended": appended,
        "skipped": skipped,
        "product_company_keys": len(products_by_company),
        "dry_run": args.dry_run,
        "finished_at_epoch": int(time.time()),
    }
    reports_dir = out_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    if not args.dry_run:
        (reports_dir / "expoegypt_ingest.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


def collect_products(page_count: int, delay: float) -> dict[str, list[dict[str, str]]]:
    products: dict[str, list[dict[str, str]]] = defaultdict(list)
    for page in range(1, page_count + 1):
        content = fetch_text(f"{PRODUCTS_URL}?page={page}")
        for block in split_blocks(content, "col-md-4 product"):
            product_url = first_href(block, "/products/i/")
            title = clean_text(first_match(block, r'class="kw-details-title"[^>]*>(.*?)</h3>'))
            image = first_image(block)
            company_url = first_href(block, "/company/contact/")
            company_key = company_key_from_url(company_url)
            if not title or not company_key:
                continue
            products[company_key].append(
                {
                    "name": title,
                    "url": absolute_url(product_url),
                    "image": image,
                    "company_url": absolute_url(company_url),
                }
            )
        time.sleep(max(0.0, delay))
    return products


def profile_from_card(
    card: str,
    source_page: str,
    products_by_company: dict[str, list[dict[str, str]]],
    now: int,
) -> dict[str, Any] | None:
    profile_url = absolute_url(first_href(card, "/co/"))
    name = clean_text(first_match(card, r'class="co_title"[^>]*>(.*?)</div>'))
    if not usable_company_name(name) or not profile_url:
        return None
    company_key = company_key_from_url(profile_url)
    sector = clean_sector(first_match(card, r'class="ind_sector"[^>]*>.*?<span class="light">\s*(.*?)\s*</span>'))
    category = infer_category(sector + " " + name)
    address = clean_text(first_match(card, r'class="co_address"[^>]*>(.*?)</div>'))
    phone = clean_text(first_match(card, r'class="co_phone"[^>]*>(.*?)</div>'))
    logo = first_image(card)
    mailtos = [clean_email(match) for match in re.findall(r'href=["\']mailto:([^"\']+)["\']', card, flags=re.I)]
    websites = []
    for href in re.findall(r'href=["\']([^"\']+)["\']', card, flags=re.I):
        if href.startswith("mailto:") or "/co/" in href or "/exporters" in href:
            continue
        if href.strip() and "javascript:" not in href:
            websites.append(normalize_website(href))
    products_raw = products_by_company.get(company_key, [])
    products = []
    images = []
    if logo and not is_placeholder_image(logo):
        images.append({"url": logo, "alt": name, "kind": "logo", "source_url": profile_url})
    for item in products_raw[:12]:
        products.append(
            {
                "name": item["name"],
                "description": f"Product listed for {name} on ExpoEgypt.",
                "url": item["url"],
                "category": infer_category(sector + " " + item["name"]),
            }
        )
        if item.get("image") and not is_placeholder_image(item["image"]):
            images.append(
                {
                    "url": item["image"],
                    "alt": item["name"],
                    "kind": "product",
                    "source_url": item["url"],
                }
            )
    if not products and sector:
        products.append(
            {
                "name": sector,
                "description": f"ExpoEgypt lists {name} in sector: {sector}.",
                "url": profile_url,
                "category": category,
            }
        )

    contacts = {
        "emails": [
            {
                "value": email,
                "kind": "company_email",
                "source_url": profile_url,
                "confidence": 0.82,
                "personal": False,
            }
            for email in dedupe(mailtos)
            if email
        ],
        "phones": [],
        "websites": dedupe([website for website in websites if website]),
        "social_links": [],
    }
    if phone:
        contacts["phones"].append(
            {
                "value": phone,
                "kind": "company_phone",
                "source_url": profile_url,
                "confidence": 0.8,
                "personal": False,
            }
        )

    description = clean_text(
        f"{name} is listed on ExpoEgypt as an Egyptian exporter. "
        f"Sector: {sector or 'not stated'}. {address}".strip()
    )
    profile_id = f"eg-expoegypt-{stable_key(profile_url)}"
    coverage = ["company_name", "country", "description"]
    if contacts["emails"]:
        coverage.append("company_email")
    if contacts["phones"]:
        coverage.append("phone")
    if contacts["websites"]:
        coverage.append("website")
    if address:
        coverage.append("address")
    if products:
        coverage.append("products")
    if images:
        coverage.append("images")

    flags = [
        "source_basis:public_b2b_directory",
        "source_rights:robots_allow_html_directory",
        "company_level_contacts_only",
    ]
    if contacts["emails"] or contacts["phones"] or contacts["websites"]:
        flags.append("public_company_contact")
    if images:
        flags.append("public_profile_images")

    score = 58
    score += 8 if contacts["emails"] or contacts["phones"] or contacts["websites"] else 0
    score += 8 if products else 0
    score += 6 if images else 0
    score += 4 if address else 0

    return {
        "id": profile_id,
        "source_name": SOURCE_NAME,
        "source_url": EXPORTERS_URL,
        "profile_url": profile_url,
        "canonical_domain": host_from_url(contacts["websites"][0]) if contacts["websites"] else None,
        "company_name": name,
        "description": description,
        "region": "MENA",
        "country": "Egypt",
        "company_type": "exporter",
        "industries": [sector] if sector else [category],
        "specializations": dedupe([category, sector]),
        "products": products,
        "services": [],
        "images": dedupe_media(images)[:10],
        "contacts": contacts,
        "addresses": [address] if address else [],
        "company_size": None,
        "revenue": None,
        "personnel": [],
        "evidence": [
            {"field": "source_basis", "value": "public_b2b_directory", "source_url": EXPORTERS_URL},
            {"field": "source_rights", "value": "robots_allow_html_directory", "source_url": f"{BASE}/robots.txt"},
            {"field": "source_page", "value": source_page, "source_url": source_page},
            {"field": "expoegypt_company_key", "value": company_key, "source_url": profile_url},
        ],
        "validation": {
            "status": "enriched",
            "score": min(score, 86),
            "issues": ["ExpoEgypt certificate chain required TLS verification bypass in this environment."],
            "compliance_flags": flags,
            "field_coverage": coverage,
        },
        "tags": dedupe(
            [
                "egypt",
                "mena",
                "exporter",
                "expoegypt",
                "with-products" if products else "",
                "with-photos" if images else "",
                "company-contact" if contacts["emails"] or contacts["phones"] or contacts["websites"] else "",
            ]
        ),
        "scraped_at_epoch": now,
        "refresh_due_epoch": now + 30 * 86400,
    }


def max_page(url: str) -> int:
    content = fetch_text(f"{url}?page=1")
    pages = [int(value) for value in re.findall(r"page=(\d+)", content)]
    return max(pages) if pages else 1


def fetch_text(url: str) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "text/html"})
    with urllib.request.urlopen(request, timeout=45, context=TLS_CONTEXT) as response:
        return response.read().decode("utf-8", errors="replace")


def split_blocks(content: str, class_marker: str) -> list[str]:
    pattern = re.compile(rf'<div[^>]+class=["\'][^"\']*{re.escape(class_marker)}[^"\']*["\'][^>]*>', re.I)
    matches = list(pattern.finditer(content))
    blocks = []
    for idx, match in enumerate(matches):
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(content)
        blocks.append(content[match.start() : end])
    return blocks


def first_href(content: str, contains: str) -> str:
    for href in re.findall(r'href=["\']([^"\']+)["\']', content, flags=re.I):
        if contains in href:
            return html.unescape(href.strip())
    return ""


def first_image(content: str) -> str:
    match = re.search(r'<img\b[^>]*\bsrc=["\']([^"\']+)["\']', content, flags=re.I)
    if not match:
        return ""
    return absolute_url(html.unescape(match.group(1).strip()))


def first_match(content: str, pattern: str) -> str:
    match = re.search(pattern, content, flags=re.I | re.S)
    return match.group(1) if match else ""


def absolute_url(url: str) -> str:
    url = html.unescape((url or "").strip())
    if not url:
        return ""
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("/"):
        return BASE + url
    return urllib.parse.urljoin(BASE + "/", url)


def normalize_website(value: str) -> str:
    value = html.unescape(value).strip()
    if not value:
        return ""
    value = value.replace("\t", "").replace(" ", "")
    if value.startswith("www."):
        value = "http://" + value
    if not value.startswith(("http://", "https://")):
        return ""
    return value


def clean_email(value: str) -> str:
    return html.unescape(value).strip().strip(".,;:").lower()


def clean_sector(value: str) -> str:
    return clean_text(value).strip(":- ")


def clean_text(value: Any) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"<script\b.*?</script>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<style\b.*?</style>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<br\s*/?>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def usable_company_name(value: str) -> bool:
    return sum(1 for ch in clean_text(value) if ch.isalnum()) >= 2


def company_key_from_url(url: str) -> str:
    if not url:
        return ""
    path = urllib.parse.urlparse(absolute_url(url)).path.rstrip("/")
    part = path.rsplit("/", 1)[-1]
    return urllib.parse.unquote(part).casefold().strip()


def stable_key(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:18]


def is_placeholder_image(url: str) -> bool:
    lower = url.lower()
    return any(marker in lower for marker in NO_IMAGE_MARKERS)


def infer_category(value: str) -> str:
    lower = clean_text(value).casefold()
    checks = [
        ("الأثاث", "Furniture"),
        ("furn", "Furniture"),
        ("غذ", "Food & Beverage"),
        ("food", "Food & Beverage"),
        ("حاصلات", "Agriculture"),
        ("زراع", "Agriculture"),
        ("agri", "Agriculture"),
        ("كيما", "Chemicals"),
        ("chemical", "Chemicals"),
        ("طبية", "Healthcare Supply"),
        ("medical", "Healthcare Supply"),
        ("بناء", "Construction Materials"),
        ("مواد البناء", "Construction Materials"),
        ("construct", "Construction Materials"),
        ("هندسية", "Industrial Equipment"),
        ("eng", "Industrial Equipment"),
        ("منسوج", "Textiles & Apparel"),
        ("ملابس", "Textiles & Apparel"),
        ("spinning", "Textiles & Apparel"),
        ("clothes", "Textiles & Apparel"),
        ("تغليف", "Packaging"),
        ("printing", "Packaging"),
        ("إلكترون", "Electronics"),
        ("electric", "Electronics"),
        ("سيارات", "Automotive"),
        ("automotive", "Automotive"),
    ]
    for needle, label in checks:
        if needle in lower:
            return label
    return "Egyptian export products"


def host_from_url(url: str) -> str | None:
    try:
        host = urllib.parse.urlparse(url).netloc.lower()
    except ValueError:
        return None
    if host.startswith("www."):
        host = host[4:]
    return host or None


def dedupe(values: Iterable[str]) -> list[str]:
    seen = set()
    out = []
    for value in values:
        value = clean_text(value)
        key = value.casefold()
        if value and key not in seen:
            seen.add(key)
            out.append(value)
    return out


def dedupe_media(values: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = set()
    out = []
    for value in values:
        url = str(value.get("url") or "").strip()
        key = url.split("?", 1)[0]
        if url and key not in seen:
            seen.add(key)
            out.append(value)
    return out


def read_existing_ids(path: Path) -> set[str]:
    ids = set()
    if not path.exists():
        return ids
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                profile = json.loads(line)
            except json.JSONDecodeError:
                continue
            profile_id = profile.get("id")
            if isinstance(profile_id, str):
                ids.add(profile_id)
    return ids


if __name__ == "__main__":
    raise SystemExit(main())
