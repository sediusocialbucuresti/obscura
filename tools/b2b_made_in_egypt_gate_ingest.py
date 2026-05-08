#!/usr/bin/env python3
"""Append public Made in Egypt Gate factory profiles to SaharaIndex.

The source exposes public WordPress REST endpoints for factories, sectors, and
media. This ingester keeps company-level contacts only, avoids private/person
lead harvesting, and stores source/right metadata on every appended profile.
"""

from __future__ import annotations

import argparse
import html
import json
import re
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Iterable

BASE = "https://madeinegyptgate.com"
API = f"{BASE}/wp-json/wp/v2"
SOURCE_NAME = "Made in Egypt Gate"
SOURCE_URL = f"{BASE}/factories/"
USER_AGENT = "SaharaIndexBot/1.0 (+https://saharaindex.com/companies/)"

ROLE_EMAIL_PREFIXES = {
    "admin",
    "business",
    "commercial",
    "contact",
    "customer",
    "customerservice",
    "export",
    "exports",
    "hello",
    "info",
    "marketing",
    "office",
    "orders",
    "sales",
    "service",
    "support",
}

BLOCKED_HOST_PARTS = {
    "madeinegyptgate.com",
    "wp.com",
    "wordpress.com",
    "squarespace-cdn.com",
    "w.org",
}

SOCIAL_HOST_PARTS = {
    "facebook.com",
    "fb.com",
    "instagram.com",
    "linkedin.com",
    "youtube.com",
    "youtu.be",
    "x.com",
    "twitter.com",
}

STOP_CATALOG_LABELS = {
    "about",
    "be honest and transparent",
    "contact",
    "facebook",
    "grow together",
    "have dedication to quality",
    "industries we support",
    "innovate and diversify",
    "purpose",
    "read more",
    "trust and respect",
    "values",
    "why choose us",
    "خدماتنا ومنتجاتنا",
    "لماذا تراست أوفيس؟",
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="data/b2b", help="B2B output directory")
    parser.add_argument("--limit", type=int, default=0, help="Maximum factories to append; 0 means all")
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--delay", type=float, default=0.2)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--update-existing", action="store_true")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    profiles_path = out_dir / "company_profiles.jsonl"
    existing_ids = read_existing_ids(profiles_path)

    sectors = fetch_terms("sectors")
    appended = 0
    fetched = 0
    skipped = 0
    errors = 0
    started = int(time.time())

    handle = None if args.dry_run else profiles_path.open("a", encoding="utf-8")
    try:
        for record in iter_factories(page_size=args.page_size, delay=args.delay):
            fetched += 1
            profile = profile_from_factory(record, sectors, started)
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
            if args.limit and appended >= args.limit:
                break
    except (urllib.error.URLError, TimeoutError, ValueError) as exc:
        errors += 1
        print(f"made-in-egypt-gate ingest error: {exc}", file=sys.stderr)
    finally:
        if handle:
            handle.close()

    report = {
        "source": SOURCE_NAME,
        "source_url": SOURCE_URL,
        "fetched": fetched,
        "appended": appended,
        "skipped": skipped,
        "errors": errors,
        "dry_run": args.dry_run,
        "finished_at_epoch": int(time.time()),
    }
    reports_dir = out_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    if not args.dry_run:
        (reports_dir / "made_in_egypt_gate_ingest.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if errors == 0 else 1


def iter_factories(page_size: int, delay: float) -> Iterable[dict[str, Any]]:
    page = 1
    page_size = max(1, min(page_size, 100))
    while True:
        url = (
            f"{API}/factories?"
            + urllib.parse.urlencode({"per_page": page_size, "page": page, "_embed": "1"})
        )
        try:
            data = fetch_json(url)
        except urllib.error.HTTPError as exc:
            if exc.code == 400:
                return
            raise
        if not isinstance(data, list) or not data:
            return
        for item in data:
            if isinstance(item, dict):
                yield item
        page += 1
        time.sleep(max(0.0, delay))


def profile_from_factory(record: dict[str, Any], sectors: dict[int, str], now: int) -> dict[str, Any] | None:
    title = clean_text(nested(record, "title", "rendered"))
    if not title:
        return None
    wp_id = str(record.get("id") or "")
    profile_url = str(record.get("link") or "")
    content_html = nested(record, "content", "rendered")
    excerpt_html = nested(record, "excerpt", "rendered")
    content_text = clean_text(content_html)
    excerpt_text = clean_text(excerpt_html)
    sector_names = [sectors[item] for item in record.get("sectors", []) if item in sectors]
    category = infer_category(" ".join(sector_names))
    if category == "Egyptian manufacturing":
        category = infer_category(" ".join([content_text, title]))
    description = first_meaningful_text([excerpt_text, content_text], title)
    images = extract_images(record, content_html, title, profile_url)
    contacts = extract_contacts(content_html, content_text, profile_url)
    products = extract_products(title, content_html, content_text, sector_names, category, profile_url)
    services = extract_services(content_html, content_text, category, profile_url)

    evidence = [
        {"field": "source_basis", "value": "public_b2b_directory", "source_url": SOURCE_URL},
        {"field": "source_rights", "value": "robots_allow_wordpress_rest_api", "source_url": f"{BASE}/robots.txt"},
        {"field": "made_in_egypt_gate_id", "value": wp_id, "source_url": profile_url},
    ]
    for sector in sector_names:
        evidence.append({"field": "sector", "value": sector, "source_url": profile_url})
    for image in images[:3]:
        evidence.append({"field": "image_url", "value": image["url"], "source_url": profile_url})

    coverage = ["company_name", "description", "country", "products"]
    if contacts["websites"]:
        coverage.append("website")
    if contacts["emails"]:
        coverage.append("company_email")
    if contacts["phones"]:
        coverage.append("phone")
    if images:
        coverage.append("images")

    flags = [
        "source_basis:public_b2b_directory",
        "source_rights:robots_allow_wordpress_rest_api",
        "company_level_contacts_only",
    ]
    if images:
        flags.append("public_profile_images")
    if contacts["emails"] or contacts["phones"] or contacts["websites"]:
        flags.append("public_company_contact")

    score = 55
    if description:
        score += 8
    if products:
        score += 8
    if images:
        score += 7
    if contacts["emails"] or contacts["phones"] or contacts["websites"]:
        score += 10

    website = contacts["websites"][0] if contacts["websites"] else None
    profile_id = f"eg-made-in-egypt-gate-{wp_id}-{slugify(title)}".strip("-")
    if not profile_id or profile_id == "eg-made-in-egypt-gate":
        profile_id = f"eg-made-in-egypt-gate-{wp_id}"

    return {
        "id": profile_id,
        "source_name": SOURCE_NAME,
        "source_url": SOURCE_URL,
        "profile_url": profile_url,
        "canonical_domain": host_from_url(website) if website else None,
        "company_name": title,
        "description": description,
        "region": "MENA",
        "country": "Egypt",
        "company_type": "manufacturer",
        "industries": sector_names or [category or "Egyptian manufacturing"],
        "specializations": dedupe([category] + sector_names),
        "products": products,
        "services": services,
        "images": images,
        "contacts": contacts,
        "addresses": [],
        "company_size": None,
        "revenue": None,
        "personnel": [],
        "evidence": evidence,
        "validation": {
            "status": "enriched",
            "score": min(score, 88),
            "issues": [],
            "compliance_flags": flags,
            "field_coverage": coverage,
        },
        "tags": dedupe(
            [
                "egypt",
                "mena",
                "manufacturer",
                "made-in-egypt-gate",
                "with-products" if products else "",
                "with-photos" if images else "",
                "company-contact" if contacts["emails"] or contacts["phones"] or contacts["websites"] else "",
            ]
        ),
        "scraped_at_epoch": now,
        "refresh_due_epoch": now + 30 * 86400,
    }


def extract_contacts(content_html: str, text: str, profile_url: str) -> dict[str, Any]:
    emails = []
    for email in re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", content_html + " " + text):
        value = email.strip(".,;:()[]{}<>").lower()
        local = value.split("@", 1)[0].replace(".", "").replace("-", "").replace("_", "")
        if local not in ROLE_EMAIL_PREFIXES and not any(local.startswith(prefix) for prefix in ROLE_EMAIL_PREFIXES):
            continue
        emails.append(
            {
                "value": value,
                "kind": "company_email",
                "source_url": profile_url,
                "confidence": 0.86,
                "personal": False,
            }
        )

    phones = []
    phone_pattern = re.compile(r"(?:(?:\+|00)20|0)\s?1[0125][\s.\-]?\d{3}[\s.\-]?\d{4}")
    for phone in phone_pattern.findall(text):
        normalized = re.sub(r"\s+", " ", phone).strip(" .,-")
        phones.append(
            {
                "value": normalized,
                "kind": "company_phone",
                "source_url": profile_url,
                "confidence": 0.78,
                "personal": False,
            }
        )

    websites = []
    social_links = []
    for url in extract_links(content_html):
        parsed_host = host_from_url(url) or ""
        if any(part in parsed_host for part in SOCIAL_HOST_PARTS):
            social_links.append(url)
        elif parsed_host and not any(part in parsed_host for part in BLOCKED_HOST_PARTS):
            websites.append(url)

    return {
        "emails": dedupe_contact_points(emails),
        "phones": dedupe_contact_points(phones),
        "websites": dedupe(websites),
        "social_links": dedupe(social_links),
    }


def extract_images(record: dict[str, Any], content_html: str, title: str, profile_url: str) -> list[dict[str, Any]]:
    images = []
    media = record.get("_embedded", {}).get("wp:featuredmedia", [])
    if media and isinstance(media[0], dict):
        url = image_url_from_media(media[0])
        if url:
            images.append({"url": url, "alt": title, "kind": "featured", "source_url": profile_url})
    for match in re.finditer(r"<img\b[^>]*>", content_html, flags=re.IGNORECASE):
        tag = match.group(0)
        url = first_attr(tag, ["src", "data-src", "data-image"])
        if not url:
            continue
        url = html.unescape(url)
        if url.startswith("//"):
            url = "https:" + url
        if not url.startswith("http"):
            continue
        alt = clean_text(first_attr(tag, ["alt"]) or title)
        images.append({"url": url, "alt": alt or title, "kind": "profile", "source_url": profile_url})
    return dedupe_media(images)[:8]


def extract_products(
    title: str,
    content_html: str,
    content_text: str,
    sectors: list[str],
    category: str,
    profile_url: str,
) -> list[dict[str, Any]]:
    labels = extract_catalog_labels(content_html, title)
    products = []
    for label in labels[:8]:
        products.append(
            {
                "name": label,
                "description": f"Public {SOURCE_NAME} factory profile lists {label} for {title}.",
                "url": profile_url,
                "category": category,
            }
        )
    if not products:
        name = sector_product_label(sectors, category)
        products.append(
            {
                "name": name,
                "description": first_sentence(content_text) or f"Public {SOURCE_NAME} factory profile for {title}.",
                "url": profile_url,
                "category": category,
            }
        )
    return products


def extract_services(content_html: str, content_text: str, category: str, profile_url: str) -> list[dict[str, Any]]:
    service_labels = []
    for label in extract_catalog_labels(content_html, ""):
        lower = label.casefold()
        if any(word in lower for word in ["service", "services", "design", "supply", "maintenance", "installation"]):
            service_labels.append(label)
    return [
        {
            "name": label,
            "description": first_sentence(content_text) or None,
            "url": profile_url,
            "category": category,
        }
        for label in dedupe(service_labels)[:6]
    ]


def extract_catalog_labels(content_html: str, company_name: str) -> list[str]:
    labels = []
    tag_pattern = re.compile(r"<(?:h2|h3|h4|strong|li)\b[^>]*>(.*?)</(?:h2|h3|h4|strong|li)>", re.I | re.S)
    for raw in tag_pattern.findall(content_html):
        label = clean_text(raw)
        if not label:
            continue
        for piece in re.split(r"[:|،؛•]+", label):
            piece = clean_text(piece)
            lower = piece.casefold()
            if not piece or lower in STOP_CATALOG_LABELS:
                continue
            if company_name and lower == company_name.casefold():
                continue
            if len(piece) < 3 or len(piece) > 90:
                continue
            if piece.count(" ") > 9:
                continue
            labels.append(piece)
    return dedupe(labels)


def sector_product_label(sectors: list[str], category: str) -> str:
    if sectors:
        cleaned = clean_text(sectors[0]).replace("Manufacture of ", "").strip()
        if cleaned:
            return cleaned
    return category or "Egyptian manufactured products"


def infer_category(value: str) -> str:
    lower = clean_text(value).casefold()
    checks = [
        ("food", "Food & Beverage"),
        ("beverage", "Food & Beverage"),
        ("agric", "Agriculture"),
        ("textile", "Textiles & Apparel"),
        ("apparel", "Textiles & Apparel"),
        ("garment", "Textiles & Apparel"),
        ("chemical", "Chemicals"),
        ("detergent", "Chemicals"),
        ("pharma", "Healthcare Supply"),
        ("medical", "Healthcare Supply"),
        ("plastic", "Packaging"),
        ("packag", "Packaging"),
        ("paper", "Packaging"),
        ("metal", "Construction Materials"),
        ("aluminum", "Construction Materials"),
        ("steel", "Construction Materials"),
        ("building", "Construction Materials"),
        ("cement", "Construction Materials"),
        ("ceramic", "Construction Materials"),
        ("machinery", "Industrial Equipment"),
        ("equipment", "Industrial Equipment"),
        ("automotive", "Automotive"),
        ("cable", "Electronics"),
        ("electrical", "Electronics"),
        ("electronics", "Electronics"),
        ("furniture", "Furniture"),
        ("logistics", "Logistics"),
    ]
    for needle, label in checks:
        if needle in lower:
            return label
    return "Egyptian manufacturing"


def fetch_terms(rest_base: str) -> dict[int, str]:
    terms = {}
    page = 1
    while True:
        url = f"{API}/{rest_base}?" + urllib.parse.urlencode({"per_page": 100, "page": page})
        try:
            data = fetch_json(url)
        except urllib.error.HTTPError as exc:
            if exc.code == 400:
                break
            raise
        if not isinstance(data, list) or not data:
            break
        for item in data:
            if isinstance(item, dict) and item.get("id") is not None:
                terms[int(item["id"])] = clean_text(str(item.get("name") or ""))
        page += 1
    return terms


def fetch_json(url: str) -> Any:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=45) as response:
        return json.loads(response.read().decode("utf-8"))


def image_url_from_media(media: dict[str, Any]) -> str:
    sizes = media.get("media_details", {}).get("sizes", {})
    for size in ["medium_large", "large", "full", "medium", "thumbnail"]:
        url = nested(sizes, size, "source_url")
        if url:
            return str(url)
    return str(media.get("source_url") or "")


def extract_links(content_html: str) -> list[str]:
    links = []
    for raw in re.findall(r"<a\b[^>]*\bhref=[\"']([^\"']+)[\"']", content_html, flags=re.I):
        url = html.unescape(raw).strip()
        if url.startswith("//"):
            url = "https:" + url
        if url.startswith("mailto:") or url.startswith("tel:"):
            continue
        if url.startswith("http"):
            links.append(url)
    return links


def first_attr(tag: str, names: list[str]) -> str:
    for name in names:
        match = re.search(rf"\b{name}=[\"']([^\"']+)[\"']", tag, flags=re.I)
        if match:
            return match.group(1)
    return ""


def first_meaningful_text(values: Iterable[str], title: str) -> str:
    for value in values:
        value = clean_text(value)
        if value and value.casefold() != title.casefold():
            return truncate(value, 700)
    return f"{title} is listed as an Egyptian manufacturer on {SOURCE_NAME}."


def first_sentence(value: str) -> str:
    value = clean_text(value)
    match = re.search(r"(.{20,220}?[.!?])(?:\s|$)", value)
    if match:
        return match.group(1)
    return truncate(value, 180)


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = html.unescape(str(value))
    text = re.sub(r"<script\b.*?</script>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<style\b.*?</style>", " ", text, flags=re.I | re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def truncate(value: str, limit: int) -> str:
    value = clean_text(value)
    if len(value) <= limit:
        return value
    return value[: limit - 3].rstrip() + "..."


def nested(data: Any, *keys: str) -> Any:
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


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


def dedupe_contact_points(values: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = set()
    out = []
    for value in values:
        key = str(value.get("value") or "").casefold()
        if key and key not in seen:
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


def slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", ascii_value).strip("-").lower()
    return slug


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
