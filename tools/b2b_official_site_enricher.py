#!/usr/bin/env python3
"""Enrich B2B profiles from official company websites.

Input is a CSV of verified official websites. This tool does not resolve
websites from search engines and does not crawl social networks or marketplaces.
It crawls same-domain company pages, extracts role-based sales/commercial emails,
public sales personnel names/titles, phones, and product/service links, then
appends updated profiles to company_profiles.jsonl.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import re
import time
import urllib.parse
import urllib.request
import urllib.robotparser
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable


USER_AGENT = "ObscuraB2BOfficialSiteEnricher/0.1"
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b", re.I)
PHONE_RE = re.compile(r"(?:\+|00)?[0-9][0-9 .()\-]{6,}[0-9]")

ROLE_EMAIL_LOCAL_PARTS = {
    "sales",
    "export",
    "exports",
    "commercial",
    "wholesale",
    "wholesales",
    "distribution",
    "distributor",
    "dealers",
    "dealer",
    "business",
    "bd",
    "info",
    "contact",
    "office",
    "enquiry",
    "enquiries",
    "inquiries",
    "customer.service",
    "customerservice",
}

SALES_EMAIL_HINTS = {
    "sales",
    "export",
    "commercial",
    "wholesale",
    "distribution",
    "dealer",
    "business",
}

SALES_TITLE_HINTS = [
    "sales manager",
    "sales director",
    "head of sales",
    "export manager",
    "export sales",
    "international sales",
    "commercial director",
    "commercial manager",
    "business development",
    "key account manager",
    "account manager",
    "area sales manager",
]

PAGE_HINTS = [
    "contact",
    "contacts",
    "sales",
    "export",
    "commercial",
    "dealer",
    "distributor",
    "products",
    "product",
    "services",
    "service",
    "catalog",
    "catalogue",
    "about",
    "team",
]


class ParsedPage(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.links: list[tuple[str, str]] = []
        self.mailtos: list[str] = []
        self.tels: list[str] = []
        self.text_parts: list[str] = []
        self._current_href: str | None = None
        self._current_text: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"script", "style", "noscript", "svg"}:
            self._skip_depth += 1
            return

        attrs_dict = {k.lower(): v or "" for k, v in attrs}
        if tag == "a":
            href = attrs_dict.get("href", "").strip()
            if href:
                href = urllib.parse.urljoin(self.base_url, href)
                if href.startswith("mailto:"):
                    self.mailtos.append(href)
                elif href.startswith("tel:"):
                    self.tels.append(href)
                else:
                    self._current_href = href
                    self._current_text = []

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style", "noscript", "svg"} and self._skip_depth:
            self._skip_depth -= 1
            return
        if tag == "a" and self._current_href:
            self.links.append((self._current_href, clean(" ".join(self._current_text))))
            self._current_href = None
            self._current_text = []
        if tag in {"p", "div", "li", "br", "tr", "section", "article", "h1", "h2", "h3"}:
            self.text_parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self._skip_depth:
            return
        value = html.unescape(data)
        if self._current_href:
            self._current_text.append(value)
        self.text_parts.append(value)

    @property
    def text(self) -> str:
        return normalize_space(" ".join(self.text_parts))


def clean(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def normalize_space(value: str) -> str:
    value = re.sub(r"[ \t\f\v]+", " ", value)
    value = re.sub(r"\s*\n\s*", "\n", value)
    return value.strip()


def slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def normalize_url(value: str) -> str:
    value = value.strip()
    if not value:
        return value
    if not re.match(r"^https?://", value, re.I):
        value = f"https://{value}"
    parsed = urllib.parse.urlparse(value)
    return parsed._replace(fragment="").geturl()


def host(value: str) -> str:
    return (urllib.parse.urlparse(value).hostname or "").lower().removeprefix("www.")


def same_site(url: str, root_host: str) -> bool:
    current = host(url)
    return current == root_host or current.endswith(f".{root_host}")


def is_html_url(url: str) -> bool:
    path = urllib.parse.urlparse(url).path.lower()
    return not re.search(r"\.(pdf|jpg|jpeg|png|gif|webp|zip|rar|7z|doc|docx|xls|xlsx)$", path)


def page_priority(url: str, text: str) -> int:
    haystack = f"{url} {text}".lower()
    score = 0
    for idx, hint in enumerate(PAGE_HINTS):
        if hint in haystack:
            score += max(1, len(PAGE_HINTS) - idx)
    return score


def request_text(url: str, timeout: int) -> tuple[str, str]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        content_type = resp.headers.get("content-type", "")
        if "html" not in content_type and "text/plain" not in content_type:
            return resp.geturl(), ""
        charset = resp.headers.get_content_charset() or "utf-8"
        return resp.geturl(), resp.read(2_000_000).decode(charset, errors="replace")


def robots_allowed(url: str, user_agent: str, cache: dict[str, urllib.robotparser.RobotFileParser]) -> bool:
    parsed = urllib.parse.urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    parser = cache.get(base)
    if parser is None:
        parser = urllib.robotparser.RobotFileParser()
        parser.set_url(f"{base}/robots.txt")
        try:
            parser.read()
        except Exception:
            return False
        cache[base] = parser
    return parser.can_fetch(user_agent, url)


def extract_emails(text: str, source_url: str) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    for email in EMAIL_RE.findall(text):
        email = email.lower().strip(".,;:()[]{}<>")
        if email in seen or not valid_email(email):
            continue
        seen.add(email)
        local = email.split("@", 1)[0]
        role = local in ROLE_EMAIL_LOCAL_PARTS or any(hint in local for hint in SALES_EMAIL_HINTS)
        sales = any(hint in local for hint in SALES_EMAIL_HINTS)
        out.append(
            {
                "value": email,
                "kind": "sales_role_email" if sales and role else ("role_email" if role else "personal_email"),
                "source_url": source_url,
                "confidence": 0.9 if role else 0.65,
                "personal": not role,
            }
        )
    return out


def valid_email(value: str) -> bool:
    if not value.isascii() or "@" not in value:
        return False
    local, domain = value.rsplit("@", 1)
    return (
        bool(local)
        and len(local) <= 64
        and "." in domain
        and not domain.startswith(".")
        and not domain.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp"))
    )


def extract_phones(text: str, source_url: str) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    for match in PHONE_RE.findall(text):
        phone = clean(match)
        digits = sum(ch.isdigit() for ch in phone)
        if digits < 7 or digits > 18 or phone in seen:
            continue
        seen.add(phone)
        out.append(
            {
                "value": phone,
                "kind": "phone",
                "source_url": source_url,
                "confidence": 0.65,
                "personal": False,
            }
        )
    return out[:10]


def extract_sales_people(text: str, source_url: str) -> list[dict]:
    people: list[dict] = []
    seen: set[str] = set()
    for raw_line in text.splitlines():
        line = clean(raw_line)
        lower = line.lower()
        title = next((hint for hint in SALES_TITLE_HINTS if hint in lower), None)
        if not title or len(line) < 8 or len(line) > 220:
            continue
        name = guess_name(line, title)
        key = f"{name or ''}|{title}|{line}".lower()
        if key in seen:
            continue
        seen.add(key)
        people.append(
            {
                "name": name,
                "title": title,
                "source_text": line,
                "source_url": source_url,
            }
        )
        if len(people) >= 10:
            break
    return people


def guess_name(line: str, title: str) -> str | None:
    lower = line.lower()
    idx = lower.find(title)
    before = clean(line[:idx]).strip("-:|,;")
    after = clean(line[idx + len(title) :]).strip("-:|,;")
    for candidate in (before, after):
        words = candidate.split()
        if 2 <= len(words) <= 4 and all(word[:1].isupper() for word in words if word[:1].isalpha()):
            return candidate
    return None


def extract_catalog_links(links: Iterable[tuple[str, str]], category: str) -> list[dict]:
    hints = ["product", "catalog", "catalogue", "brand", "range"] if category == "product" else [
        "service",
        "solution",
        "support",
        "logistics",
        "distribution",
    ]
    out: list[dict] = []
    seen: set[str] = set()
    for href, text in links:
        haystack = f"{href} {text}".lower()
        if not any(hint in haystack for hint in hints):
            continue
        name = clean(text) or clean(urllib.parse.urlparse(href).path.rsplit("/", 1)[-1]) or category
        key = f"{name}|{href}".lower()
        if len(name) < 3 or key in seen:
            continue
        seen.add(key)
        out.append({"name": name[:160], "description": None, "url": href, "category": category})
        if len(out) >= 20:
            break
    return out


def read_profiles(path: Path) -> list[dict]:
    profiles: list[dict] = []
    if not path.exists():
        return profiles
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if line:
                profiles.append(json.loads(line))
    return profiles


def read_targets(csv_path: Path | None, profiles: list[dict]) -> list[dict]:
    if csv_path:
        with csv_path.open(newline="", encoding="utf-8") as file:
            return [dict(row) for row in csv.DictReader(file)]

    targets = []
    for profile in profiles:
        websites = []
        if profile.get("canonical_domain"):
            websites.append(profile["canonical_domain"])
        websites.extend(profile.get("contacts", {}).get("websites") or [])
        for website in websites:
            targets.append(
                {
                    "profile_id": profile["id"],
                    "company_name": profile["company_name"],
                    "website": website,
                }
            )
    return targets


def merge_unique_dicts(existing: list[dict], additions: list[dict], key: str) -> list[dict]:
    out = list(existing or [])
    seen = {str(item.get(key, "")).lower() for item in out}
    for item in additions:
        item_key = str(item.get(key, "")).lower()
        if item_key and item_key not in seen:
            seen.add(item_key)
            out.append(item)
    return out


def merge_unique_values(existing: list[str], additions: list[str]) -> list[str]:
    out = list(existing or [])
    seen = {value.lower() for value in out}
    for value in additions:
        if value and value.lower() not in seen:
            seen.add(value.lower())
            out.append(value)
    return out


def update_validation(profile: dict, mark_ready: bool) -> None:
    validation = profile.setdefault("validation", {})
    issues = set(validation.get("issues") or [])
    flags = set(validation.get("compliance_flags") or [])
    coverage = set(validation.get("field_coverage") or [])

    if profile.get("canonical_domain"):
        coverage.add("domain")
        issues.discard("website_enrichment_required")
    if profile.get("contacts", {}).get("emails") or profile.get("contacts", {}).get("phones"):
        coverage.add("contact")
        issues.discard("contact_enrichment_required")
    if profile.get("products") or profile.get("services"):
        coverage.add("catalog")
        issues.discard("catalog_enrichment_required")

    flags.add("source_basis:official_company_website")
    flags.add("official_site_enrichment")
    flags.add("robots_txt_policy_enabled")

    role_email = any(
        not email.get("personal") for email in profile.get("contacts", {}).get("emails") or []
    )
    has_catalog = bool(profile.get("products") or profile.get("services"))
    if mark_ready and role_email and profile.get("canonical_domain"):
        flags.discard("mautic_export_not_campaign_ready")
        validation["status"] = "ready" if has_catalog else "review"
    else:
        flags.add("mautic_export_not_campaign_ready")
        validation["status"] = "review"

    score = int(validation.get("score") or 0)
    if profile.get("canonical_domain"):
        score = max(score, 65)
    if role_email:
        score = max(score, 75 if has_catalog else 70)
    validation["score"] = min(score, 95)
    validation["issues"] = sorted(issues)
    validation["compliance_flags"] = sorted(flags)
    validation["field_coverage"] = sorted(coverage)


def enrich_target(
    target: dict,
    profile: dict,
    pages_per_site: int,
    timeout: int,
    delay: float,
    obey_robots: bool,
    robots_cache: dict[str, urllib.robotparser.RobotFileParser],
) -> tuple[dict, dict]:
    root_url = normalize_url(target.get("website") or target.get("url") or "")
    root_host = host(root_url)
    if not root_url or not root_host:
        raise ValueError("missing website")

    pages: list[str] = [root_url]
    visited: set[str] = set()
    all_text: list[str] = []
    all_links: list[tuple[str, str]] = []
    source_pages: list[str] = []

    while pages and len(visited) < pages_per_site:
        url = pages.pop(0)
        if url in visited or not same_site(url, root_host) or not is_html_url(url):
            continue
        if obey_robots and not robots_allowed(url, USER_AGENT, robots_cache):
            continue
        visited.add(url)
        final_url, body = request_text(url, timeout)
        if not body:
            continue
        parser = ParsedPage(final_url)
        parser.feed(body)
        page_text = parser.text
        source_pages.append(final_url)
        all_text.append(page_text)
        all_links.extend(parser.links)
        all_links.extend((mailto, "") for mailto in parser.mailtos)
        all_links.extend((tel, "") for tel in parser.tels)

        candidates = []
        for href, text in parser.links:
            href = urllib.parse.urldefrag(href)[0]
            if (
                href
                and href not in visited
                and same_site(href, root_host)
                and is_html_url(href)
            ):
                score = page_priority(href, text)
                if score:
                    candidates.append((score, href))
        for _, href in sorted(candidates, reverse=True):
            if href not in pages:
                pages.append(href)
        time.sleep(delay)

    combined_text = "\n".join(all_text)
    emails = []
    for href, _ in all_links:
        if href.startswith("mailto:"):
            value = urllib.parse.unquote(href[7:].split("?", 1)[0])
            emails.extend(extract_emails(value, href))
    emails.extend(extract_emails(combined_text, root_url))
    phones = extract_phones(combined_text, root_url)
    people = extract_sales_people(combined_text, root_url)
    products = extract_catalog_links(all_links, "product")
    services = extract_catalog_links(all_links, "service")

    profile = json.loads(json.dumps(profile))
    profile["canonical_domain"] = root_host
    profile.setdefault("contacts", {})
    profile["contacts"]["websites"] = merge_unique_values(
        profile["contacts"].get("websites") or [], [root_url, *source_pages]
    )
    profile["contacts"]["emails"] = merge_unique_dicts(
        profile["contacts"].get("emails") or [], emails, "value"
    )
    profile["contacts"]["phones"] = merge_unique_dicts(
        profile["contacts"].get("phones") or [], phones, "value"
    )
    profile["personnel"] = merge_unique_dicts(profile.get("personnel") or [], people, "source_text")
    profile["products"] = merge_unique_dicts(profile.get("products") or [], products, "url")
    profile["services"] = merge_unique_dicts(profile.get("services") or [], services, "url")
    profile["evidence"] = merge_unique_dicts(
        profile.get("evidence") or [],
        [
            {
                "field": "official_website",
                "value": root_url,
                "source_url": root_url,
            }
        ],
        "field",
    )
    profile["tags"] = merge_unique_values(profile.get("tags") or [], ["official-site-enriched"])
    profile["scraped_at_epoch"] = int(time.time())

    summary = {
        "profile_id": profile["id"],
        "company_name": profile["company_name"],
        "website": root_url,
        "pages_crawled": len(source_pages),
        "role_emails": sum(1 for item in emails if not item.get("personal")),
        "personal_emails": sum(1 for item in emails if item.get("personal")),
        "phones": len(phones),
        "sales_people": len(people),
        "products": len(products),
        "services": len(services),
        "source_pages": source_pages,
    }
    return profile, summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="data/b2b")
    parser.add_argument("--websites-csv")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--pages-per-site", type=int, default=6)
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--delay", type=float, default=1.5)
    parser.add_argument("--ignore-robots", action="store_true")
    parser.add_argument("--mark-ready-with-role-email", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    out = Path(args.out)
    profiles_path = out / "company_profiles.jsonl"
    enrichment_dir = out / "enrichment"
    enrichment_dir.mkdir(parents=True, exist_ok=True)
    summary_path = enrichment_dir / "official-site-enrichment.jsonl"

    profiles = read_profiles(profiles_path)
    by_id = {profile["id"]: profile for profile in profiles}
    by_name = {slug(profile["company_name"]): profile for profile in profiles}
    targets = read_targets(Path(args.websites_csv) if args.websites_csv else None, profiles)
    robots_cache: dict[str, urllib.robotparser.RobotFileParser] = {}

    processed = enriched = failed = skipped = 0
    with summary_path.open("a", encoding="utf-8") as summary_file:
        for target in targets:
            if processed >= args.limit:
                break
            profile = None
            if target.get("profile_id"):
                profile = by_id.get(target["profile_id"])
            if profile is None and target.get("id"):
                profile = by_id.get(target["id"])
            if profile is None and target.get("company_name"):
                profile = by_name.get(slug(target["company_name"]))
            if profile is None:
                skipped += 1
                continue

            processed += 1
            try:
                updated, summary = enrich_target(
                    target,
                    profile,
                    args.pages_per_site,
                    args.timeout,
                    args.delay,
                    not args.ignore_robots,
                    robots_cache,
                )
                update_validation(updated, args.mark_ready_with_role_email)
                summary["status"] = "ok"
                summary_file.write(json.dumps(summary, ensure_ascii=False) + "\n")
                if not args.dry_run:
                    with profiles_path.open("a", encoding="utf-8") as profiles_file:
                        profiles_file.write(json.dumps(updated, ensure_ascii=False) + "\n")
                enriched += 1
                print(
                    f"ok {processed}: {summary['company_name']} "
                    f"role_emails={summary['role_emails']} sales_people={summary['sales_people']}"
                )
            except Exception as exc:
                failed += 1
                summary = {
                    "status": "failed",
                    "target": target,
                    "error": str(exc),
                }
                summary_file.write(json.dumps(summary, ensure_ascii=False) + "\n")
                print(f"failed {processed}: {target.get('company_name') or target.get('website')}: {exc}")

    print(
        json.dumps(
            {
                "targets_seen": len(targets),
                "processed": processed,
                "enriched": enriched,
                "failed": failed,
                "skipped_unmatched": skipped,
                "summary_path": str(summary_path),
                "appended_to_profiles": not args.dry_run,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
