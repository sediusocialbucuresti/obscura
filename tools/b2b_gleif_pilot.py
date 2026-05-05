#!/usr/bin/env python3
"""Build an open-data B2B pilot dataset from GLEIF LEI records.

This creates company_profiles.jsonl that can be exported by:

    obscura-b2b export --out data/b2b

GLEIF gives canonical legal entity identity and address data. It does not
provide product catalogs, websites, or campaign-ready contacts; those should be
added by a later enrichment crawl from official websites or vetted directories.
"""

from __future__ import annotations

import argparse
import json
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path


EUROPE = {
    "AL": "Albania",
    "AD": "Andorra",
    "AT": "Austria",
    "BE": "Belgium",
    "BA": "Bosnia and Herzegovina",
    "BG": "Bulgaria",
    "HR": "Croatia",
    "CY": "Cyprus",
    "CZ": "Czechia",
    "DK": "Denmark",
    "EE": "Estonia",
    "FI": "Finland",
    "FR": "France",
    "DE": "Germany",
    "GR": "Greece",
    "HU": "Hungary",
    "IS": "Iceland",
    "IE": "Ireland",
    "IT": "Italy",
    "LV": "Latvia",
    "LI": "Liechtenstein",
    "LT": "Lithuania",
    "LU": "Luxembourg",
    "MT": "Malta",
    "MD": "Moldova",
    "MC": "Monaco",
    "ME": "Montenegro",
    "NL": "Netherlands",
    "MK": "North Macedonia",
    "NO": "Norway",
    "PL": "Poland",
    "PT": "Portugal",
    "RO": "Romania",
    "SM": "San Marino",
    "RS": "Serbia",
    "SK": "Slovakia",
    "SI": "Slovenia",
    "ES": "Spain",
    "SE": "Sweden",
    "CH": "Switzerland",
    "GB": "United Kingdom",
}

MENA = {
    "DZ": "Algeria",
    "BH": "Bahrain",
    "EG": "Egypt",
    "IR": "Iran",
    "IQ": "Iraq",
    "IL": "Israel",
    "JO": "Jordan",
    "KW": "Kuwait",
    "LB": "Lebanon",
    "LY": "Libya",
    "MA": "Morocco",
    "OM": "Oman",
    "QA": "Qatar",
    "SA": "Saudi Arabia",
    "SY": "Syria",
    "TN": "Tunisia",
    "TR": "Turkey",
    "AE": "United Arab Emirates",
    "YE": "Yemen",
    "PS": "Palestine",
}

API = "https://api.gleif.org/api/v1/lei-records"


def slugify(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "-", value).strip("-").lower()
    return slug or "company"


def request_json(country: str, page: int, size: int) -> dict:
    params = {
        "filter[entity.legalAddress.country]": country,
        "filter[entity.status]": "ACTIVE",
        "filter[registration.status]": "ISSUED",
        "page[size]": str(size),
        "page[number]": str(page),
    }
    url = f"{API}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.api+json",
            "User-Agent": "ObscuraB2B/0.1 GLEIF pilot",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def address_text(address: dict) -> str:
    lines = [line for line in address.get("addressLines") or [] if line]
    for key in ("postalCode", "city", "region", "country"):
        value = address.get(key)
        if value:
            lines.append(value)
    return ", ".join(lines)


def region_for(country: str) -> str:
    return "MENA" if country in MENA else "Europe"


def country_name(country: str) -> str:
    return MENA.get(country) or EUROPE.get(country) or country


def profile_from_record(record: dict, now: int) -> dict:
    attrs = record["attributes"]
    entity = attrs["entity"]
    registration = attrs["registration"]
    lei = attrs["lei"]
    name = entity["legalName"]["name"]
    country = entity["legalAddress"]["country"]
    legal_address = address_text(entity.get("legalAddress") or {})
    hq_address = address_text(entity.get("headquartersAddress") or {})
    addresses = []
    for value in (legal_address, hq_address):
        if value and value not in addresses:
            addresses.append(value)

    source_url = record["links"]["self"]
    description = (
        f"{name} is an active legal entity in {country_name(country)} with LEI {lei}. "
        "This baseline profile was imported from the GLEIF Global LEI Index and needs "
        "website/contact/catalog enrichment before publication or outreach."
    )
    validation_score = 55 if addresses else 45

    return {
        "id": slugify(f"{name}-{lei}"),
        "source_name": "GLEIF Global LEI Index",
        "source_url": source_url,
        "profile_url": source_url,
        "canonical_domain": None,
        "company_name": name,
        "description": description,
        "region": region_for(country),
        "country": country_name(country),
        "company_type": "legal entity",
        "industries": ["Unclassified B2B entity"],
        "specializations": [],
        "products": [],
        "services": [],
        "contacts": {
            "emails": [],
            "phones": [],
            "websites": [],
            "social_links": [],
        },
        "addresses": addresses,
        "company_size": None,
        "revenue": None,
        "personnel": [],
        "evidence": [
            {
                "field": "lei",
                "value": lei,
                "source_url": source_url,
            },
            {
                "field": "registration_status",
                "value": registration.get("status") or "",
                "source_url": source_url,
            },
        ],
        "validation": {
            "status": "review",
            "score": validation_score,
            "issues": [
                "website_enrichment_required",
                "contact_enrichment_required",
                "catalog_enrichment_required",
            ],
            "compliance_flags": [
                "source_basis:gleif_open_lei_data",
                "public_legal_entity_record",
                "mautic_export_not_campaign_ready",
            ],
            "field_coverage": ["company_name", "location", "registry_identifier"],
        },
        "tags": ["gleif", "lei", region_for(country).lower(), country.lower()],
        "scraped_at_epoch": now,
        "refresh_due_epoch": now + 30 * 86400,
    }


def iter_countries() -> list[str]:
    # Interleave regions so early pilot cuts contain both Europe and MENA.
    europe = list(EUROPE)
    mena = list(MENA)
    countries: list[str] = []
    for i in range(max(len(europe), len(mena))):
        if i < len(europe):
            countries.append(europe[i])
        if i < len(mena):
            countries.append(mena[i])
    return countries


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="data/b2b")
    parser.add_argument("--limit", type=int, default=10_000)
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--sleep", type=float, default=0.05)
    args = parser.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    raw_dir = out / "sources" / "gleif"
    raw_dir.mkdir(parents=True, exist_ok=True)
    profiles_path = out / "company_profiles.jsonl"
    manifest_path = out / "sources" / "gleif-manifest.json"

    countries = iter_countries()
    pages = {country: 1 for country in countries}
    exhausted: set[str] = set()
    seen: set[str] = set()
    total = 0
    now = int(time.time())

    with profiles_path.open("w", encoding="utf-8") as profiles:
        while total < args.limit and len(exhausted) < len(countries):
            for country in countries:
                if total >= args.limit:
                    break
                if country in exhausted:
                    continue
                page = pages[country]
                try:
                    payload = request_json(country, page, args.page_size)
                except Exception as exc:
                    print(f"warn: {country} page {page} failed: {exc}")
                    exhausted.add(country)
                    continue

                records = payload.get("data") or []
                if not records:
                    exhausted.add(country)
                    continue

                raw_path = raw_dir / f"{country.lower()}-{page}.json"
                raw_path.write_text(json.dumps(payload), encoding="utf-8")
                for record in records:
                    lei = record["attributes"]["lei"]
                    if lei in seen:
                        continue
                    seen.add(lei)
                    profiles.write(json.dumps(profile_from_record(record, now), ensure_ascii=False))
                    profiles.write("\n")
                    total += 1
                    if total >= args.limit:
                        break

                pages[country] += 1
                print(f"{total}/{args.limit} profiles after {country} page {page}")
                time.sleep(args.sleep)

    manifest = {
        "source": "GLEIF Global LEI Index API",
        "api": API,
        "limit": args.limit,
        "profiles": total,
        "countries": countries,
        "generated_at_epoch": now,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"wrote {total} profiles to {profiles_path}")


if __name__ == "__main__":
    main()
