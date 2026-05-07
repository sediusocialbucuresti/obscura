#!/usr/bin/env python3
"""Append worldwide GLEIF legal-entity records to the B2B corpus.

This is the production-safe counterpart to the original one-shot GLEIF pilot:
it opens ``company_profiles.jsonl`` in append mode, reads existing profile IDs
and LEIs first, and only writes new legal entities.

GLEIF is an official identifier and legal-entity source. It does not provide
public company phones, emails, websites, or product catalogs; those remain in
the separate permitted-source enrichment lanes.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

API = "https://api.gleif.org/api/v1/lei-records"
USER_AGENT = "SaharaIndexBot/1.0 (+https://saharaindex.com/companies/)"

AMERICAS = {
    "AG": "Antigua and Barbuda",
    "AR": "Argentina",
    "BB": "Barbados",
    "BM": "Bermuda",
    "BO": "Bolivia",
    "BR": "Brazil",
    "BS": "Bahamas",
    "BZ": "Belize",
    "CA": "Canada",
    "CL": "Chile",
    "CO": "Colombia",
    "CR": "Costa Rica",
    "CU": "Cuba",
    "DO": "Dominican Republic",
    "EC": "Ecuador",
    "GT": "Guatemala",
    "HN": "Honduras",
    "JM": "Jamaica",
    "KY": "Cayman Islands",
    "MX": "Mexico",
    "NI": "Nicaragua",
    "PA": "Panama",
    "PE": "Peru",
    "PR": "Puerto Rico",
    "PY": "Paraguay",
    "SV": "El Salvador",
    "TT": "Trinidad and Tobago",
    "US": "United States",
    "UY": "Uruguay",
    "VE": "Venezuela",
    "VG": "British Virgin Islands",
    "VI": "U.S. Virgin Islands",
}

ASIA_PACIFIC = {
    "AU": "Australia",
    "BD": "Bangladesh",
    "BN": "Brunei",
    "CN": "China",
    "FJ": "Fiji",
    "HK": "Hong Kong",
    "ID": "Indonesia",
    "IN": "India",
    "JP": "Japan",
    "KH": "Cambodia",
    "KR": "South Korea",
    "KZ": "Kazakhstan",
    "LA": "Laos",
    "LK": "Sri Lanka",
    "MM": "Myanmar",
    "MN": "Mongolia",
    "MO": "Macau",
    "MY": "Malaysia",
    "NC": "New Caledonia",
    "NP": "Nepal",
    "NZ": "New Zealand",
    "PH": "Philippines",
    "PK": "Pakistan",
    "SG": "Singapore",
    "TH": "Thailand",
    "TW": "Taiwan",
    "UZ": "Uzbekistan",
    "VN": "Vietnam",
}

AFRICA = {
    "AO": "Angola",
    "BF": "Burkina Faso",
    "BJ": "Benin",
    "BW": "Botswana",
    "CI": "Cote d'Ivoire",
    "CM": "Cameroon",
    "ET": "Ethiopia",
    "GA": "Gabon",
    "GH": "Ghana",
    "GM": "Gambia",
    "KE": "Kenya",
    "LR": "Liberia",
    "LS": "Lesotho",
    "MG": "Madagascar",
    "MU": "Mauritius",
    "MW": "Malawi",
    "MZ": "Mozambique",
    "NA": "Namibia",
    "NG": "Nigeria",
    "RW": "Rwanda",
    "SC": "Seychelles",
    "SN": "Senegal",
    "TZ": "Tanzania",
    "UG": "Uganda",
    "ZA": "South Africa",
    "ZM": "Zambia",
    "ZW": "Zimbabwe",
}

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
    "AE": "United Arab Emirates",
    "BH": "Bahrain",
    "DZ": "Algeria",
    "EG": "Egypt",
    "IL": "Israel",
    "IQ": "Iraq",
    "IR": "Iran",
    "JO": "Jordan",
    "KW": "Kuwait",
    "LB": "Lebanon",
    "LY": "Libya",
    "MA": "Morocco",
    "OM": "Oman",
    "PS": "Palestine",
    "QA": "Qatar",
    "SA": "Saudi Arabia",
    "SY": "Syria",
    "TN": "Tunisia",
    "TR": "Turkey",
    "YE": "Yemen",
}

REGION_COUNTRIES = {
    "Americas": AMERICAS,
    "Asia-Pacific": ASIA_PACIFIC,
    "Africa": AFRICA,
    "Europe": EUROPE,
    "MENA": MENA,
}

COUNTRIES = {
    code: name
    for countries in REGION_COUNTRIES.values()
    for code, name in countries.items()
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="data/b2b", help="B2B output directory")
    parser.add_argument("--limit", type=int, default=30_000, help="new profiles to append")
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--sleep", type=float, default=0.05)
    parser.add_argument(
        "--countries",
        default="",
        help="optional comma-separated ISO2 country list; default is global priority order",
    )
    parser.add_argument(
        "--regions",
        default="Americas,Asia-Pacific,Africa,Europe,MENA",
        help="comma-separated regions to include when --countries is not set",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    out = Path(args.out)
    profiles_path = out / "company_profiles.jsonl"
    raw_dir = out / "sources" / "gleif-worldwide"
    manifest_path = out / "sources" / "gleif-worldwide-manifest.json"
    out.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)

    existing_ids, existing_leis = read_existing_profiles(profiles_path)
    countries = requested_countries(args.countries, args.regions)
    if not countries:
        raise SystemExit("no countries selected")

    stats: dict[str, dict[str, int]] = defaultdict(
        lambda: {
            "pages": 0,
            "fetched": 0,
            "appended": 0,
            "skipped_existing": 0,
            "skipped_invalid": 0,
            "errors": 0,
        }
    )
    pages = {country: 1 for country in countries}
    exhausted: set[str] = set()
    appended = 0
    fetched = 0
    skipped_existing = 0
    errors = 0
    now = int(time.time())

    mode = "a" if profiles_path.exists() else "w"
    with profiles_path.open(mode, encoding="utf-8") as profiles:
        while appended < args.limit and len(exhausted) < len(countries):
            made_progress = False
            for country in countries:
                if appended >= args.limit:
                    break
                if country in exhausted:
                    continue
                page = pages[country]
                try:
                    payload = request_json(country, page, args.page_size)
                except Exception as exc:  # noqa: BLE001
                    errors += 1
                    stats[country]["errors"] += 1
                    print(f"warn: {country} page {page} failed: {exc}", file=sys.stderr)
                    exhausted.add(country)
                    continue

                records = payload.get("data") or []
                if not records:
                    exhausted.add(country)
                    continue

                stats[country]["pages"] += 1
                stats[country]["fetched"] += len(records)
                fetched += len(records)
                raw_path = raw_dir / f"{country.lower()}-{page}.json"
                if not args.dry_run:
                    raw_path.write_text(json.dumps(payload), encoding="utf-8")

                for record in records:
                    attrs = record.get("attributes") or {}
                    lei = text(attrs.get("lei")).upper()
                    profile = profile_from_record(record, now)
                    if not profile:
                        stats[country]["skipped_invalid"] += 1
                        continue
                    profile_id = profile.get("id") if profile else ""
                    if not lei or lei in existing_leis or profile_id in existing_ids:
                        skipped_existing += 1
                        stats[country]["skipped_existing"] += 1
                        continue
                    if not args.dry_run:
                        profiles.write(json.dumps(profile, ensure_ascii=False, sort_keys=True) + "\n")
                        profiles.flush()
                    existing_leis.add(lei)
                    existing_ids.add(str(profile_id))
                    appended += 1
                    stats[country]["appended"] += 1
                    made_progress = True
                    if appended >= args.limit:
                        break

                pages[country] += 1
                print(
                    f"{appended}/{args.limit} new GLEIF profiles after {country} page {page} "
                    f"({stats[country]['appended']} new in {country})",
                    flush=True,
                )
                time.sleep(args.sleep)
            if not made_progress and len(exhausted) < len(countries):
                # Continue through countries with duplicate-heavy first pages, but surface
                # the condition in logs so long runs are diagnosable.
                print(
                    f"progress note: no new profiles in this pass; {len(exhausted)}/{len(countries)} countries exhausted",
                    flush=True,
                )

    manifest = {
        "source": "GLEIF Global LEI Index API",
        "api": API,
        "countries": countries,
        "dry_run": args.dry_run,
        "limit": args.limit,
        "page_size": args.page_size,
        "generated_at_epoch": now,
        "existing_profile_ids_before_run": len(existing_ids) - appended,
        "existing_leis_before_run": len(existing_leis) - appended,
        "fetched": fetched,
        "appended": appended,
        "skipped_existing": skipped_existing,
        "errors": errors,
        "stats_by_country": dict(sorted(stats.items())),
    }
    if not args.dry_run:
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


def request_json(country: str, page: int, size: int) -> dict[str, Any]:
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
            "User-Agent": USER_AGENT,
        },
    )
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, timeout=45) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if exc.code not in {429, 500, 502, 503, 504} or attempt == 4:
                raise
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"failed to fetch {url}")


def requested_countries(countries_arg: str, regions_arg: str) -> list[str]:
    if countries_arg.strip():
        return unique(
            text(item).upper()
            for item in countries_arg.split(",")
            if text(item)
        )
    regions = [text(item) for item in regions_arg.split(",") if text(item)]
    countries: list[str] = []
    for region in regions:
        values = REGION_COUNTRIES.get(region)
        if not values:
            raise SystemExit(f"unknown region {region!r}; expected one of {', '.join(REGION_COUNTRIES)}")
        countries.extend(values.keys())
    return unique(countries)


def profile_from_record(record: dict[str, Any], now: int) -> dict[str, Any] | None:
    attrs = record.get("attributes") or {}
    entity = attrs.get("entity") or {}
    registration = attrs.get("registration") or {}
    lei = text(attrs.get("lei")).upper()
    name = text(deep_get(entity, "legalName", "name"))
    legal_address = entity.get("legalAddress") or {}
    country = text(legal_address.get("country")).upper()
    if not lei or not name or not country:
        return None

    source_url = text(deep_get(record, "links", "self")) or f"{API}/{urllib.parse.quote(lei)}"
    addresses = unique(
        value
        for value in [
            address_text(legal_address),
            address_text(entity.get("headquartersAddress") or {}),
        ]
        if value
    )
    country_label = country_name(country)
    region_label = region_for_country(country)
    status = text(entity.get("status")) or "ACTIVE"
    registration_status = text(registration.get("status")) or "ISSUED"
    legal_form = text(deep_get(entity, "legalForm", "id")) or text(deep_get(entity, "legalForm", "other"))
    registered_at = text(deep_get(entity, "registeredAt", "id")) or text(deep_get(entity, "registeredAt", "other"))
    validated_at = text(deep_get(registration, "validatedAt", "id")) or text(deep_get(registration, "validatedAt", "other"))

    evidence_items = [
        evidence("lei", lei, source_url),
        evidence("gleif_legal_name", name, source_url),
        evidence("gleif_entity_status", status, source_url),
        evidence("gleif_entity_category", text(entity.get("category")), source_url),
        evidence("gleif_entity_subcategory", text(entity.get("subCategory")), source_url),
        evidence("gleif_legal_form", legal_form, source_url),
        evidence("gleif_registered_at", registered_at, source_url),
        evidence("gleif_registered_as", text(entity.get("registeredAs")), source_url),
        evidence("gleif_jurisdiction", text(entity.get("jurisdiction")), source_url),
        evidence("gleif_creation_date", text(entity.get("creationDate")), source_url),
        evidence("gleif_initial_registration_date", text(registration.get("initialRegistrationDate")), source_url),
        evidence("gleif_last_update_date", text(registration.get("lastUpdateDate")), source_url),
        evidence("gleif_next_renewal_date", text(registration.get("nextRenewalDate")), source_url),
        evidence("gleif_registration_status", registration_status, source_url),
        evidence("gleif_corroboration_level", text(registration.get("corroborationLevel")), source_url),
        evidence("gleif_conformity_flag", text(attrs.get("conformityFlag")), source_url),
        evidence("gleif_managing_lou", text(registration.get("managingLou")), source_url),
        evidence("gleif_validated_at", validated_at, source_url),
        evidence("gleif_validated_as", text(registration.get("validatedAs")), source_url),
        evidence("source_rights", "gleif_open_lei_data", "https://www.gleif.org/en/lei-data/global-lei-index"),
    ]
    validation_score = 65 if addresses else 58
    description = (
        f"{name} is an active legal entity in {country_label} with LEI {lei}. "
        "This SaharaIndex profile is built from the official GLEIF Global LEI Index "
        "as a verified legal-entity baseline for B2B discovery and RFQ routing."
    )

    return {
        "id": slugify(f"{name}-{lei}"),
        "source_name": "GLEIF Global LEI Index",
        "source_url": "https://www.gleif.org/en/lei-data/global-lei-index",
        "profile_url": source_url,
        "canonical_domain": None,
        "company_name": name,
        "description": description,
        "region": region_label,
        "country": country_label,
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
        "evidence": [item for item in evidence_items if item["value"]],
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
                "lei_verified",
                "mautic_export_not_campaign_ready",
            ],
            "field_coverage": [
                "company_name",
                "location",
                "registry_identifier",
                "legal_entity_status",
                "registration_dates",
            ],
        },
        "tags": ["gleif", "lei", "worldwide", slugify(region_label), country.lower()],
        "scraped_at_epoch": now,
        "refresh_due_epoch": now + 30 * 24 * 60 * 60,
    }


def read_existing_profiles(path: Path) -> tuple[set[str], set[str]]:
    ids: set[str] = set()
    leis: set[str] = set()
    if not path.exists():
        return ids, leis
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                profile = json.loads(line)
            except json.JSONDecodeError:
                continue
            profile_id = profile.get("id")
            if isinstance(profile_id, str) and profile_id:
                ids.add(profile_id)
            for item in profile.get("evidence") or []:
                if not isinstance(item, dict):
                    continue
                if item.get("field") == "lei":
                    lei = text(item.get("value")).upper()
                    if lei:
                        leis.add(lei)
    return ids, leis


def address_text(address: dict[str, Any]) -> str:
    lines = [text(line) for line in address.get("addressLines") or [] if text(line)]
    for key in ("postalCode", "city", "region", "country"):
        value = text(address.get(key))
        if value:
            lines.append(value)
    return ", ".join(lines)


def region_for_country(country: str) -> str:
    for region, countries in REGION_COUNTRIES.items():
        if country in countries:
            return region
    return "Worldwide"


def country_name(country: str) -> str:
    return COUNTRIES.get(country, country)


def evidence(field: str, value: str, source_url: str) -> dict[str, str]:
    return {
        "field": field,
        "value": value,
        "source_url": source_url,
    }


def deep_get(value: Any, *keys: str) -> Any:
    current = value
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def slugify(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "-", value).strip("-").lower()
    return slug or "company"


def text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def unique(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            out.append(value)
    return out


if __name__ == "__main__":
    raise SystemExit(main())
