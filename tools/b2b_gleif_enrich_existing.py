#!/usr/bin/env python3
"""Append GLEIF legal-entity enrichment revisions for existing profiles.

This tool uses only the GLEIF Global LEI Index API/cache for LEI enrichment. It
does not access private databases, bypass restricted systems, or harvest
non-public personal contact data.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

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
ENRICHMENT_VERSION = "gleif-v1"
LEI_RE = re.compile(r"\b[0-9A-Z]{18}[0-9]{2}\b")
COUNTRY_TO_CODE = {name.casefold(): code for code, name in {**EUROPE, **MENA}.items()}


def deep_get(data: Any, *path: str) -> Any:
    current = data
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def first_text(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def latest_profiles(path: Path) -> list[dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                profile = json.loads(line)
                latest[profile["id"]] = profile
    return list(latest.values())


def load_gleif_cache(raw_dir: Path) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    if not raw_dir.exists():
        return records
    for path in sorted(raw_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"warn: cannot read {path}: {exc}", file=sys.stderr)
            continue
        for record in payload.get("data") or []:
            lei = deep_get(record, "attributes", "lei")
            if lei:
                records[str(lei).upper()] = record
    return records


def profile_lei(profile: dict[str, Any]) -> str | None:
    for evidence in profile.get("evidence") or []:
        if evidence.get("field") == "lei":
            value = first_text(evidence.get("value"))
            if value:
                return value.upper()
    for value in (profile.get("id"), profile.get("source_url"), profile.get("company_name")):
        if not isinstance(value, str):
            continue
        match = LEI_RE.search(value.upper())
        if match:
            return match.group(0)
    return None


def already_enriched(profile: dict[str, Any]) -> bool:
    return any(
        item.get("field") == "gleif_enrichment_version"
        and item.get("value") == ENRICHMENT_VERSION
        for item in profile.get("evidence") or []
    )


def request_json(url: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.api+json",
            "User-Agent": "ObscuraB2B/0.1 GLEIF enrichment",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def lookup_by_lei(lei: str) -> dict[str, Any] | None:
    payload = request_json(f"{API}/{urllib.parse.quote(lei)}")
    data = payload.get("data")
    return data if isinstance(data, dict) else None


def lookup_by_name(profile: dict[str, Any]) -> dict[str, Any] | None:
    name = first_text(profile.get("company_name"))
    if not name:
        return None
    params = {
        "filter[entity.legalName]": name,
        "filter[entity.status]": "ACTIVE",
        "filter[registration.status]": "ISSUED",
        "page[size]": "5",
    }
    country = country_code_for_profile(profile)
    if country:
        params["filter[entity.legalAddress.country]"] = country
    payload = request_json(f"{API}?{urllib.parse.urlencode(params)}")
    records = payload.get("data") or []
    normalized = normalize_name(name)
    for record in records:
        legal_name = deep_get(record, "attributes", "entity", "legalName", "name")
        if normalize_name(str(legal_name or "")) == normalized:
            return record
    return records[0] if len(records) == 1 else None


def country_code_for_profile(profile: dict[str, Any]) -> str | None:
    country = first_text(profile.get("country"))
    if not country:
        return None
    if len(country) == 2 and country.upper() in {**EUROPE, **MENA}:
        return country.upper()
    return COUNTRY_TO_CODE.get(country.casefold())


def normalize_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.casefold()).strip()


def relationship_link(record: dict[str, Any], kind: str, link_type: str) -> str | None:
    return first_text(deep_get(record, "relationships", kind, "links", link_type))


def add_evidence(
    evidence: list[dict[str, str]],
    seen: set[tuple[str, str, str]],
    field: str,
    value: Any,
    source_url: str,
) -> None:
    text = first_text(value)
    if not text:
        return
    key = (field, text, source_url)
    if key in seen:
        return
    seen.add(key)
    evidence.append({"field": field, "value": text, "source_url": source_url})


def associated_entity_summary(entity: dict[str, Any]) -> str | None:
    lei = first_text(deep_get(entity, "associatedEntity", "lei"))
    name = first_text(deep_get(entity, "associatedEntity", "name"))
    if lei and name:
        return f"{name} ({lei})"
    return name or lei


def relationship_summary(record: dict[str, Any], kind: str) -> str | None:
    relationship = relationship_link(record, kind, "relationship-record")
    lei_record = relationship_link(record, kind, "lei-record")
    if relationship and lei_record:
        return f"GLEIF {kind.replace('-', ' ')} relationship available"
    return None


def address_text(address: dict[str, Any]) -> str:
    lines = [str(line).strip() for line in address.get("addressLines") or [] if str(line).strip()]
    for key in ("postalCode", "city", "region", "country"):
        value = first_text(address.get(key))
        if value:
            lines.append(value)
    return ", ".join(lines)


def gleif_addresses(entity: dict[str, Any]) -> list[str]:
    addresses: list[str] = []
    for key in ("legalAddress", "headquartersAddress"):
        value = address_text(entity.get(key) or {})
        if value and value not in addresses:
            addresses.append(value)
    return addresses


def merge_unique(values: list[Any], additions: list[Any]) -> list[Any]:
    merged: list[Any] = []
    seen: set[str] = set()
    for value in [*values, *additions]:
        text = first_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        merged.append(value)
    return merged


def enriched_validation(validation: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": validation.get("status") or "review",
        "score": min(max(int(validation.get("score") or 0), 70), 100),
        "issues": list(validation.get("issues") or []),
        "compliance_flags": merge_unique(
            validation.get("compliance_flags") or [],
            [
                "source_basis:gleif_open_lei_data",
                "public_legal_entity_record",
                "lei_verified",
                "gleif_enriched",
            ],
        ),
        "field_coverage": merge_unique(
            validation.get("field_coverage") or [],
            ["lei", "legal_entity", "registration_dates", "legal_entity_status"],
        ),
    }


def enriched_description(profile: dict[str, Any], attrs: dict[str, Any], entity: dict[str, Any]) -> str:
    lei = first_text(attrs.get("lei"))
    legal_name = first_text(deep_get(entity, "legalName", "name"), profile.get("company_name"))
    country = first_text(profile.get("country"), entity.get("jurisdiction"))
    status = first_text(entity.get("status"))
    if not lei or not legal_name:
        return profile.get("description") or ""
    location = f" in {country}" if country else ""
    state = f" listed as {status.lower()}" if status else ""
    return (
        f"{legal_name} is a legal entity{location}{state} with LEI {lei}. "
        "This profile is verified against the GLEIF Global LEI Index and still "
        "requires official website, contact, and catalog enrichment before outreach."
    )


def enrich_profile(profile: dict[str, Any], record: dict[str, Any]) -> dict[str, Any]:
    attrs = record.get("attributes") or {}
    entity = attrs.get("entity") or {}
    registration = attrs.get("registration") or {}
    source_url = first_text(deep_get(record, "links", "self"), profile.get("source_url"), API) or API
    legal_form = " / ".join(
        part
        for part in [
            first_text(deep_get(entity, "legalForm", "id")),
            first_text(deep_get(entity, "legalForm", "other")),
        ]
        if part
    )
    registered_at = first_text(deep_get(entity, "registeredAt", "id"), deep_get(entity, "registeredAt", "other"))
    validated_at = first_text(
        deep_get(registration, "validatedAt", "id"),
        deep_get(registration, "validatedAt", "other"),
    )

    enriched = json.loads(json.dumps(profile))
    evidence = list(enriched.get("evidence") or [])
    seen = {
        (item.get("field") or "", item.get("value") or "", item.get("source_url") or "")
        for item in evidence
    }

    for field, value in [
        ("gleif_enrichment_version", ENRICHMENT_VERSION),
        ("lei", attrs.get("lei")),
        ("gleif_legal_name", deep_get(entity, "legalName", "name")),
        ("gleif_entity_status", entity.get("status")),
        ("gleif_entity_category", entity.get("category")),
        ("gleif_entity_subcategory", entity.get("subCategory")),
        ("gleif_legal_form", legal_form),
        ("gleif_legal_form_id", deep_get(entity, "legalForm", "id")),
        ("gleif_legal_form_other", deep_get(entity, "legalForm", "other")),
        ("gleif_registered_at", registered_at),
        ("gleif_registered_as", entity.get("registeredAs")),
        ("gleif_jurisdiction", entity.get("jurisdiction")),
        ("gleif_creation_date", entity.get("creationDate")),
        ("gleif_initial_registration_date", registration.get("initialRegistrationDate")),
        ("gleif_last_update_date", registration.get("lastUpdateDate")),
        ("gleif_next_renewal_date", registration.get("nextRenewalDate")),
        ("gleif_registration_status", registration.get("status")),
        ("gleif_corroboration_level", registration.get("corroborationLevel")),
        ("gleif_conformity_flag", attrs.get("conformityFlag")),
        ("gleif_managing_lou", registration.get("managingLou")),
        ("gleif_validated_at", validated_at),
        ("gleif_validated_as", registration.get("validatedAs")),
        ("gleif_associated_entity", associated_entity_summary(entity)),
        ("gleif_direct_parent", relationship_summary(record, "direct-parent")),
        ("gleif_ultimate_parent", relationship_summary(record, "ultimate-parent")),
        ("gleif_direct_parent_relationship_url", relationship_link(record, "direct-parent", "relationship-record")),
        ("gleif_ultimate_parent_relationship_url", relationship_link(record, "ultimate-parent", "relationship-record")),
        ("gleif_direct_parent_reporting_exception_url", relationship_link(record, "direct-parent", "reporting-exception")),
        ("gleif_ultimate_parent_reporting_exception_url", relationship_link(record, "ultimate-parent", "reporting-exception")),
    ]:
        add_evidence(evidence, seen, field, value, source_url)

    enriched["source_name"] = profile.get("source_name") or "GLEIF Global LEI Index"
    enriched["source_url"] = source_url
    enriched["profile_url"] = source_url
    enriched["description"] = enriched_description(enriched, attrs, entity)
    enriched["addresses"] = merge_unique(enriched.get("addresses") or [], gleif_addresses(entity))
    enriched["evidence"] = evidence
    enriched["validation"] = enriched_validation(enriched.get("validation") or {})
    enriched["tags"] = merge_unique(enriched.get("tags") or [], ["gleif-enriched", "lei-verified"])
    enriched["scraped_at_epoch"] = int(time.time())
    return enriched


def write_summary(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="data/b2b")
    parser.add_argument("--profiles")
    parser.add_argument("--source-dir")
    parser.add_argument("--summary-path")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--api-search", action="store_true")
    parser.add_argument("--cached-only", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sleep", type=float, default=0.05)
    args = parser.parse_args()

    out = Path(args.out)
    profiles_path = Path(args.profiles) if args.profiles else out / "company_profiles.jsonl"
    source_dir = Path(args.source_dir) if args.source_dir else out / "sources" / "gleif"
    summary_path = (
        Path(args.summary_path)
        if args.summary_path
        else out / "enrichment" / "gleif-existing-enrichment.json"
    )

    profiles = latest_profiles(profiles_path)
    records = load_gleif_cache(source_dir)
    counters: dict[str, Any] = {
        "profiles_seen": len(profiles),
        "cache_records": len(records),
        "processed": 0,
        "already_enriched": 0,
        "matched_cache": 0,
        "matched_api": 0,
        "unmatched": 0,
        "appended": 0,
    }
    enriched_rows: list[dict[str, Any]] = []

    for profile in profiles:
        if args.limit is not None and counters["processed"] >= args.limit:
            break
        counters["processed"] += 1
        if not args.force and already_enriched(profile):
            counters["already_enriched"] += 1
            continue

        record = None
        lei = profile_lei(profile)
        if lei:
            record = records.get(lei)
            if record:
                counters["matched_cache"] += 1

        if record is None and args.api_search and not args.cached_only:
            try:
                record = lookup_by_lei(lei) if lei else lookup_by_name(profile)
                if record:
                    counters["matched_api"] += 1
                if args.sleep > 0:
                    time.sleep(args.sleep)
            except Exception as exc:
                print(f"warn: GLEIF API lookup failed for {profile.get('company_name')}: {exc}", file=sys.stderr)

        if record is None:
            counters["unmatched"] += 1
            continue
        enriched_rows.append(enrich_profile(profile, record))

    if enriched_rows and not args.dry_run:
        with profiles_path.open("a", encoding="utf-8") as handle:
            for profile in enriched_rows:
                handle.write(json.dumps(profile, ensure_ascii=False, separators=(",", ":")))
                handle.write("\n")
        counters["appended"] = len(enriched_rows)

    summary = {
        **counters,
        "source": "GLEIF Global LEI Index API/cache",
        "source_dir": str(source_dir),
        "profiles_path": str(profiles_path),
        "api_search": args.api_search and not args.cached_only,
        "dry_run": args.dry_run,
        "enrichment_version": ENRICHMENT_VERSION,
        "updated_at_epoch": int(time.time()),
    }
    write_summary(summary_path, summary)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
