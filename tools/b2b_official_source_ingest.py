#!/usr/bin/env python3
"""Append official/open registry company profiles to the B2B corpus.

The ingesters in this file use sanctioned API endpoints or documented open
data services. They keep only company-level fields and do not store personal
directors, private employee contacts, or scraped directory data.
"""

from __future__ import annotations

import argparse
import io
import json
import re
import sys
import tempfile
import time
import zipfile
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, TextIO

USER_AGENT = "SaharaIndexBot/1.0 (+https://saharaindex.com/companies/)"

FRANCE_DOC_URL = "https://www.data.gouv.fr/dataservices/api-recherche-dentreprises/"
FRANCE_API_URL = "https://recherche-entreprises.api.gouv.fr/search"
FRANCE_PROFILE_URL = "https://annuaire-entreprises.data.gouv.fr/entreprise/{siren}"

FINLAND_DOC_URL = "https://avoindata.prh.fi/opendata-ytj-api/v3/schema?lang=en"
FINLAND_API_URL = "https://avoindata.prh.fi/opendata-ytj-api/v3/companies"
FINLAND_PROFILE_URL = "https://avoindata.prh.fi/opendata-ytj-api/v3/companies?businessId={business_id}"

NORWAY_DOC_URL = "https://data.brreg.no/enhetsregisteret/api/docs/index.html"
NORWAY_API_URL = "https://data.brreg.no/enhetsregisteret/api/enheter"
NORWAY_PROFILE_URL = "https://data.brreg.no/enhetsregisteret/api/enheter/{orgnr}"

ESTONIA_DOC_URL = "https://avaandmed.ariregister.rik.ee/en/downloading-open-data"
ESTONIA_OPEN_DATA_URL = (
    "https://avaandmed.ariregister.rik.ee/sites/default/files/"
    "avaandmed/ettevotja_rekvisiidid__yldandmed.json.zip"
)
ESTONIA_PROFILE_URL = "https://ariregister.rik.ee/est/company/{registry_code}"

GENERIC_EMAIL_DOMAINS = {
    "gmail.com",
    "yahoo.com",
    "outlook.com",
    "hotmail.com",
    "icloud.com",
    "mail.ru",
    "gmx.com",
    "gmx.at",
    "yandex.com",
    "yandex.ru",
    "protonmail.com",
    "live.com",
    "mail.com",
    "aol.com",
}

MANUFACTURING_NACE_PREFIXES = tuple(f"{idx:02d}" for idx in range(10, 34))


@dataclass
class FetchStats:
    source: str
    fetched: int = 0
    appended: int = 0
    skipped_existing: int = 0
    skipped_invalid: int = 0
    errors: int = 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="data/b2b", help="B2B output directory")
    parser.add_argument(
        "--sources",
        default="france,norway,finland",
        help="Comma-separated sources: france,norway,finland,estonia",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Maximum records to append per source",
    )
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--delay", type=float, default=0.2)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--update-existing",
        action="store_true",
        help="Append a fresh revision even when profile id already exists",
    )
    args = parser.parse_args()

    out_dir = Path(args.out)
    profiles_path = out_dir / "company_profiles.jsonl"
    out_dir.mkdir(parents=True, exist_ok=True)
    existing_ids = read_existing_ids(profiles_path)
    requested = [item.strip().lower() for item in args.sources.split(",") if item.strip()]
    all_stats: list[FetchStats] = []

    with profiles_path.open("a", encoding="utf-8") as handle:
        for source in requested:
            if source == "france":
                stats = ingest_france(
                    handle,
                    existing_ids,
                    limit=args.limit,
                    page_size=min(args.page_size, 25),
                    delay=args.delay,
                    dry_run=args.dry_run,
                    update_existing=args.update_existing,
                )
            elif source == "norway":
                stats = ingest_norway(
                    handle,
                    existing_ids,
                    limit=args.limit,
                    page_size=min(args.page_size, 100),
                    delay=args.delay,
                    dry_run=args.dry_run,
                    update_existing=args.update_existing,
                )
            elif source == "finland":
                stats = ingest_finland(
                    handle,
                    existing_ids,
                    limit=args.limit,
                    page_size=min(args.page_size, 100),
                    delay=args.delay,
                    dry_run=args.dry_run,
                    update_existing=args.update_existing,
                )
            elif source == "estonia":
                stats = ingest_estonia(
                    handle,
                    existing_ids,
                    limit=args.limit,
                    page_size=min(args.page_size, 5),
                    delay=args.delay,
                    dry_run=args.dry_run,
                    update_existing=args.update_existing,
                )
            else:
                print(f"unknown source: {source}", file=sys.stderr)
                continue
            all_stats.append(stats)

    summary = {
        "dry_run": args.dry_run,
        "profiles_path": str(profiles_path),
        "stats": [stats.__dict__ for stats in all_stats],
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def ingest_france(
    handle: Any,
    existing_ids: set[str],
    *,
    limit: int,
    page_size: int,
    delay: float,
    dry_run: bool,
    update_existing: bool,
) -> FetchStats:
    stats = FetchStats("France Annuaire des Entreprises API")
    queries = [
        {"section_activite_principale": "C"},
        {"activite_principale": "46.90Z"},
        {"activite_principale": "46.39B"},
        {"activite_principale": "46.69B"},
    ]
    per_query_limit = max(1, limit // len(queries))

    for query in queries:
        query_target = min(limit, stats.appended + per_query_limit)
        page = 1
        while stats.appended < query_target:
            params = {
                **query,
                "page": str(page),
                "per_page": str(page_size),
                "etat_administratif": "A",
                "est_siege": "true",
            }
            try:
                payload = fetch_json(FRANCE_API_URL, params)
            except Exception as error:  # noqa: BLE001
                stats.errors += 1
                print(f"france fetch failed page={page} query={query}: {error}", file=sys.stderr)
                break

            records = payload.get("results") or []
            if not records:
                break

            for record in records:
                stats.fetched += 1
                profile = france_profile(record)
                append_profile(
                    handle,
                    profile,
                    existing_ids,
                    stats,
                    dry_run=dry_run,
                    update_existing=update_existing,
                )
                if stats.appended >= query_target:
                    break

            page += 1
            time.sleep(delay)

    return stats


def ingest_norway(
    handle: Any,
    existing_ids: set[str],
    *,
    limit: int,
    page_size: int,
    delay: float,
    dry_run: bool,
    update_existing: bool,
) -> FetchStats:
    stats = FetchStats("Norway Bronnoysund Register Centre API")
    nace_queries = [
        "10",
        "11",
        "12",
        "13",
        "14",
        "15",
        "16",
        "17",
        "18",
        "19",
        "20",
        "21",
        "22",
        "23",
        "24",
        "25",
        "26",
        "27",
        "28",
        "29",
        "30",
        "31",
        "32",
        "33",
        "46",
        "52",
    ]
    per_query_limit = max(1, limit // len(nace_queries))

    for nace in nace_queries:
        query_target = min(limit, stats.appended + per_query_limit)
        page = 0
        while stats.appended < query_target:
            params = {
                "naeringskode": nace,
                "size": str(page_size),
                "page": str(page),
            }
            try:
                payload = fetch_json(NORWAY_API_URL, params)
            except Exception as error:  # noqa: BLE001
                stats.errors += 1
                print(f"norway fetch failed page={page} nace={nace}: {error}", file=sys.stderr)
                break

            records = (payload.get("_embedded") or {}).get("enheter") or []
            if not records:
                break

            for record in records:
                stats.fetched += 1
                profile = norway_profile(record)
                append_profile(
                    handle,
                    profile,
                    existing_ids,
                    stats,
                    dry_run=dry_run,
                    update_existing=update_existing,
                )
                if stats.appended >= query_target:
                    break

            page += 1
            time.sleep(delay)

    return stats


def ingest_finland(
    handle: Any,
    existing_ids: set[str],
    *,
    limit: int,
    page_size: int,
    delay: float,
    dry_run: bool,
    update_existing: bool,
) -> FetchStats:
    stats = FetchStats("Finland PRH YTJ Open Data API")
    # Use broad name prefixes to discover active profiles without query terms that require
    # any credentials or paywall access.
    queries = [
        "a",
        "e",
        "i",
        "o",
        "u",
        "k",
        "m",
        "p",
        "r",
        "t",
        "s",
    ]
    per_query_limit = max(1, limit // len(queries))

    for query in queries:
        query_target = min(limit, stats.appended + per_query_limit)
        page = 1
        while stats.appended < query_target:
            params = {
                "name": query,
                "page": str(page),
                "pageSize": str(page_size),
            }
            try:
                payload = fetch_json(FINLAND_API_URL, params)
            except Exception as error:  # noqa: BLE001
                stats.errors += 1
                print(f"finland fetch failed page={page} query={query}: {error}", file=sys.stderr)
                break

            records = payload.get("companies") or []
            if not records:
                break

            for record in records:
                stats.fetched += 1
                profile = finland_profile(record)
                append_profile(
                    handle,
                    profile,
                    existing_ids,
                    stats,
                    dry_run=dry_run,
                    update_existing=update_existing,
                )
                if stats.appended >= query_target:
                    break

            page += 1
            time.sleep(delay)

    return stats


def ingest_estonia(
    handle: Any,
    existing_ids: set[str],
    *,
    limit: int,
    page_size: int,
    delay: float,
    dry_run: bool,
    update_existing: bool,
) -> FetchStats:
    stats = FetchStats("Estonia e-Business Register Open Data")
    del page_size  # Unused for streamed file source.
    del delay  # Unused for streamed file source.

    try:
        data_path = fetch_to_temp_file(ESTONIA_OPEN_DATA_URL)
    except Exception as error:  # noqa: BLE001
        stats.errors += 1
        print(f"estonia download failed: {error}", file=sys.stderr)
        return stats

    try:
        with zipfile.ZipFile(data_path) as archive:
            members = [name for name in archive.namelist() if name.lower().endswith(".json")]
            if not members:
                raise RuntimeError("no json members found in Estonia open-data archive")
            with archive.open(members[0]) as archive_file:
                text_source = io.TextIOWrapper(archive_file, encoding="utf-8", errors="replace")
                # The archive payload is a single JSON array; parse object-by-object to avoid
                # loading millions of records into memory.
                for record in iter_json_array_records(text_source):
                    stats.fetched += 1
                    profile = estonia_profile(record)
                    append_profile(
                        handle,
                        profile,
                        existing_ids,
                        stats,
                        dry_run=dry_run,
                        update_existing=update_existing,
                    )
                    if stats.appended >= limit:
                        break
    except Exception as error:  # noqa: BLE001
        stats.errors += 1
        print(f"estonia parse failed: {error}", file=sys.stderr)
    finally:
        data_path.unlink(missing_ok=True)

    return stats


def append_profile(
    handle: Any,
    profile: dict[str, Any] | None,
    existing_ids: set[str],
    stats: FetchStats,
    *,
    dry_run: bool,
    update_existing: bool,
) -> None:
    if not profile:
        stats.skipped_invalid += 1
        return
    profile_id = profile["id"]
    if profile_id in existing_ids and not update_existing:
        stats.skipped_existing += 1
        return
    if not dry_run:
        handle.write(json.dumps(profile, ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()
        existing_ids.add(profile_id)
    stats.appended += 1


def france_profile(record: dict[str, Any]) -> dict[str, Any] | None:
    siren = text(record.get("siren"))
    name = clean_name(record.get("nom_complet") or record.get("nom_raison_sociale"))
    if not siren or not name:
        return None

    activity_code = text(record.get("activite_principale"))
    activity = activity_code
    section = text(record.get("section_activite_principale"))
    company_type = company_type_from_code(activity_code, section)
    siege = record.get("siege") or {}
    address = text(siege.get("adresse"))
    city = text(siege.get("libelle_commune"))
    profile_url = FRANCE_PROFILE_URL.format(siren=siren)
    now = now_epoch()
    industries = [activity_label(activity_code, "NAF activity")]
    specializations = [item for item in [activity_code, text(record.get("categorie_entreprise"))] if item]

    evidence_items = [
        evidence("registration_number", siren, profile_url),
        evidence("registry_status", "active", profile_url),
        evidence("activity_code", activity_code, profile_url),
        evidence("activity_section", section, profile_url),
        evidence("source_rights", "official_open_api", FRANCE_DOC_URL),
    ]
    if city:
        evidence_items.append(evidence("city", city, profile_url))

    description = (
        f"{name} is an active French {company_type} profile from the official "
        f"Annuaire des Entreprises API. Activity code: {activity or 'not supplied'}."
    )

    return company_profile(
        profile_id=f"fr-siren-{siren}",
        source_name="France Annuaire des Entreprises API",
        source_url=FRANCE_DOC_URL,
        profile_url=profile_url,
        company_name=name,
        description=description,
        region="Europe",
        country="France",
        company_type=company_type,
        industries=industries,
        specializations=specializations,
        websites=[],
        emails=[],
        phones=[],
        addresses=[address] if address else [],
        company_size=text(record.get("tranche_effectif_salarie")) or None,
        evidence_items=evidence_items,
        tags=["official_registry", "open_api", "france", "annuaire_entreprises"],
        now=now,
    )


def norway_profile(record: dict[str, Any]) -> dict[str, Any] | None:
    orgnr = text(record.get("organisasjonsnummer"))
    name = clean_name(record.get("navn"))
    if not orgnr or not name:
        return None

    activity_node = record.get("naeringskode1") or {}
    activity_code = text(activity_node.get("kode"))
    activity_desc = text(activity_node.get("beskrivelse"))
    company_type = company_type_from_code(activity_code, "")
    address = norway_address(record.get("forretningsadresse") or record.get("postadresse") or {})
    website = normalize_website(record.get("hjemmeside"))
    email = normalize_email(record.get("epostadresse"))
    if email and not is_role_email(email):
        email = ""
    phones = dedupe(
        item
        for item in [
            normalize_phone(record.get("telefon"), default_country_code="47"),
            normalize_phone(record.get("mobil"), default_country_code="47"),
        ]
        if item
    )
    profile_url = NORWAY_PROFILE_URL.format(orgnr=orgnr)
    now = now_epoch()
    industries = [activity_desc or activity_label(activity_code, "NACE activity")]
    evidence_items = [
        evidence("registration_number", orgnr, profile_url),
        evidence("activity_code", activity_code, profile_url),
        evidence("activity_description", activity_desc, profile_url),
        evidence("source_rights", "official_open_api", NORWAY_DOC_URL),
    ]
    if website:
        evidence_items.append(evidence("website", website, profile_url))
    if email:
        evidence_items.append(evidence("email", email, profile_url))
    for phone in phones:
        evidence_items.append(evidence("phone", phone, profile_url))

    description = (
        f"{name} is a Norwegian {company_type} profile from the official "
        f"Bronnoysund Register Centre open API. Activity: {activity_desc or activity_code or 'not supplied'}."
    )

    return company_profile(
        profile_id=f"no-orgnr-{orgnr}",
        source_name="Norway Bronnoysund Register Centre API",
        source_url=NORWAY_DOC_URL,
        profile_url=profile_url,
        company_name=name,
        description=description,
        region="Europe",
        country="Norway",
        company_type=company_type,
        industries=industries,
        specializations=[item for item in [activity_code, activity_desc] if item],
        websites=[website] if website else [],
        emails=[email] if email else [],
        phones=phones,
        addresses=[address] if address else [],
        company_size=None,
        evidence_items=evidence_items,
        tags=["official_registry", "open_api", "norway", "bronnoysund"],
        now=now,
    )


def finland_profile(record: dict[str, Any]) -> dict[str, Any] | None:
    business_id = text((record.get("businessId") or {}).get("value"))
    name = finland_company_name(record.get("names") or [])
    if not business_id or not name:
        return None
    if text(record.get("tradeRegisterStatus")) and text(record.get("tradeRegisterStatus")) != "1":
        return None

    main_activity = record.get("mainBusinessLine") or {}
    activity_code = text(main_activity.get("type"))
    activity_desc = selected_description(main_activity.get("descriptions"))
    company_type = company_type_from_code(activity_code, "")
    website = normalize_website((record.get("website") or {}).get("url"))
    profile_url = FINLAND_PROFILE_URL.format(business_id=business_id)
    now = now_epoch()
    address = finland_address(record.get("addresses") or [])
    industries = [activity_label(activity_code, "NACE activity"), activity_desc]
    evidence_items = [
        evidence("registration_number", business_id, profile_url),
        evidence("registry_status", text(record.get("tradeRegisterStatus")), profile_url),
        evidence("activity_code", activity_code, profile_url),
        evidence("source_rights", "official_open_api", FINLAND_DOC_URL),
    ]
    if activity_desc:
        evidence_items.append(evidence("activity_description", activity_desc, profile_url))
    if address:
        evidence_items.append(evidence("address", address, profile_url))
    if website:
        evidence_items.append(evidence("website", website, profile_url))

    description = (
        f"{name} is a Finland PRH YTJ registry company profile from the official open-data API. "
        f"Main activity: {activity_desc or activity_code or 'not supplied'}."
    )

    return company_profile(
        profile_id=f"fi-businessid-{business_id}",
        source_name="Finland PRH YTJ Open Data API",
        source_url=FINLAND_DOC_URL,
        profile_url=profile_url,
        company_name=name,
        description=description,
        region="Europe",
        country="Finland",
        company_type=company_type,
        industries=[item for item in industries if item],
        specializations=[item for item in [activity_code, activity_desc] if item],
        websites=[website] if website else [],
        emails=[],
        phones=[],
        addresses=[address] if address else [],
        company_size=None,
        evidence_items=evidence_items,
        tags=["official_registry", "open_api", "finland", "ytj"],
        now=now,
    )


def estonia_profile(record: dict[str, Any]) -> dict[str, Any] | None:
    registry_code = text(record.get("ariregistri_kood"))
    name = clean_name(record.get("nimi"))
    details = record.get("yldandmed") or {}
    if not registry_code or not name:
        return None
    if not is_estonian_business_name(name):
        return None
    if text(details.get("staatus")) and text(details.get("staatus")) != "R":
        return None

    websites, emails, phones = estonia_contacts(details.get("sidevahendid") or [])
    if not emails and not phones and not websites:
        return None

    profile_url = ESTONIA_PROFILE_URL.format(registry_code=registry_code)
    now = now_epoch()
    address = estonia_address(details.get("aadressid") or [])
    activity_code, activity_desc = estonia_activity(details.get("teatatud_tegevusalad") or [])
    company_type = company_type_from_code(activity_code, "")
    evidence_items = [
        evidence("registration_number", registry_code, profile_url),
        evidence("registry_status", text(details.get("staatus")), profile_url),
        evidence("source_rights", "official_open_data", ESTONIA_DOC_URL),
    ]
    if activity_code:
        evidence_items.append(evidence("activity_code", activity_code, profile_url))
    if activity_desc:
        evidence_items.append(evidence("activity_description", activity_desc, profile_url))
    for website in websites:
        evidence_items.append(evidence("website", website, profile_url))
    for email in emails:
        evidence_items.append(evidence("email", email, profile_url))
    for phone in phones:
        evidence_items.append(evidence("phone", phone, profile_url))
    if address:
        evidence_items.append(evidence("address", address, profile_url))

    description = (
        f"{name} is an Estonia e-Business Register company profile from official open data. "
        f"Main activity: {activity_desc or activity_code or 'not supplied'}."
    )

    return company_profile(
        profile_id=f"ee-ariregister-{registry_code}",
        source_name="Estonia e-Business Register Open Data",
        source_url=ESTONIA_DOC_URL,
        profile_url=profile_url,
        company_name=name,
        description=description,
        region="Europe",
        country="Estonia",
        company_type=company_type,
        industries=[item for item in [activity_label(activity_code, "EMTAK activity")] if item],
        specializations=[item for item in [activity_code, activity_desc] if item],
        websites=websites,
        emails=emails,
        phones=phones,
        addresses=[address] if address else [],
        company_size=None,
        evidence_items=evidence_items,
        tags=["official_registry", "open_data", "estonia", "e-business_register"],
        now=now,
    )


def company_profile(
    *,
    profile_id: str,
    source_name: str,
    source_url: str,
    profile_url: str,
    company_name: str,
    description: str,
    region: str,
    country: str,
    company_type: str,
    industries: list[str],
    specializations: list[str],
    websites: list[str],
    emails: list[str],
    phones: list[str],
    addresses: list[str],
    company_size: str | None,
    evidence_items: list[dict[str, str]],
    tags: list[str],
    now: int,
) -> dict[str, Any]:
    score = 20 + 15 + 10 + 5
    coverage = ["company_name", "description", "location", "classification"]
    role_emails = dedupe(email for email in emails if is_role_email(email))
    if websites:
        score += 10
        coverage.append("domain")
    if role_emails:
        score += 15
        coverage.append("email")
    if phones:
        score += 10
        coverage.append("phone")
    status = "review" if score >= 45 else "low_confidence"
    issues = ["manual_review_recommended"]
    if not role_emails:
        issues.append("missing_email")
    if not websites:
        issues.append("missing_domain")

    return {
        "id": slugify(profile_id),
        "source_name": source_name,
        "source_url": source_url,
        "profile_url": profile_url,
        "canonical_domain": apex_domain(websites[0]) if websites else None,
        "company_name": company_name,
        "description": description,
        "region": region,
        "country": country,
        "company_type": company_type,
        "industries": [item for item in industries if item],
        "specializations": dedupe([item for item in specializations if item]),
        "products": [],
        "services": [],
        "contacts": {
            "emails": [
                contact_point(email, "registry_email", profile_url, 0.9)
                for email in role_emails
            ],
            "phones": [
                contact_point(phone, "registry_phone", profile_url, 0.9)
                for phone in dedupe(phones)
            ],
            "websites": websites,
            "social_links": [],
        },
        "addresses": addresses,
        "company_size": company_size,
        "revenue": None,
        "personnel": [],
        "evidence": [item for item in evidence_items if item["value"]],
        "validation": {
            "status": status,
            "score": min(score, 100),
            "issues": issues,
            "compliance_flags": [
                "robots_txt_policy_not_applicable_api",
                "source_basis:official_open_api",
                "company_level_contacts_only",
                "mautic_default_export_role_based_emails_only",
            ],
            "field_coverage": coverage,
        },
        "tags": tags,
        "scraped_at_epoch": now,
        "refresh_due_epoch": now + 30 * 24 * 60 * 60,
    }


def fetch_json(url: str, params: dict[str, str]) -> dict[str, Any]:
    full_url = f"{url}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(full_url, headers={"User-Agent": USER_AGENT})
    for attempt in range(5):
        try:
            with urllib.request.urlopen(request, timeout=45) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            if error.code not in {429, 500, 502, 503, 504} or attempt == 4:
                raise
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"failed to fetch {full_url}")


def read_existing_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    ids: set[str] = set()
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            profile_id = value.get("id")
            if isinstance(profile_id, str):
                ids.add(profile_id)
    return ids


def iter_json_array_records(source: TextIO) -> Iterator[dict[str, Any]]:
    started = False
    collecting = False
    depth = 0
    in_string = False
    escaped = False
    buf: list[str] = []
    while True:
        chunk = source.read(65536)
        if not chunk:
            break
        for char in chunk:
            if not started:
                if char == "[":
                    started = True
                continue

            if not collecting:
                if char != "{":
                    if char == "]":
                        return
                    continue
                collecting = True
                depth = 1
                buf = ["{"]
                continue

            buf.append(char)
            if in_string:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == "\"":
                    in_string = False
                continue

            if char == "\"":
                in_string = True
                continue
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    record = json.loads("".join(buf))
                    buf = []
                    collecting = False
                    yield record


def finland_company_name(names: list[dict[str, Any]]) -> str:
    for item in names:
        if not isinstance(item, dict):
            continue
        if text(item.get("type")) == "1" and not item.get("endDate"):
            return clean_name(item.get("name"))
    for item in names:
        if isinstance(item, dict):
            name = clean_name(item.get("name"))
            if name:
                return name
    return ""


def selected_description(descriptions: list[dict[str, Any]] | None, prefer: tuple[str, ...] = ("1", "3", "2")) -> str:
    if not descriptions:
        return ""
    for language in prefer:
        for item in descriptions:
            if text(item.get("languageCode")) == language:
                label = text(item.get("description"))
                if label:
                    return label
    for item in descriptions:
        description = text(item.get("description"))
        if description:
            return description
    return ""


def finland_address(addresses: list[dict[str, Any]]) -> str:
    for value in addresses:
        if not isinstance(value, dict):
            continue
        parts: list[str] = []
        line = text(value.get("street"))
        if line:
            number = text(value.get("buildingNumber"))
            if number:
                line = f"{line} {number}"
            parts.append(line)
        for post_office in value.get("postOffices") or []:
            city = text(post_office.get("city"))
            if city:
                parts.append(city)
                break
        for key in ("postCode", "co", "postOfficeBox"):
            item = text(value.get(key))
            if item:
                parts.append(item)
        if parts:
            return ", ".join(dedupe(parts))
    return ""


def estonia_activity(values: list[dict[str, Any]]) -> tuple[str, str]:
    for value in values:
        if not isinstance(value, dict):
            continue
        if value.get("on_pohitegevusala"):
            return text(value.get("emtak_kood")), text(value.get("emtak_tekstina"))
    if values:
        value = values[0]
        if isinstance(value, dict):
            return text(value.get("emtak_kood")), text(value.get("emtak_tekstina"))
    return "", ""


def estonia_address(values: list[dict[str, Any]]) -> str:
    for value in values:
        if not isinstance(value, dict):
            continue
        address = text(value.get("aadress_ads__ads_normaliseeritud_taisaadress"))
        if not address:
            address = text(value.get("aadress_ads__ads_normaliseeritud_taisaadress_tapsustus"))
        if not address:
            address = text(value.get("tanav_maja_korter"))
        city = text(value.get("ehak_nimetus"))
        postal_code = text(value.get("postiindeks"))
        if city:
            address = f"{address}, {city}" if address else city
        if postal_code:
            address = f"{address}, {postal_code}" if address else postal_code
        if address:
            return address
    return ""


def estonia_contacts(items: list[dict[str, Any]]) -> tuple[list[str], list[str], list[str]]:
    websites: list[str] = []
    emails: list[str] = []
    phones: list[str] = []
    seen_email_domains: set[str] = set()

    for item in items:
        if not isinstance(item, dict):
            continue
        if item.get("lopp_kpv"):
            continue
        raw = text(item.get("sisu"))
        if not raw:
            continue
        kind = text(item.get("liik")).upper()
        if kind == "WWW":
            website = normalize_website(raw)
            if website:
                websites.append(website)
        elif kind in {"TEL", "FAX"}:
            normalized = normalize_phone(raw, default_country_code="372")
            if normalized:
                phones.append(normalized)
        elif kind == "EMAIL":
            email = raw.strip().lower().strip(".;,")
            if "@" not in email:
                continue
            email_domain = email.rsplit("@", 1)[-1].lower()
            if email_domain in GENERIC_EMAIL_DOMAINS:
                continue
            if not is_public_company_domain(email_domain, websites):
                continue
            if not is_role_email(email):
                continue
            seen_email_domains.add(email_domain)
            emails.append(email)

    if websites:
        company_domains = [apex_domain(url) for url in websites if apex_domain(url)]
        # Add role-based mailbox patterns that match company website domains when
        # an explicit website entry is present but no email matched domain checks above.
        for item in items:
            if not isinstance(item, dict) or item.get("lopp_kpv"):
                continue
            if text(item.get("liik")).upper() != "EMAIL":
                continue
            email = text(item.get("sisu")).lower().strip(".;,")
            if "@" not in email:
                continue
            email_domain = email.rsplit("@", 1)[-1].lower()
            if email_domain in GENERIC_EMAIL_DOMAINS:
                continue
            if email_domain in seen_email_domains:
                continue
            if not is_role_email(email):
                continue
            if not any(
                company_domain and (
                    email_domain == company_domain or email_domain.endswith(f".{company_domain}")
                )
                for company_domain in company_domains
            ):
                continue
            emails.append(email)
            seen_email_domains.add(email_domain)

    return dedupe(websites), dedupe(emails), dedupe(phones)


def is_estonian_business_name(name: str) -> bool:
    upper = clean_name(name).upper()
    markers = (
        " OÜ",
        " OU",
        " AS",
        " MTÜ",
        " TÜ",
        " UÜ",
        " SA",
        " FIE",
        "ÜHISTU",
        "OSAÜHING",
        "AKTSIASELTS",
        "TULUNDUSÜHISTU",
        "SIHTASUTUS",
    )
    return any(marker in upper or upper.endswith(marker.strip()) for marker in markers)


def is_public_company_domain(domain: str, websites: list[str]) -> bool:
    company_domains = [apex_domain(url) for url in websites if apex_domain(url)]
    if not company_domains:
        return False
    return any(
        domain == company_domain or domain.endswith(f".{company_domain}")
        for company_domain in company_domains
    )


def normalize_email(value: Any) -> str:
    raw = re.sub(r"\s+", "", text(value)).lower().strip(".;,")
    if not raw or "@" not in raw:
        return ""
    local, domain = raw.rsplit("@", 1)
    if not local or "." not in domain or domain in GENERIC_EMAIL_DOMAINS:
        return ""
    return f"{local}@{domain}"


def is_role_email(email: str) -> bool:
    local = email.split("@", 1)[0].lower()
    role_prefixes = (
        "info",
        "sales",
        "export",
        "commercial",
        "contact",
        "office",
        "admin",
        "orders",
        "order",
        "support",
        "service",
        "customerservice",
        "kundeservice",
        "firmapost",
        "post",
        "mail",
        "hello",
    )
    return local in role_prefixes or any(local.startswith(f"{prefix}.") for prefix in role_prefixes)


def contact_point(value: str, kind: str, source_url: str, confidence: float) -> dict[str, Any]:
    personal = kind.endswith("email") and not is_role_email(value)
    return {
        "value": value,
        "kind": "role_email" if kind.endswith("email") and not personal else kind,
        "source_url": source_url,
        "confidence": confidence,
        "personal": personal,
    }


def normalize_phone(value: Any, *, default_country_code: str = "") -> str:
    raw = re.sub(r"[^0-9+]", "", text(value))
    if not raw:
        return ""
    if raw.startswith("00"):
        raw = f"+{raw[2:]}"
    if raw.startswith("+"):
        return raw
    if default_country_code and raw.startswith(default_country_code):
        return f"+{raw}"
    if default_country_code and 6 <= len(raw) <= 10:
        return f"+{default_country_code}{raw}"
    return f"+{raw}" if len(raw) >= 7 else ""


def fetch_to_temp_file(url: str) -> Path:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(5):
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as temp_file:
                    while True:
                        chunk = response.read(1024 * 1024)
                        if not chunk:
                            break
                        temp_file.write(chunk)
                    return Path(temp_file.name)
        except urllib.error.HTTPError as error:
            if error.code not in {429, 500, 502, 503, 504} or attempt == 4:
                raise
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"failed to fetch {url}")


def company_type_from_code(activity_code: str, section: str) -> str:
    prefix = activity_code[:2]
    if section == "C" or prefix in MANUFACTURING_NACE_PREFIXES:
        return "manufacturer"
    if prefix == "46":
        return "wholesaler"
    if prefix in {"47", "49", "50", "51", "52", "53"}:
        return "distributor"
    return "legal entity"


def activity_label(activity_code: str, fallback: str) -> str:
    if activity_code:
        return f"{fallback} {activity_code}"
    return fallback


def norway_address(value: dict[str, Any]) -> str:
    parts: list[str] = []
    for line in value.get("adresse") or []:
        item = text(line)
        if item:
            parts.append(item)
    for key in ("postnummer", "poststed", "kommune", "land"):
        item = text(value.get(key))
        if item:
            parts.append(item)
    return ", ".join(dedupe(parts))


def normalize_website(value: Any) -> str:
    raw = text(value).strip().rstrip("/")
    if not raw:
        return ""
    if raw.startswith(("http://", "https://")):
        return raw
    return f"https://{raw}"


def apex_domain(url: str) -> str | None:
    try:
        parsed = urllib.parse.urlparse(url)
    except ValueError:
        return None
    host = parsed.netloc.lower().removeprefix("www.")
    return host or None


def clean_name(value: Any) -> str:
    raw = text(value).replace("&quot;", "").replace("&#34;", "")
    raw = raw.replace('"', "")
    raw = re.sub(r"\s+", " ", raw).strip(" ,;:")
    raw = raw.strip("' .&+*-")
    return raw


def text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def evidence(field: str, value: str, source_url: str) -> dict[str, str]:
    return {"field": field, "value": text(value), "source_url": source_url}


def dedupe(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        key = value.strip().casefold()
        if key and key not in seen:
            seen.add(key)
            out.append(value.strip())
    return out


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value).strip("-").lower()
    return slug or f"profile-{abs(hash(value))}"


def now_epoch() -> int:
    return int(time.time())


if __name__ == "__main__":
    raise SystemExit(main())
