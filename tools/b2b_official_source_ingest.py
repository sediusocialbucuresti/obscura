#!/usr/bin/env python3
"""Append official/open registry company profiles to the B2B corpus.

The ingesters in this file use sanctioned API endpoints or documented open
data services. They keep only company-level fields and do not store personal
directors, private employee contacts, or scraped directory data.
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
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


USER_AGENT = "SaharaIndexBot/1.0 (+https://saharaindex.com/companies/)"

FRANCE_DOC_URL = "https://www.data.gouv.fr/dataservices/api-recherche-dentreprises/"
FRANCE_API_URL = "https://recherche-entreprises.api.gouv.fr/search"
FRANCE_PROFILE_URL = "https://annuaire-entreprises.data.gouv.fr/entreprise/{siren}"

NORWAY_DOC_URL = "https://data.brreg.no/enhetsregisteret/api/docs/index.html"
NORWAY_API_URL = "https://data.brreg.no/enhetsregisteret/api/enheter"
NORWAY_PROFILE_URL = "https://data.brreg.no/enhetsregisteret/api/enheter/{orgnr}"

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
        default="france,norway",
        help="Comma-separated sources: france,norway",
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
        addresses=[address] if address else [],
        company_size=None,
        evidence_items=evidence_items,
        tags=["official_registry", "open_api", "norway", "bronnoysund"],
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
    addresses: list[str],
    company_size: str | None,
    evidence_items: list[dict[str, str]],
    tags: list[str],
    now: int,
) -> dict[str, Any]:
    score = 20 + 15 + 10 + 5
    coverage = ["company_name", "description", "location", "classification"]
    if websites:
        score += 10
        coverage.append("domain")
    status = "review" if score >= 45 else "low_confidence"
    issues = ["missing_email", "manual_review_recommended"]
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
            "emails": [],
            "phones": [],
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
