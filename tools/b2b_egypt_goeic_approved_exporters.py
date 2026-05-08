#!/usr/bin/env python3
"""Append Egypt GOEIC approved-exporter product records to SaharaIndex.

GOEIC's public PDF carries company names, approved items, HS codes, approved
exporter codes, and approval dates. It is useful product/export evidence, not
a phone/email source.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import tempfile
import time
import unicodedata
import urllib.request
from pathlib import Path
from typing import Any

PDF_URL = "https://www.goeic.gov.eg/upload/online/2026/01/documents/files/en/1758.pdf"
SOURCE_NAME = "GOEIC Approved Exporter System"
SOURCE_URL = "https://www.goeic.gov.eg/en/pages/default/view/id/1758"
USER_AGENT = "SaharaIndexBot/1.0 (+https://saharaindex.com/companies/)"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="data/b2b", help="B2B output directory")
    parser.add_argument("--pdf", default="", help="Local PDF path; downloads the GOEIC PDF when omitted")
    parser.add_argument("--pdf-url", default=PDF_URL)
    parser.add_argument("--limit", type=int, default=0, help="Maximum records to append; 0 means all")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--update-existing", action="store_true")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    profiles_path = out_dir / "company_profiles.jsonl"
    existing_ids = read_existing_ids(profiles_path)

    pdf_path = Path(args.pdf) if args.pdf else download_pdf(args.pdf_url)
    text = pdftotext(pdf_path)
    records = parse_records(text)
    now = int(time.time())
    appended = 0
    skipped = 0

    handle = None if args.dry_run else profiles_path.open("a", encoding="utf-8")
    try:
        for record in records:
            profile = profile_from_record(record, now, args.pdf_url)
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
    finally:
        if handle:
            handle.close()

    report = {
        "source": SOURCE_NAME,
        "source_url": SOURCE_URL,
        "pdf_url": args.pdf_url,
        "parsed": len(records),
        "appended": appended,
        "skipped": skipped,
        "dry_run": args.dry_run,
        "finished_at_epoch": int(time.time()),
    }
    reports_dir = out_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    if not args.dry_run:
        (reports_dir / "goeic_approved_exporters_ingest.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


def download_pdf(url: str) -> Path:
    target = Path(tempfile.gettempdir()) / "goeic-approved-exporters.pdf"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/pdf"})
    with urllib.request.urlopen(request, timeout=60) as response:
        target.write_bytes(response.read())
    return target


def pdftotext(path: Path) -> str:
    output = subprocess.check_output(["pdftotext", "-layout", str(path), "-"], text=True)
    return output


def parse_records(text: str) -> list[dict[str, str]]:
    records: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    pattern = re.compile(r"(?P<hs>\d{6,8})\s+(?P<code>EG/[0-9/]+)\s+(?P<date>[0-9/]+)")
    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        if not line.strip() or "Company" in line and "HS Code" in line:
            continue
        match = pattern.search(line)
        if match:
            if current:
                records.append(finish_record(current))
            before = line[: match.start()]
            company_part = before[:32].strip()
            item_part = before[32:].strip()
            parts = re.split(r"\s{2,}", before.strip())
            if len(parts) >= 2:
                company_part = parts[0]
                item_part = " ".join(parts[1:])
            current = {
                "company_parts": [company_part],
                "item_parts": [item_part],
                "hs_code": match.group("hs"),
                "approved_exporter_code": match.group("code"),
                "approval_date": match.group("date"),
            }
            continue
        if current is None:
            continue
        text_part = line.strip().strip("\u202b\u202c")
        if not text_part or looks_like_header(text_part):
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent >= 28:
            current["item_parts"].append(text_part)
        else:
            current["company_parts"].append(text_part)
    if current:
        records.append(finish_record(current))
    return [record for record in records if record["company_name"] and record["item"]]


def finish_record(record: dict[str, Any]) -> dict[str, str]:
    return {
        "company_name": clean_join(record.get("company_parts", [])),
        "item": clean_join(record.get("item_parts", [])),
        "hs_code": str(record.get("hs_code") or ""),
        "approved_exporter_code": str(record.get("approved_exporter_code") or ""),
        "approval_date": str(record.get("approval_date") or ""),
    }


def profile_from_record(record: dict[str, str], now: int, pdf_url: str) -> dict[str, Any]:
    name = record["company_name"]
    item = record["item"]
    hs_code = record["hs_code"]
    exporter_code = record["approved_exporter_code"]
    approval_date = record["approval_date"]
    category = category_from_hs(hs_code, item)
    profile_id = f"eg-goeic-approved-exporter-{slugify(name)}-{slugify(exporter_code)}".strip("-")
    if not profile_id:
        profile_id = f"eg-goeic-approved-exporter-{slugify(exporter_code)}"
    description = (
        f"{name} is listed in Egypt's GOEIC Approved Exporter System. "
        f"Approved item: {item}. HS code: {hs_code}. Approval date: {approval_date}."
    )
    return {
        "id": profile_id,
        "source_name": SOURCE_NAME,
        "source_url": SOURCE_URL,
        "profile_url": pdf_url,
        "canonical_domain": None,
        "company_name": name,
        "description": description,
        "region": "MENA",
        "country": "Egypt",
        "company_type": "exporter",
        "industries": [category, "Egyptian approved exporters"],
        "specializations": [item, f"HS {hs_code}", "Approved exporter"],
        "products": [
            {
                "name": item,
                "description": f"Approved exporter item under HS code {hs_code}; GOEIC approved exporter code {exporter_code}.",
                "url": pdf_url,
                "category": category,
            }
        ],
        "services": [],
        "images": [],
        "contacts": {"emails": [], "phones": [], "websites": [], "social_links": []},
        "addresses": [],
        "company_size": None,
        "revenue": None,
        "personnel": [],
        "evidence": [
            {"field": "source_basis", "value": "official_public_pdf", "source_url": SOURCE_URL},
            {"field": "approved_exporter_code", "value": exporter_code, "source_url": pdf_url},
            {"field": "hs_code", "value": hs_code, "source_url": pdf_url},
            {"field": "approval_date", "value": approval_date, "source_url": pdf_url},
            {"field": "approved_item", "value": item, "source_url": pdf_url},
        ],
        "validation": {
            "status": "official_product_evidence",
            "score": 64,
            "issues": ["GOEIC PDF does not publish company phone, email, website, or address fields."],
            "compliance_flags": [
                "source_basis:official_public_pdf",
                "company_level_contacts_only",
                "product_evidence_only",
            ],
            "field_coverage": ["company_name", "country", "products", "official_exporter_code", "hs_code"],
        },
        "tags": ["egypt", "mena", "exporter", "goeic", "approved-exporter", "with-products"],
        "scraped_at_epoch": now,
        "refresh_due_epoch": now + 30 * 86400,
    }


def category_from_hs(hs_code: str, item: str) -> str:
    prefix = int((hs_code[:2] or "0")) if hs_code[:2].isdigit() else 0
    lower = item.casefold()
    if 1 <= prefix <= 24:
        return "Food & Beverage"
    if 25 <= prefix <= 27:
        return "Construction Materials"
    if 28 <= prefix <= 38 or "resin" in lower or "poly" in lower:
        return "Chemicals"
    if prefix in {39, 40}:
        return "Chemicals"
    if 50 <= prefix <= 63 or "fabric" in lower or "textile" in lower:
        return "Textiles & Apparel"
    if 72 <= prefix <= 83 or "aluminum" in lower or "steel" in lower:
        return "Construction Materials"
    if 84 <= prefix <= 85:
        return "Industrial Equipment"
    if prefix == 87:
        return "Automotive"
    if prefix in {90, 94, 95, 96}:
        return "Industrial Equipment"
    return "Egyptian export products"


def clean_join(parts: list[str]) -> str:
    text = " ".join(part.strip() for part in parts if part and part.strip())
    text = re.sub(r"\s+", " ", text)
    text = text.replace("\u202b", "").replace("\u202c", "")
    return text.strip(" .,-")


def looks_like_header(value: str) -> bool:
    lower = value.casefold()
    return any(word in lower for word in ["approved", "approval", "company", "exporter", "hs code"]) and len(value) < 40


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
