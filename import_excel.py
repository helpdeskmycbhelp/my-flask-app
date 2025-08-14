#!/usr/bin/env python3
"""
Excel → MongoDB Importer (robust headers + owner merge + municipality fields)

Requirements:
    pip install pandas openpyxl pymongo
"""

from pathlib import Path
import re
from datetime import datetime
from typing import List, Tuple, Optional

import pandas as pd
from pymongo import MongoClient


# ===========================
# CONFIG — EDIT AS NEEDED
# ===========================
MONGO_URI  = "mongodb+srv://flaskuser:mypassword@cluster0.c971gqv.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME    = "property_db"
COLL_NAME  = "properties"

EXCEL_PATH = "Dubai Marina.xlsx"  # full path or relative path


# ===========================
# HELPERS
# ===========================
def parse_money(val) -> Optional[float]:
    """Return float or None. Handles 'AED 1,200,000', '1,25,000', etc."""
    if val is None:
        return None
    s = str(val).strip()
    if s.lower() in ("", "nan", "null", "none", "-"):
        return None
    # keep only digits, dot, minus
    s = re.sub(r"[^\d\.\-]", "", s)
    parts = s.split(".")
    if len(parts) > 2:
        s = parts[0] + "." + "".join(parts[1:])  # collapse multi-dots
    try:
        return float(s)
    except ValueError:
        return None


def parse_number(val) -> Optional[float | int]:
    """Generic numeric parser for area/beds."""
    if val is None:
        return None
    s = str(val).strip()
    if s.lower() in ("", "nan", "null", "none", "-"):
        return None
    s = re.sub(r"[^\d\.\-]", "", s)
    try:
        f = float(s)
        return int(f) if f.is_integer() else f
    except ValueError:
        return None


def parse_date(val) -> str:
    """Return ISO date 'YYYY-MM-DD' or ''."""
    if not val:
        return ""
    try:
        # dayfirst=True to support 21-10-2023 style
        dt = pd.to_datetime(val, errors="coerce", dayfirst=True)
        if pd.isna(dt):
            return ""
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""


def clean_phone(val: str) -> str:
    """
    Normalize phone: keep + and digits, convert '00' prefix to '+',
    fix UAE 05… → +9715…
    """
    if val is None:
        return ""
    s = str(val).strip()
    if s.lower() in ("", "nan", "null", "none", "-"):
        return ""
    if s.endswith(".0"):  # from excel floats
        s = s[:-2]
    s = re.sub(r"[^\d+]", "", s)
    if not s:
        return ""
    if s.startswith("00"):
        s = "+" + s[2:]
    if s.startswith("05"):  # UAE local mobile -> E.164-ish
        s = "+971" + s[1:]
    return s


def split_contacts(raw: str) -> List[str]:
    """Split a 'Contact' cell that may contain multiple numbers."""
    if not raw:
        return []
    parts = re.split(r"[;,/|&\s]+", str(raw))
    out, seen = [], set()
    for p in parts:
        c = clean_phone(p)
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return out


def pick(row: pd.Series, candidates: List[str]) -> str:
    """
    Return the first non-empty value from row across candidate column names.
    - Case-insensitive exact match first, then partial contains match.
    """
    cols_lower = {str(c).lower(): c for c in row.index}

    # exact (case-insensitive)
    for cand in candidates:
        key = str(cand).lower()
        if key in cols_lower:
            v = row.get(cols_lower[key], "")
            if str(v).strip():
                return str(v).strip()

    # partial contains (case-insensitive)
    for cand in candidates:
        key = str(cand).lower()
        for col_low, original in cols_lower.items():
            if key in col_low:
                v = row.get(original, "")
                if str(v).strip():
                    return str(v).strip()

    return ""


def find_owner_indices(owners: List[dict], owner_name: str, role: str, reg_date: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Return (idx_same_date, idx_same_owner_any_date) where either can be None.
    idx_same_date: owner with same (name, role, registration_date)
    idx_same_owner_any_date: first owner with same (name, role) ignoring date
    """
    reg_date_norm = (reg_date or "")
    idx_same_date = next(
        (i for i, o in enumerate(owners)
         if o.get("owner_name") == owner_name
         and o.get("role") == role
         and (o.get("registration_date") or "") == reg_date_norm),
        None
    )
    idx_same_owner_any_date = next(
        (i for i, o in enumerate(owners)
         if o.get("owner_name") == owner_name
         and o.get("role") == role),
        None
    )
    return idx_same_date, idx_same_owner_any_date


# ===========================
# MAIN
# ===========================
def main():
    in_path = Path(EXCEL_PATH)
    if not in_path.exists():
        raise SystemExit(f"ERROR: Input file not found:\n  {in_path}")

    # ---- Load ALL sheets as text (preserves phone formatting) ----
    excel_book = pd.read_excel(
        in_path,
        sheet_name=None,
        dtype=str,              # keep everything as strings
        keep_default_na=False,  # don't convert "" to NaN
        na_filter=False
    )
    df = pd.concat(excel_book.values(), ignore_index=True)
    total_rows = len(df)

    # ---- DB & indexes ----
    client = MongoClient(MONGO_URI)
    db = client.get_database(DB_NAME)
    collection = db[COLL_NAME]

    # Useful indexes (idempotent)
    collection.create_index([("building_name", 1), ("unit_number", 1)])
    collection.create_index("owners.owner_name")
    collection.create_index("owners.contacts")
    collection.create_index("owners.registration_date")
    collection.create_index("municipality_number")
    collection.create_index("municipality_sub_number")

    # Cache existing to avoid repeated lookups
    existing_cache = {
        (doc["building_name"], doc["unit_number"]): {"_id": doc["_id"], "owners": doc.get("owners", [])}
        for doc in collection.find({}, {"building_name": 1, "unit_number": 1, "owners": 1})
    }

    inserted = updated = 0
    owners_merged_contacts = 0
    owners_added_same_owner_new_date = 0
    owners_added_new_owner = 0

    for _, row in df.iterrows():
        # === Robust field extraction (header variations supported) ===

        # Basic property:
        building = pick(row, [
            "Building", "Building Name", "BuildingName", "BuildingNameEn",
            "Tower", "Tower Name", "Building (EN)"
        ])
        unit_number = pick(row, [
            "Unit No", "Unit no", "Unit Number", "UnitNumber", "Unit_No",
            "Unit-No", "Unit#", "Unit #", "Unit", "unitno", "unitno."
        ])
        area_sqft = parse_number(pick(row, ["Unit Size", "Size", "Area", "Area (sqft)", "Built-up Area"]))
        price_raw = pick(row, ["Price", "ProcedureValue", "Procedure Val", "ProcedureVal", "Value"])
        price = parse_money(price_raw)

        # Classification:
        property_type = (pick(row, ["Property Type", "PropertyType", "PropertyTypeEn"]) or None)
        sub_type      = (pick(row, ["Sub Type", "SubType", "SubTypeNameEn"]) or None)
        beds          = parse_number(pick(row, ["Beds", "Bed", "Bedrooms"]))

        # Location:
        city          = (pick(row, ["City"]) or None)
        community     = (pick(row, ["Community", "Project Lnd", "Project"]) or None)
        sub_community = (pick(row, ["Sub Community", "Sub-Community", "SubCommunity"]) or None)

        # Municipality (NOT land):
        municipality_number     = (pick(row, ["Mun No", "Municipality No", "Municipality Number"]) or None)
        municipality_sub_number = (pick(row, ["Mun Sub No", "Municipality Sub No", "Municipality Sub Number"]) or None)

        # Owner / transaction:
        owner_name = pick(row, ["Name", "NameEn", "Owner Name"])
        role       = pick(row, ["Role", "Owner Type", "ProcedurePartyTypeNameEn"])
        reg_date   = parse_date(pick(row, ["Regis", "Registration Date", "Reg Date"]))
        contacts   = split_contacts(pick(row, ["Contact", "Phone", "Mobile", "Whatsapp", "Tel"]))

        # Skip rows without core identifiers
        if not building or not unit_number or not owner_name:
            continue

        owner_doc = {
            "owner_name": owner_name,
            "role": role,
            "contacts": contacts,
            "registration_date": reg_date
        }

        key = (building, unit_number)
        if key in existing_cache:
            # ----- UPDATE existing property -----
            doc_id = existing_cache[key]["_id"]
            owners = existing_cache[key].get("owners", [])

            # 1) refresh top-level fields when provided
            set_fields = {
                k: v for k, v in {
                    "area_sqft": area_sqft,
                    "price": price,
                    "price_raw": price_raw if price_raw else None,
                    "property_type": property_type,
                    "sub_type": sub_type,
                    "beds": beds,
                    "city": city,
                    "community": community,
                    "sub_community": sub_community,
                    "municipality_number": municipality_number,
                    "municipality_sub_number": municipality_sub_number,
                }.items() if v is not None and v != ""
            }
            if set_fields:
                collection.update_one({"_id": doc_id}, {"$set": set_fields})

            # 2) merge/append owners
            idx_same_date, idx_same_owner_any_date = find_owner_indices(
                owners, owner_name, role, reg_date
            )

            if idx_same_date is not None:
                # Same owner+role+date -> merge contacts only (no duplicate row)
                current = owners[idx_same_date]
                exist_contacts = set(current.get("contacts", []))
                new_nums = [c for c in contacts if c and c not in exist_contacts]

                if new_nums:
                    collection.update_one(
                        {
                            "_id": doc_id,
                            "owners.owner_name": owner_name,
                            "owners.role": role,
                            "owners.registration_date": reg_date or ""
                        },
                        {"$addToSet": {"owners.$.contacts": {"$each": new_nums}}}
                    )
                    # keep cache in sync
                    current.setdefault("contacts", []).extend(new_nums)
                    owners_merged_contacts += 1

            elif idx_same_owner_any_date is not None:
                # Same owner+role, different date -> push new dated entry
                collection.update_one({"_id": doc_id}, {"$push": {"owners": owner_doc}})
                owners.append(owner_doc)
                owners_added_same_owner_new_date += 1

            else:
                # Completely new owner for this property
                collection.update_one({"_id": doc_id}, {"$push": {"owners": owner_doc}})
                owners.append(owner_doc)
                owners_added_new_owner += 1

            updated += 1

        else:
            # ----- INSERT new property -----
            new_doc = {
                "building_name": building,
                "unit_number": unit_number,
                "area_sqft": area_sqft,
                "price": price,
                "price_raw": price_raw if price_raw else None,
                "property_type": property_type,
                "sub_type": sub_type,
                "beds": beds,
                "city": city,
                "community": community,
                "sub_community": sub_community,
                "municipality_number": municipality_number,
                "municipality_sub_number": municipality_sub_number,
                "owners": [owner_doc],
            }
            res = collection.insert_one(new_doc)
            existing_cache[key] = {"_id": res.inserted_id, "owners": [owner_doc]}
            inserted += 1

    # ---- Summary ----
    print("\n=== Import Summary ===")
    print(f"File: {in_path}")
    print(f"Total rows read: {total_rows}")
    print(f"Inserted properties: {inserted}")
    print(f"Updated properties:  {updated}")
    print(f"Owners merged (contacts-only): {owners_merged_contacts}")
    print(f"Owners added (same owner+role, NEW date): {owners_added_same_owner_new_date}")
    print(f"Owners added (new owner):               {owners_added_new_owner}")


if __name__ == "__main__":
    main()
