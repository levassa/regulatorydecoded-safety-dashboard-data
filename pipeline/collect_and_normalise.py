"""
Product Safety Dashboard — Data Collection & Normalisation Pipeline

Fetches product recall and safety alert data from eight public sources,
normalises everything into a shared schema, and writes pre-aggregated
JSON files to /data/ for static serving via jsDelivr CDN.

Sources
-------
Primary (REST/JSON APIs)
  1. EU Safety Gate (RAPEX)  — OpenDataSoft REST API
  2. CPSC                    — SaferProducts.gov REST API
  3. Health Canada           — Open Government JSON feed
  4. RappelConso (France)    — OpenDataSoft REST API
  5. FDA openFDA             — device + food enforcement REST API

Secondary (file downloads)
  6. NHTSA (US vehicles)     — flat-file ZIP download
  7. OPSS (UK)               — GOV.UK search API
  8. ACCC (Australia)        — RSS feed

Shared schema
-------------
  id                 SHA-1 of (source + reference) — deduplication key
  source             human-readable source name
  date               ISO date string YYYY-MM-DD
  region             broad region label
  notifying_country  country that issued the alert
  country_of_origin  manufacturing/origin country
  product_category   normalised product taxonomy  (see CATEGORY_MAP)
  product_desc       free-text product description

  --- Hazard & injury fields ---
  risk_type          legacy field — kept for dashboard backward-compatibility
  hazard_type        ROOT CAUSE mechanism  (see HAZARD_TYPE_MAP)
                     e.g. "Electrical Fault", "Flammability", "Sharp Edges"
  injury_type        INJURY to the person  (see INJURY_TYPE_MAP)
                     e.g. "Burns", "Laceration", "Poisoning"
  injury_description free-text narrative of the incident / injury
  severity           gravity level: Fatal | Serious | Moderate | Minor | Not Reported
  injury_flag        True when at least one injury has been reported
  injury_count       number of reported injuries (int; 0 = unknown or none reported)

  corrective_action  recall / withdrawal / ban description
  reference          source-specific alert identifier

Design intent
-------------
  hazard_type  enables "trend by root cause" analysis
  injury_type  enables "trend by injury category" analysis
  severity     enables "trend by gravity level" analysis
  region +     enables geographic breakdowns
  product_category
  date         enables all time-series views

  The three fields (hazard_type, injury_type, severity) are extracted from
  the same raw text using separate lookup maps, because a single incident
  often contains all three concepts collapsed into one string:
    "fire and burn hazard" → hazard=Flammability, injury=Burns, severity=depends on context
"""

import csv
import hashlib
import io
import json
import logging
import os
import sys
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, date

import pandas as pd
import requests


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
LOG_LEVEL = logging.INFO

SAFETY_GATE_URL = (
    "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
    "healthref-europe-rapex-en@public/records"
)
SAFETY_GATE_PAGE_SIZE = 100
SAFETY_GATE_MAX_RECORDS = 5000

CPSC_URL = "https://www.saferproducts.gov/RestWebServices/Recall"
CPSC_MAX_RECORDS = 2000
CPSC_DATE_START = "2020-01-01"

HEALTH_CANADA_URL = (
    "https://recalls-rappels.canada.ca/sites/default/files/"
    "opendata-donneesouvertes/HCRSAMOpenData.json"
)

RAPPELCONSO_URL = (
    "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/"
    "rappelconso0/records"
)
RAPPELCONSO_PAGE_SIZE = 100
RAPPELCONSO_MAX_RECORDS = 3000

FDA_DEVICE_URL = "https://api.fda.gov/device/enforcement.json"
FDA_FOOD_URL   = "https://api.fda.gov/food/enforcement.json"
FDA_PAGE_SIZE  = 100
FDA_MAX_RECORDS = 2000
FDA_DATE_START  = "20200101"   # YYYYMMDD — openFDA search format

NHTSA_ZIP_URL  = "https://static.nhtsa.gov/odi/ffdd/rcl/FLAT_RCL.zip"
NHTSA_FILENAME = "FLAT_RCL.txt"
NHTSA_MIN_YEAR = 2020

OPSS_URL        = "https://www.gov.uk/api/search.json"
OPSS_PAGE_SIZE  = 100
OPSS_MAX_RECORDS = 1000

ACCC_RSS_URL = "https://www.productsafety.gov.au/rss/recalls"

REQUEST_TIMEOUT = 45


# ---------------------------------------------------------------------------
# Taxonomy maps — Product categories
# ---------------------------------------------------------------------------

CATEGORY_MAP = {
    "cosmetics": "Cosmetics & Personal Care",
    "cosmetic": "Cosmetics & Personal Care",
    "personal care": "Cosmetics & Personal Care",
    "hygiene": "Cosmetics & Personal Care",
    "beauty": "Cosmetics & Personal Care",
    "perfume": "Cosmetics & Personal Care",
    "skincare": "Cosmetics & Personal Care",

    "toys": "Toys & Childcare",
    "toy": "Toys & Childcare",
    "childcare": "Toys & Childcare",
    "child care": "Toys & Childcare",
    "articles for children": "Toys & Childcare",
    "children": "Toys & Childcare",
    "infant": "Toys & Childcare",
    "baby": "Toys & Childcare",

    "electrical": "Electrical & Electronics",
    "electronics": "Electrical & Electronics",
    "electronic": "Electrical & Electronics",
    "lighting": "Electrical & Electronics",
    "batteries": "Electrical & Electronics",
    "battery": "Electrical & Electronics",
    "appliances": "Electrical & Electronics",
    "household appliances": "Electrical & Electronics",
    "charger": "Electrical & Electronics",
    "power": "Electrical & Electronics",
    "laser": "Electrical & Electronics",

    "motor vehicles": "Motor Vehicles",
    "motor vehicle": "Motor Vehicles",
    "vehicles": "Motor Vehicles",
    "vehicle": "Motor Vehicles",
    "automotive": "Motor Vehicles",
    "automobile": "Motor Vehicles",
    "bicycle": "Motor Vehicles",
    "bicycles": "Motor Vehicles",
    "e-bike": "Motor Vehicles",
    "e-scooter": "Motor Vehicles",
    "scooter": "Motor Vehicles",
    "tire": "Motor Vehicles",
    "tyre": "Motor Vehicles",
    "airbag": "Motor Vehicles",
    "seat belt": "Motor Vehicles",
    "child seat": "Motor Vehicles",

    "chemical": "Chemicals",
    "chemicals": "Chemicals",

    "clothing": "Clothing & Textiles",
    "textile": "Clothing & Textiles",
    "textiles": "Clothing & Textiles",
    "garment": "Clothing & Textiles",
    "footwear": "Clothing & Textiles",
    "apparel": "Clothing & Textiles",
    "fashion": "Clothing & Textiles",

    "food": "Food & Food Contact",
    "food contact": "Food & Food Contact",
    "allergen": "Food & Food Contact",
    "dietary": "Food & Food Contact",

    "medical": "Medical Devices",
    "medical device": "Medical Devices",
    "medical devices": "Medical Devices",
    "device": "Medical Devices",

    "furniture": "Furniture & Home",
    "home": "Furniture & Home",
    "home furnishings": "Furniture & Home",
    "bedding": "Furniture & Home",

    "sport": "Sports & Leisure",
    "sports": "Sports & Leisure",
    "leisure": "Sports & Leisure",
    "outdoor": "Sports & Leisure",
    "camping": "Sports & Leisure",
    "fitness": "Sports & Leisure",

    "machinery": "Tools & Machinery",
    "tools": "Tools & Machinery",
    "tool": "Tools & Machinery",
    "equipment": "Tools & Machinery",
}


# ---------------------------------------------------------------------------
# Taxonomy maps — Hazard type (root cause / mechanism)
# ---------------------------------------------------------------------------
# "What went wrong with the product?"

HAZARD_TYPE_MAP = {
    # Electrical
    "electrical fault": "Electrical Fault",
    "electric shock": "Electrical Fault",
    "electrocution": "Electrical Fault",
    "short circuit": "Electrical Fault",
    "electrical": "Electrical Fault",
    "wiring": "Electrical Fault",
    "insulation": "Electrical Fault",
    "voltage": "Electrical Fault",
    "overload": "Electrical Fault",

    # Flammability / overheating
    "fire": "Flammability / Overheating",
    "flammab": "Flammability / Overheating",
    "overheating": "Flammability / Overheating",
    "overheat": "Flammability / Overheating",
    "ignit": "Flammability / Overheating",
    "combustion": "Flammability / Overheating",
    "smoke": "Flammability / Overheating",
    "thermal runaway": "Flammability / Overheating",

    # Explosion
    "explosion": "Explosion",
    "explosive": "Explosion",
    "burst": "Explosion",
    "pressur": "Explosion",

    # Sharp edges / points
    "sharp edge": "Sharp Edges / Points",
    "sharp point": "Sharp Edges / Points",
    "sharp": "Sharp Edges / Points",
    "laceration": "Sharp Edges / Points",
    "puncture": "Sharp Edges / Points",
    "blade": "Sharp Edges / Points",

    # Mechanical failure
    "mechanical failure": "Mechanical Failure",
    "structural failure": "Mechanical Failure",
    "collapse": "Mechanical Failure",
    "breakage": "Mechanical Failure",
    "fracture": "Mechanical Failure",
    "defect": "Mechanical Failure",
    "malfunction": "Mechanical Failure",
    "detach": "Mechanical Failure",
    "loose part": "Mechanical Failure",

    # Small parts / choking
    "small part": "Small Parts / Choking Hazard",
    "choking": "Small Parts / Choking Hazard",
    "suffocation": "Small Parts / Choking Hazard",
    "strangulation": "Small Parts / Choking Hazard",
    "asphyxia": "Small Parts / Choking Hazard",

    # Chemical / toxic substance
    "chemical": "Chemical / Toxic Substance",
    "toxic": "Chemical / Toxic Substance",
    "hazardous substance": "Chemical / Toxic Substance",
    "carcinogen": "Chemical / Toxic Substance",
    "heavy metal": "Chemical / Toxic Substance",
    "lead": "Chemical / Toxic Substance",
    "cadmium": "Chemical / Toxic Substance",
    "phthalate": "Chemical / Toxic Substance",
    "bpa": "Chemical / Toxic Substance",
    "formaldehyde": "Chemical / Toxic Substance",

    # Microbiological contamination
    "microbiological": "Microbiological Contamination",
    "bacteria": "Microbiological Contamination",
    "listeria": "Microbiological Contamination",
    "salmonella": "Microbiological Contamination",
    "e. coli": "Microbiological Contamination",
    "mould": "Microbiological Contamination",
    "mold": "Microbiological Contamination",
    "pathogen": "Microbiological Contamination",

    # Allergen
    "allergen": "Allergen",
    "allergy": "Allergen",
    "anaphyla": "Allergen",
    "undeclared": "Allergen",  # undeclared allergen — common in food recalls

    # Instability / fall risk
    "instability": "Instability / Fall Risk",
    "tip-over": "Instability / Fall Risk",
    "tip over": "Instability / Fall Risk",
    "topple": "Instability / Fall Risk",
    "fall": "Instability / Fall Risk",

    # Radiation
    "radiation": "Radiation",
    "uv": "Radiation",
    "laser": "Radiation",
    "radioactive": "Radiation",

    # Drowning
    "drowning": "Drowning Risk",
    "water": "Drowning Risk",
    "submersion": "Drowning Risk",

    # Entrapment
    "entrapment": "Entrapment",
    "entrap": "Entrapment",
    "pinch": "Entrapment",
    "crush": "Entrapment",
    "trap": "Entrapment",
}


# ---------------------------------------------------------------------------
# Taxonomy maps — Injury type (what happens to the person)
# ---------------------------------------------------------------------------
# "What injury does the person sustain?"

INJURY_TYPE_MAP = {
    # Burns
    "burn": "Burns",
    "burns": "Burns",
    "scald": "Burns",
    "thermal injury": "Burns",
    "skin burn": "Burns",

    # Electric shock
    "electric shock": "Electric Shock",
    "electrocution": "Electric Shock",
    "shock": "Electric Shock",

    # Laceration / cuts
    "laceration": "Laceration / Cut",
    "cut": "Laceration / Cut",
    "slash": "Laceration / Cut",
    "abrasion": "Laceration / Cut",
    "wound": "Laceration / Cut",

    # Fracture / blunt trauma
    "fracture": "Fracture / Blunt Trauma",
    "broken bone": "Fracture / Blunt Trauma",
    "broken": "Fracture / Blunt Trauma",
    "contusion": "Fracture / Blunt Trauma",
    "bruise": "Fracture / Blunt Trauma",
    "impact": "Fracture / Blunt Trauma",
    "blunt": "Fracture / Blunt Trauma",

    # Choking / suffocation
    "choking": "Choking / Suffocation",
    "choke": "Choking / Suffocation",
    "suffocation": "Choking / Suffocation",
    "suffocate": "Choking / Suffocation",
    "strangulation": "Choking / Suffocation",
    "asphyxia": "Choking / Suffocation",
    "asphyxiation": "Choking / Suffocation",

    # Poisoning / ingestion
    "poisoning": "Poisoning / Ingestion",
    "poison": "Poisoning / Ingestion",
    "ingestion": "Poisoning / Ingestion",
    "toxic": "Poisoning / Ingestion",
    "intoxication": "Poisoning / Ingestion",
    "overdose": "Poisoning / Ingestion",

    # Allergic reaction
    "allerg": "Allergic Reaction",
    "anaphyla": "Allergic Reaction",

    # Drowning
    "drowning": "Drowning",
    "drown": "Drowning",

    # Eye injury
    "eye injury": "Eye Injury",
    "eye": "Eye Injury",
    "vision": "Eye Injury",
    "blindness": "Eye Injury",

    # Skin irritation / dermatitis
    "skin irritation": "Skin Irritation",
    "dermatitis": "Skin Irritation",
    "rash": "Skin Irritation",
    "irritation": "Skin Irritation",

    # Respiratory
    "respiratory": "Respiratory",
    "inhalation": "Respiratory",
    "lung": "Respiratory",
    "breathing": "Respiratory",
    "asthma": "Respiratory",

    # Fall injury
    "fall": "Fall Injury",
    "tip-over": "Fall Injury",
    "tip over": "Fall Injury",

    # Entrapment
    "entrapment": "Entrapment",
    "entrap": "Entrapment",
    "crush": "Entrapment",
    "pinch": "Entrapment",

    # Radiation exposure
    "radiation": "Radiation Exposure",
    "uv exposure": "Radiation Exposure",
    "laser": "Radiation Exposure",
}


# ---------------------------------------------------------------------------
# Taxonomy maps — Severity / gravity level
# ---------------------------------------------------------------------------
# Order matters: Fatal checked first, then descending gravity.

SEVERITY_FATAL_KEYWORDS = [
    "fatal", "death", "fatality", "fatalities", "died", "killed",
    "kill", "deadly", "lethal",
]

SEVERITY_SERIOUS_KEYWORDS = [
    "hospitaliz", "hospitalis", "hospitalization", "surgery", "surgic",
    "serious injur", "severe injur", "permanent", "disabilit",
    "life-threatening", "life threatening", "critical", "intensive care",
    "icu", "emergency room", "emergency department", "amputation",
    "class i",       # FDA Class I = most serious
    "danger 1",      # Health Canada Danger 1 = most serious
    "class 1",
]

SEVERITY_MODERATE_KEYWORDS = [
    "moderate", "medical treatment", "medical attention", "doctor",
    "physician", "treatment required", "injured", "injury reported",
    "class ii", "class 2",
    "danger 2",
]

SEVERITY_MINOR_KEYWORDS = [
    "minor", "slight", "low risk", "mild", "superficial",
    "no injury", "no injuries", "potential", "risk of",
    "class iii", "class 3",
    "warning",       # Health Canada Warning = least serious
    "caution",
]


# ---------------------------------------------------------------------------
# Legacy risk_type map (kept for backward-compatibility with dashboard JS)
# ---------------------------------------------------------------------------

RISK_MAP = {
    "chemical": "Chemical",
    "fire": "Fire",
    "fire hazard": "Fire",
    "flammab": "Fire",
    "burns": "Burns",
    "burn": "Burns",
    "electric shock": "Electric Shock",
    "electrical": "Electric Shock",
    "electrocution": "Electric Shock",
    "injury": "Injury",
    "laceration": "Injury",
    "choking": "Choking",
    "suffocation": "Choking",
    "strangulation": "Choking",
    "explosion": "Explosion",
    "environmental": "Environmental",
    "contamination": "Contamination",
    "microbiological": "Contamination",
    "drowning": "Drowning",
    "fall": "Falls",
    "radiation": "Radiation",
    "allergen": "Contamination",
    "danger 1": "High Risk",
    "danger 2": "Moderate Risk",
    "warning": "Low Risk",
}


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Utilities — IDs and dates
# ---------------------------------------------------------------------------

def make_id(source: str, reference: str) -> str:
    raw = f"{source}::{reference}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()


def safe_date(value) -> str:
    """Return an ISO date string YYYY-MM-DD or empty string."""
    if not value:
        return ""
    if isinstance(value, (datetime, date)):
        return value.strftime("%Y-%m-%d")
    s = str(value).strip()
    for fmt in (
        "%Y-%m-%d",
        "%Y%m%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%B %d, %Y",
        "%d %B %Y",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
    ):
        try:
            return datetime.strptime(s[:len(fmt)], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return s[:10] if len(s) >= 10 else ""


def get_json(url: str, params: dict = None) -> dict:
    r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# Utilities — Normalisation functions
# ---------------------------------------------------------------------------

def normalise_category(raw: str) -> str:
    if not raw:
        return "Other"
    lower = raw.lower().strip()
    for key, value in CATEGORY_MAP.items():
        if key in lower:
            return value
    return raw.title()


def normalise_risk(raw: str) -> str:
    """Legacy risk_type — kept for backward compatibility."""
    if not raw:
        return "Other"
    lower = raw.lower().strip()
    for key, value in RISK_MAP.items():
        if key in lower:
            return value
    return raw.title()


def normalise_hazard_type(text: str) -> str:
    """
    Extract the root cause / mechanism from a free-text hazard description.
    Returns the most specific match found, or 'Other' if none.
    """
    if not text:
        return "Other"
    lower = text.lower()
    # Longer / more specific keys are checked first via sorted length (desc)
    for key in sorted(HAZARD_TYPE_MAP, key=len, reverse=True):
        if key in lower:
            return HAZARD_TYPE_MAP[key]
    return "Other"


def normalise_injury_type(text: str) -> str:
    """
    Extract the injury category from a free-text description.
    Returns the most specific match found, or 'Not Specified' if none.
    """
    if not text:
        return "Not Specified"
    lower = text.lower()
    for key in sorted(INJURY_TYPE_MAP, key=len, reverse=True):
        if key in lower:
            return INJURY_TYPE_MAP[key]
    return "Not Specified"


def extract_severity(
    text: str,
    fda_classification: str = "",
    hc_hazard_class: str = "",
    has_injuries: bool = False,
) -> str:
    """
    Determine gravity level from multiple signals:
      1. Keyword scan of the free-text description
      2. FDA classification (Class I / II / III)
      3. Health Canada hazard classification (Danger 1 / 2 / Warning)
      4. Presence of any injury reports as a floor

    Returns one of: Fatal | Serious | Moderate | Minor | Not Reported
    """
    combined = f"{text} {fda_classification} {hc_hazard_class}".lower()

    for kw in SEVERITY_FATAL_KEYWORDS:
        if kw in combined:
            return "Fatal"

    for kw in SEVERITY_SERIOUS_KEYWORDS:
        if kw in combined:
            return "Serious"

    for kw in SEVERITY_MODERATE_KEYWORDS:
        if kw in combined:
            return "Moderate"

    for kw in SEVERITY_MINOR_KEYWORDS:
        if kw in combined:
            return "Minor"

    # If injuries are reported but we couldn't determine level, call it Moderate
    if has_injuries:
        return "Moderate"

    return "Not Reported"


# ---------------------------------------------------------------------------
# Source 1 — EU Safety Gate (OpenDataSoft mirror)
# ---------------------------------------------------------------------------

def fetch_safety_gate() -> pd.DataFrame:
    log.info("Fetching EU Safety Gate data from OpenDataSoft...")
    records, offset = [], 0
    while offset < SAFETY_GATE_MAX_RECORDS:
        params = {"limit": SAFETY_GATE_PAGE_SIZE, "offset": offset, "order_by": "date desc"}
        try:
            payload = get_json(SAFETY_GATE_URL, params)
        except Exception as exc:
            log.warning("EU Safety Gate error at offset %d: %s", offset, exc)
            break
        batch = payload.get("results", [])
        if not batch:
            break
        records.extend(batch)
        log.info("  EU Safety Gate: %d fetched (total: %d)", len(batch), len(records))
        if len(batch) < SAFETY_GATE_PAGE_SIZE:
            break
        offset += SAFETY_GATE_PAGE_SIZE
    log.info("EU Safety Gate: %d raw records.", len(records))
    return pd.DataFrame(records)


def normalise_safety_gate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    rows = []
    for _, r in df.iterrows():
        ref = str(r.get("reference", r.get("alert_number", ""))).strip()
        # EU Safety Gate has separate risk_type (short) and risk (long description)
        risk_short = str(r.get("risk_type", r.get("risk", ""))).strip()
        risk_long  = str(r.get("risk", risk_short)).strip()
        injury_desc = risk_long  # the long risk description is the injury narrative

        rows.append({
            "id":                make_id("EU Safety Gate", ref),
            "source":            "EU Safety Gate",
            "date":              safe_date(r.get("date", r.get("publication_date", ""))),
            "region":            "EU / EEA",
            "notifying_country": str(r.get("notifying_country", "")).strip(),
            "country_of_origin": str(r.get("country_of_origin", "")).strip(),
            "product_category":  normalise_category(str(r.get("product_category", ""))),
            "product_desc":      str(r.get("product_description", r.get("product_name", ""))).strip(),
            "risk_type":         normalise_risk(risk_short),
            "hazard_type":       normalise_hazard_type(risk_long),
            "injury_type":       normalise_injury_type(risk_long),
            "injury_description": injury_desc[:500],
            "severity":          extract_severity(risk_long),
            "injury_flag":       False,
            "injury_count":      0,
            "corrective_action": str(r.get("measures_taken", r.get("corrective_action", ""))).strip(),
            "reference":         ref,
        })
    out = pd.DataFrame(rows)
    log.info("EU Safety Gate: %d normalised.", len(out))
    return out


# ---------------------------------------------------------------------------
# Source 2 — CPSC (SaferProducts.gov)
# ---------------------------------------------------------------------------

def fetch_cpsc() -> pd.DataFrame:
    log.info("Fetching CPSC recall data...")
    params = {"format": "json", "RecallDateStart": CPSC_DATE_START, "Limit": CPSC_MAX_RECORDS}
    try:
        data = get_json(CPSC_URL, params)
    except Exception as exc:
        log.warning("CPSC fetch error: %s", exc)
        return pd.DataFrame()
    if isinstance(data, list):
        records = data
    elif isinstance(data, dict):
        records = data.get("Recalls", data.get("recalls", []))
    else:
        records = []
    log.info("CPSC: %d raw records.", len(records))
    return pd.DataFrame(records)


def normalise_cpsc(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    rows = []
    for _, r in df.iterrows():
        ref = str(r.get("RecallNumber", r.get("recall_number", ""))).strip()

        products = r.get("Products", [])
        product_desc = (
            products[0].get("Name", products[0].get("Description", ""))
            if products and isinstance(products, list)
            else str(r.get("ProductName", "")).strip()
        )

        hazards = r.get("Hazards", [])
        risk_raw = (
            hazards[0].get("Name", hazards[0].get("Description", ""))
            if hazards and isinstance(hazards, list)
            else str(r.get("Hazard", "")).strip()
        )

        categories = r.get("ProductTypes", [])
        cat_raw = (
            categories[0].get("Name", "")
            if categories and isinstance(categories, list)
            else str(r.get("ProductType", "")).strip()
        )

        remedy = r.get("Remedies", [])
        action = (
            remedy[0].get("Name", "")
            if remedy and isinstance(remedy, list)
            else str(r.get("Remedy", "")).strip()
        )

        injuries = r.get("Injuries", [])
        has_injuries = bool(injuries and isinstance(injuries, list) and len(injuries) > 0)
        injury_count = 0
        injury_desc_parts = []
        if has_injuries:
            for inj in injuries:
                n = inj.get("NumInjuries", inj.get("Count", 0))
                try:
                    injury_count += int(n)
                except (ValueError, TypeError):
                    pass
                desc = inj.get("Description", inj.get("Name", ""))
                if desc:
                    injury_desc_parts.append(str(desc))

        injury_desc = "; ".join(injury_desc_parts) if injury_desc_parts else risk_raw
        # Combine hazard name and injury description for classification
        full_text = f"{risk_raw} {injury_desc}"

        rows.append({
            "id":                make_id("CPSC", ref),
            "source":            "CPSC",
            "date":              safe_date(r.get("RecallDate", r.get("recall_date", ""))),
            "region":            "USA",
            "notifying_country": "United States",
            "country_of_origin": str(r.get("ManufacturerCountry", "")).strip(),
            "product_category":  normalise_category(cat_raw),
            "product_desc":      str(product_desc).strip(),
            "risk_type":         normalise_risk(risk_raw),
            "hazard_type":       normalise_hazard_type(full_text),
            "injury_type":       normalise_injury_type(full_text),
            "injury_description": injury_desc[:500],
            "severity":          extract_severity(full_text, has_injuries=has_injuries),
            "injury_flag":       has_injuries,
            "injury_count":      injury_count,
            "corrective_action": action,
            "reference":         ref,
        })
    out = pd.DataFrame(rows)
    log.info("CPSC: %d normalised.", len(out))
    return out


# ---------------------------------------------------------------------------
# Source 3 — Health Canada
# ---------------------------------------------------------------------------

def fetch_health_canada() -> pd.DataFrame:
    log.info("Fetching Health Canada recall data...")
    try:
        data = get_json(HEALTH_CANADA_URL)
    except Exception as exc:
        log.warning("Health Canada fetch error: %s", exc)
        return pd.DataFrame()
    if isinstance(data, list):
        records = data
    elif isinstance(data, dict):
        records = data.get("results", data.get("data", []))
    else:
        records = []
    log.info("Health Canada: %d raw records.", len(records))
    return pd.DataFrame(records)


def normalise_health_canada(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    if "recallType" in df.columns:
        mask = df["recallType"].astype(str).str.lower().str.contains("consumer|product", na=False)
        df = df[mask].copy()
        log.info("Health Canada: %d after consumer-product filter.", len(df))
    rows = []
    for _, r in df.iterrows():
        ref = str(r.get("recallId", r.get("recall_id", r.get("id", "")))).strip()
        hc_class = str(r.get("hazardClassification", r.get("hazard", ""))).strip()
        desc      = str(r.get("description", r.get("summary", ""))).strip()
        # Combine classification and description for richer analysis
        full_text = f"{hc_class} {desc}"

        rows.append({
            "id":                make_id("Health Canada", ref),
            "source":            "Health Canada",
            "date":              safe_date(r.get("datePublished", r.get("recallDate", r.get("date", "")))),
            "region":            "Canada",
            "notifying_country": "Canada",
            "country_of_origin": str(r.get("countryOfOrigin", "")).strip(),
            "product_category":  normalise_category(str(r.get("recallCategory", r.get("category", "")))),
            "product_desc":      str(r.get("title", r.get("productName", ""))).strip(),
            "risk_type":         normalise_risk(hc_class),
            "hazard_type":       normalise_hazard_type(full_text),
            "injury_type":       normalise_injury_type(full_text),
            "injury_description": desc[:500],
            "severity":          extract_severity(full_text, hc_hazard_class=hc_class),
            "injury_flag":       False,
            "injury_count":      0,
            "corrective_action": str(r.get("corrective_action", r.get("action", ""))).strip(),
            "reference":         ref,
        })
    out = pd.DataFrame(rows)
    log.info("Health Canada: %d normalised.", len(out))
    return out


# ---------------------------------------------------------------------------
# Source 4 — RappelConso (France)
# ---------------------------------------------------------------------------

def fetch_rappelconso() -> pd.DataFrame:
    log.info("Fetching RappelConso (France) from OpenDataSoft...")
    records, offset = [], 0
    while offset < RAPPELCONSO_MAX_RECORDS:
        params = {
            "limit": RAPPELCONSO_PAGE_SIZE,
            "offset": offset,
            "order_by": "date_de_publication desc",
        }
        try:
            payload = get_json(RAPPELCONSO_URL, params)
        except Exception as exc:
            log.warning("RappelConso error at offset %d: %s", offset, exc)
            break
        batch = payload.get("results", [])
        if not batch:
            break
        records.extend(batch)
        log.info("  RappelConso: %d fetched (total: %d)", len(batch), len(records))
        if len(batch) < RAPPELCONSO_PAGE_SIZE:
            break
        offset += RAPPELCONSO_PAGE_SIZE
    log.info("RappelConso: %d raw records.", len(records))
    return pd.DataFrame(records)


def normalise_rappelconso(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    rows = []
    for _, r in df.iterrows():
        ref  = str(r.get("reference_fiche", r.get("numero_de_version", ""))).strip()
        brand = str(r.get("nom_de_la_marque_du_produit", "")).strip()
        name  = str(r.get("noms_des_modeles_ou_references", r.get("produits_ou_sous_categories", ""))).strip()
        product_desc = f"{brand} — {name}".strip(" —") if (brand or name) else ""

        # Risk fields
        risk_short = str(r.get("risques_encourus_par_le_consommateur", "")).strip()
        risk_long  = str(r.get("description_complementaire_du_risque", risk_short)).strip()
        full_text  = f"{risk_short} {risk_long}"

        rows.append({
            "id":                make_id("RappelConso", ref),
            "source":            "RappelConso",
            "date":              safe_date(r.get("date_de_publication", "")),
            "region":            "France",
            "notifying_country": "France",
            "country_of_origin": str(r.get("pays_fabricant", r.get("pays_de_fabrication", ""))).strip(),
            "product_category":  normalise_category(
                str(r.get("categorie_de_produit", r.get("sous_categorie_de_produit", "")))
            ),
            "product_desc":      product_desc,
            "risk_type":         normalise_risk(risk_short),
            "hazard_type":       normalise_hazard_type(full_text),
            "injury_type":       normalise_injury_type(full_text),
            "injury_description": risk_long[:500],
            "severity":          extract_severity(full_text),
            "injury_flag":       False,
            "injury_count":      0,
            "corrective_action": str(r.get("conduites_a_tenir_par_le_consommateur", "")).strip(),
            "reference":         ref,
        })
    out = pd.DataFrame(rows)
    log.info("RappelConso: %d normalised.", len(out))
    return out


# ---------------------------------------------------------------------------
# Source 5 — FDA openFDA (device + food enforcement)
# ---------------------------------------------------------------------------

def _fetch_fda_endpoint(url: str, label: str) -> list:
    records, skip = [], 0
    search = f"report_date:[{FDA_DATE_START}+TO+29991231]"
    while skip < FDA_MAX_RECORDS:
        try:
            payload = get_json(url, {"search": search, "limit": FDA_PAGE_SIZE, "skip": skip})
        except Exception as exc:
            log.warning("FDA %s error at skip=%d: %s", label, skip, exc)
            break
        batch = payload.get("results", [])
        if not batch:
            break
        records.extend(batch)
        log.info("  FDA %s: %d fetched (total: %d)", label, len(batch), len(records))
        total = payload.get("meta", {}).get("results", {}).get("total", 0)
        if len(records) >= total or len(batch) < FDA_PAGE_SIZE:
            break
        skip += FDA_PAGE_SIZE
    return records


def fetch_fda() -> pd.DataFrame:
    log.info("Fetching FDA openFDA enforcement data...")
    records = _fetch_fda_endpoint(FDA_DEVICE_URL, "device") + \
              _fetch_fda_endpoint(FDA_FOOD_URL,   "food")
    log.info("FDA openFDA: %d total raw records.", len(records))
    return pd.DataFrame(records)


def normalise_fda(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    # FDA Class I = most dangerous (serious risk of injury/death)
    # FDA Class II = may cause temporary adverse effects
    # FDA Class III = unlikely to cause adverse effects
    fda_severity = {"Class I": "Serious", "Class II": "Moderate", "Class III": "Minor"}

    rows = []
    for _, r in df.iterrows():
        ref          = str(r.get("recall_number", r.get("event_id", ""))).strip()
        fda_class    = str(r.get("classification", "")).strip()
        reason       = str(r.get("reason_for_recall", "")).strip()
        product_desc = str(r.get("product_description", "")).strip()
        full_text    = f"{fda_class} {reason} {product_desc}"

        rows.append({
            "id":                make_id("FDA", ref),
            "source":            "FDA",
            "date":              safe_date(str(r.get("report_date", ""))),
            "region":            "USA",
            "notifying_country": "United States",
            "country_of_origin": str(r.get("country", "")).strip(),
            "product_category":  normalise_category(
                str(r.get("product_type", product_desc))
            ),
            "product_desc":      product_desc[:300],
            "risk_type":         normalise_risk(reason),
            "hazard_type":       normalise_hazard_type(full_text),
            "injury_type":       normalise_injury_type(full_text),
            "injury_description": reason[:500],
            "severity":          extract_severity(full_text, fda_classification=fda_class),
            "injury_flag":       False,
            "injury_count":      0,
            "corrective_action": str(r.get("action", r.get("voluntary_mandated", ""))).strip(),
            "reference":         ref,
        })
    out = pd.DataFrame(rows)
    log.info("FDA openFDA: %d normalised.", len(out))
    return out


# ---------------------------------------------------------------------------
# Source 6 — NHTSA Vehicle Recalls (flat-file ZIP)
# ---------------------------------------------------------------------------

def fetch_nhtsa() -> pd.DataFrame:
    """
    Downloads FLAT_RCL.zip from NHTSA and returns records from NHTSA_MIN_YEAR+.
    Flat file columns (pipe-delimited):
      CAMPNO | MAKETXT | MODELTXT | YEARTXT | MFGCAMPNO | COMPNAME |
      MFGNAME | BGMAN | ENDMAN | RPNO | FMVSS |
      DESC_DEFECT | CONEQUENCE_DEFECT | CORRECTIVE_ACTION | NOTES | ...
    """
    log.info("Downloading NHTSA recall flat file...")
    try:
        response = requests.get(NHTSA_ZIP_URL, timeout=120, stream=True)
        response.raise_for_status()
        zip_bytes = io.BytesIO(response.content)
    except Exception as exc:
        log.warning("NHTSA ZIP download failed: %s", exc)
        return pd.DataFrame()

    try:
        with zipfile.ZipFile(zip_bytes) as zf:
            names  = zf.namelist()
            target = NHTSA_FILENAME if NHTSA_FILENAME in names else names[0]
            with zf.open(target) as fh:
                content = fh.read().decode("latin-1", errors="replace")
    except Exception as exc:
        log.warning("NHTSA ZIP extraction failed: %s", exc)
        return pd.DataFrame()

    reader  = csv.DictReader(io.StringIO(content), delimiter="|")
    records = []
    min_yr  = str(NHTSA_MIN_YEAR)

    for row in reader:
        campno = row.get("CAMPNO", "")
        bgman  = row.get("BGMAN", "").strip()
        if bgman and len(bgman) == 8 and bgman.isdigit():
            if bgman[:4] < min_yr:
                continue
        else:
            try:
                if int("20" + campno[:2]) < NHTSA_MIN_YEAR:
                    continue
            except (ValueError, IndexError):
                pass
        records.append(dict(row))

    log.info("NHTSA: %d records after year >= %d filter.", len(records), NHTSA_MIN_YEAR)
    return pd.DataFrame(records)


def normalise_nhtsa(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    rows = []
    for _, r in df.iterrows():
        ref   = str(r.get("CAMPNO", "")).strip()
        make  = str(r.get("MAKETXT", "")).strip().title()
        model = str(r.get("MODELTXT", "")).strip().title()
        year  = str(r.get("YEARTXT", "")).strip()
        # NHTSA provides both defect description and consequence — combine them
        defect      = str(r.get("DESC_DEFECT", "")).strip()
        consequence = str(r.get("CONEQUENCE_DEFECT", "")).strip()
        full_text   = f"{defect} {consequence}"

        rows.append({
            "id":                make_id("NHTSA", ref),
            "source":            "NHTSA",
            "date":              safe_date(str(r.get("BGMAN", "")).strip()),
            "region":            "USA",
            "notifying_country": "United States",
            "country_of_origin": str(r.get("MFGNAME", "")).strip(),
            "product_category":  "Motor Vehicles",
            "product_desc":      f"{year} {make} {model}".strip(),
            "risk_type":         normalise_risk(full_text),
            "hazard_type":       normalise_hazard_type(defect),
            "injury_type":       normalise_injury_type(consequence),
            "injury_description": consequence[:500],
            "severity":          extract_severity(full_text),
            "injury_flag":       any(kw in full_text.lower() for kw in ["injur", "death", "fatal", "crash"]),
            "injury_count":      0,
            "corrective_action": str(r.get("CORRECTIVE_ACTION", "")).strip(),
            "reference":         ref,
        })
    out = pd.DataFrame(rows)
    log.info("NHTSA: %d normalised.", len(out))
    return out


# ---------------------------------------------------------------------------
# Source 7 — OPSS UK (GOV.UK search API)
# ---------------------------------------------------------------------------

def fetch_opss() -> pd.DataFrame:
    log.info("Fetching OPSS UK from GOV.UK search API...")
    records, start = [], 0
    while start < OPSS_MAX_RECORDS:
        params = {
            "filter_content_store_document_type": "product_safety_alert_report_recall",
            "count": OPSS_PAGE_SIZE,
            "start": start,
            "order": "-public_timestamp",
        }
        try:
            payload = get_json(OPSS_URL, params)
        except Exception as exc:
            log.warning("OPSS UK error at start=%d: %s", start, exc)
            break
        batch = payload.get("results", [])
        if not batch:
            break
        records.extend(batch)
        log.info("  OPSS UK: %d fetched (total: %d)", len(batch), len(records))
        total = payload.get("total", 0)
        if len(records) >= total or len(batch) < OPSS_PAGE_SIZE:
            break
        start += OPSS_PAGE_SIZE
    log.info("OPSS UK: %d raw records.", len(records))
    return pd.DataFrame(records)


def normalise_opss(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    rows = []
    for _, r in df.iterrows():
        link = str(r.get("link", "")).strip()
        ref  = link.split("/")[-1] if link else str(r.get("_id", "")).strip()
        desc = str(r.get("description", r.get("summary", ""))).strip()

        rows.append({
            "id":                make_id("OPSS", ref),
            "source":            "OPSS",
            "date":              safe_date(r.get("public_timestamp", "")),
            "region":            "United Kingdom",
            "notifying_country": "United Kingdom",
            "country_of_origin": "",
            "product_category":  normalise_category(
                str((r.get("filter_topics") or [""])[0])
            ),
            "product_desc":      str(r.get("title", "")).strip(),
            "risk_type":         normalise_risk(desc),
            "hazard_type":       normalise_hazard_type(desc),
            "injury_type":       normalise_injury_type(desc),
            "injury_description": desc[:500],
            "severity":          extract_severity(desc),
            "injury_flag":       False,
            "injury_count":      0,
            "corrective_action": "",
            "reference":         ref,
        })
    out = pd.DataFrame(rows)
    log.info("OPSS UK: %d normalised.", len(out))
    return out


# ---------------------------------------------------------------------------
# Source 8 — ACCC Australia (RSS feed)
# ---------------------------------------------------------------------------

def fetch_accc() -> pd.DataFrame:
    log.info("Fetching ACCC Australia recall RSS feed...")
    try:
        response = requests.get(ACCC_RSS_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        root = ET.fromstring(response.content)
    except Exception as exc:
        log.warning("ACCC RSS fetch/parse failed: %s", exc)
        return pd.DataFrame()
    items = root.findall(".//item")
    records = [
        {
            "title":       (i.findtext("title") or "").strip(),
            "link":        (i.findtext("link") or "").strip(),
            "pubDate":     (i.findtext("pubDate") or "").strip(),
            "description": (i.findtext("description") or "").strip(),
            "category":    (i.findtext("category") or "").strip(),
        }
        for i in items
    ]
    log.info("ACCC Australia: %d raw records from RSS.", len(records))
    return pd.DataFrame(records)


def normalise_accc(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    rows = []
    for _, r in df.iterrows():
        link = str(r.get("link", "")).strip()
        ref  = link.split("/")[-1].rstrip("/") if link else str(r.get("title", ""))[:60]
        desc = str(r.get("description", "")).strip()

        rows.append({
            "id":                make_id("ACCC", ref),
            "source":            "ACCC",
            "date":              safe_date(r.get("pubDate", "")),
            "region":            "Australia",
            "notifying_country": "Australia",
            "country_of_origin": "",
            "product_category":  normalise_category(str(r.get("category", ""))),
            "product_desc":      str(r.get("title", "")).strip(),
            "risk_type":         normalise_risk(desc),
            "hazard_type":       normalise_hazard_type(desc),
            "injury_type":       normalise_injury_type(desc),
            "injury_description": desc[:500],
            "severity":          extract_severity(desc),
            "injury_flag":       False,
            "injury_count":      0,
            "corrective_action": "",
            "reference":         ref,
        })
    out = pd.DataFrame(rows)
    log.info("ACCC Australia: %d normalised.", len(out))
    return out


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates(subset=["id"])
    after  = len(df)
    if before != after:
        log.info("Deduplication: removed %d duplicates.", before - after)
    return df


# ---------------------------------------------------------------------------
# Aggregation helpers
# ---------------------------------------------------------------------------

def build_meta(df: pd.DataFrame) -> dict:
    return {
        "last_updated":   datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_records":  len(df),
        "by_source":      df.groupby("source").size().to_dict(),
        "injury_reports": int(df["injury_flag"].sum()),
        "total_injuries": int(df["injury_count"].sum()),
        "by_severity":    df.groupby("severity").size().to_dict(),
    }


def _breakdown_with_sources(df: pd.DataFrame, group_col: str) -> list:
    """Generic helper: group by col, add total + per-source breakdown."""
    grouped = (
        df.groupby(group_col)
        .agg(total=("id", "count"))
        .reset_index()
        .sort_values("total", ascending=False)
    )
    source_counts = df.groupby([group_col, "source"]).size().unstack(fill_value=0)
    result = []
    for _, row in grouped.iterrows():
        key   = row[group_col]
        entry = {group_col: key, "total": int(row["total"]), "by_source": {}}
        if key in source_counts.index:
            entry["by_source"] = {s: int(source_counts.loc[key, s]) for s in source_counts.columns}
        result.append(entry)
    return result


def build_by_category(df: pd.DataFrame) -> list:
    return _breakdown_with_sources(df, "product_category")


def build_by_region(df: pd.DataFrame) -> list:
    grouped = df.groupby("region").agg(total=("id", "count")).reset_index()
    cat_counts = df.groupby(["region", "product_category"]).size().unstack(fill_value=0)
    sev_counts = df.groupby(["region", "severity"]).size().unstack(fill_value=0)
    result = []
    for _, row in grouped.iterrows():
        reg   = row["region"]
        entry = {
            "region":       reg,
            "total":        int(row["total"]),
            "by_category":  {},
            "by_severity":  {},
        }
        if reg in cat_counts.index:
            entry["by_category"] = {c: int(cat_counts.loc[reg, c]) for c in cat_counts.columns}
        if reg in sev_counts.index:
            entry["by_severity"] = {s: int(sev_counts.loc[reg, s]) for s in sev_counts.columns}
        result.append(entry)
    return result


def build_by_hazard_type(df: pd.DataFrame) -> list:
    """Breakdown by root cause — enables 'trend by root cause' analysis."""
    return _breakdown_with_sources(df, "hazard_type")


def build_by_injury_type(df: pd.DataFrame) -> list:
    """Breakdown by injury category."""
    return _breakdown_with_sources(df, "injury_type")


def build_by_severity(df: pd.DataFrame) -> list:
    """Breakdown by gravity level, with region and category breakdowns."""
    order = ["Fatal", "Serious", "Moderate", "Minor", "Not Reported"]
    grouped = (
        df.groupby("severity")
        .agg(total=("id", "count"), injuries=("injury_count", "sum"))
        .reset_index()
    )
    # Sort by defined gravity order
    grouped["_order"] = grouped["severity"].apply(
        lambda s: order.index(s) if s in order else len(order)
    )
    grouped = grouped.sort_values("_order").drop(columns=["_order"])

    region_counts   = df.groupby(["severity", "region"]).size().unstack(fill_value=0)
    category_counts = df.groupby(["severity", "product_category"]).size().unstack(fill_value=0)
    hazard_counts   = df.groupby(["severity", "hazard_type"]).size().unstack(fill_value=0)

    result = []
    for _, row in grouped.iterrows():
        sev   = row["severity"]
        entry = {
            "severity":        sev,
            "total":           int(row["total"]),
            "total_injuries":  int(row["injuries"]),
            "by_region":       {},
            "by_category":     {},
            "by_hazard_type":  {},
        }
        if sev in region_counts.index:
            entry["by_region"] = {r: int(region_counts.loc[sev, r]) for r in region_counts.columns}
        if sev in category_counts.index:
            entry["by_category"] = {c: int(category_counts.loc[sev, c]) for c in category_counts.columns}
        if sev in hazard_counts.index:
            entry["by_hazard_type"] = {h: int(hazard_counts.loc[sev, h]) for h in hazard_counts.columns}
        result.append(entry)
    return result


def build_by_country_origin(df: pd.DataFrame) -> list:
    counts = (
        df[df["country_of_origin"].str.len() > 0]
        .groupby("country_of_origin")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .head(30)
    )
    return counts.rename(columns={"country_of_origin": "country"}).to_dict("records")


def build_trends_monthly(df: pd.DataFrame) -> list:
    """Monthly totals per source — for the main trend line chart."""
    df2 = df.copy()
    df2["month"] = pd.to_datetime(df2["date"], errors="coerce").dt.to_period("M")
    df2 = df2.dropna(subset=["month"])
    pivot = (
        df2.groupby(["month", "source"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
        .sort_values("month")
    )
    result = []
    for _, row in pivot.iterrows():
        entry = {"month": str(row["month"])}
        for source in [c for c in pivot.columns if c != "month"]:
            entry[source] = int(row[source])
        result.append(entry)
    return result


def build_severity_trends_monthly(df: pd.DataFrame) -> list:
    """Monthly counts broken down by severity — for the gravity-over-time chart."""
    df2 = df.copy()
    df2["month"] = pd.to_datetime(df2["date"], errors="coerce").dt.to_period("M")
    df2 = df2.dropna(subset=["month"])
    pivot = (
        df2.groupby(["month", "severity"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
        .sort_values("month")
    )
    result = []
    for _, row in pivot.iterrows():
        entry = {"month": str(row["month"])}
        for sev in [c for c in pivot.columns if c != "month"]:
            entry[sev] = int(row[sev])
        result.append(entry)
    return result


def build_hazard_trends_monthly(df: pd.DataFrame) -> list:
    """Monthly counts by root cause — for the root-cause trend chart."""
    df2 = df.copy()
    df2["month"] = pd.to_datetime(df2["date"], errors="coerce").dt.to_period("M")
    df2 = df2.dropna(subset=["month"])
    # Limit to top 8 hazard types to keep the JSON small
    top_hazards = (
        df2.groupby("hazard_type").size()
        .sort_values(ascending=False)
        .head(8)
        .index.tolist()
    )
    df2 = df2[df2["hazard_type"].isin(top_hazards)]
    pivot = (
        df2.groupby(["month", "hazard_type"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
        .sort_values("month")
    )
    result = []
    for _, row in pivot.iterrows():
        entry = {"month": str(row["month"])}
        for h in [c for c in pivot.columns if c != "month"]:
            entry[h] = int(row[h])
        result.append(entry)
    return result


def build_recent_alerts(df: pd.DataFrame, n: int = 100) -> list:
    return df.sort_values("date", ascending=False).head(n).fillna("").to_dict("records")


def build_risk_breakdown(df: pd.DataFrame) -> list:
    counts = (
        df.groupby("risk_type")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    total = counts["count"].sum()
    counts["percentage"] = (counts["count"] / total * 100).round(1)
    return counts.to_dict("records")


def build_injury_alerts(df: pd.DataFrame) -> list:
    """Records where injury_flag is True, sorted by severity then date."""
    sev_order = {"Fatal": 0, "Serious": 1, "Moderate": 2, "Minor": 3, "Not Reported": 4}
    injured = df[df["injury_flag"] == True].copy().fillna("")
    injured["_sev_order"] = injured["severity"].map(sev_order).fillna(5)
    injured = injured.sort_values(["_sev_order", "date"], ascending=[True, False])
    injured = injured.drop(columns=["_sev_order"])
    return injured.to_dict("records")


# ---------------------------------------------------------------------------
# Output writer
# ---------------------------------------------------------------------------

def write_json(filename: str, data) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    log.info("Written: %s", path)


# ---------------------------------------------------------------------------
# Source registry — add new sources here, nothing else needs to change
# ---------------------------------------------------------------------------

SOURCES = [
    ("EU Safety Gate", fetch_safety_gate,   normalise_safety_gate),
    ("CPSC",           fetch_cpsc,           normalise_cpsc),
    ("Health Canada",  fetch_health_canada,  normalise_health_canada),
    ("RappelConso",    fetch_rappelconso,    normalise_rappelconso),
    ("FDA",            fetch_fda,            normalise_fda),
    ("NHTSA",          fetch_nhtsa,          normalise_nhtsa),
    ("OPSS",           fetch_opss,           normalise_opss),
    ("ACCC",           fetch_accc,           normalise_accc),
]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run() -> None:
    log.info("=== Safety Dashboard Pipeline — START (%d sources) ===", len(SOURCES))

    frames = []
    for label, fetch_fn, normalise_fn in SOURCES:
        try:
            raw  = fetch_fn()
            norm = normalise_fn(raw)
            if not norm.empty:
                frames.append(norm)
                log.info("%s: %d records added.", label, len(norm))
            else:
                log.warning("%s: 0 records returned.", label)
        except Exception as exc:
            log.error("%s pipeline failed: %s", label, exc, exc_info=True)

    if not frames:
        log.error("All sources failed — nothing to write.")
        sys.exit(1)

    df = pd.concat(frames, ignore_index=True)
    df = deduplicate(df)

    # Ensure typed columns
    df["injury_flag"]  = df["injury_flag"].fillna(False).astype(bool)
    df["injury_count"] = pd.to_numeric(df["injury_count"], errors="coerce").fillna(0).astype(int)

    log.info("Combined dataset: %d records from %d source(s).", len(df), len(frames))

    # --- Core outputs ---
    write_json("meta.json",                  build_meta(df))
    write_json("by_category.json",           build_by_category(df))
    write_json("by_region.json",             build_by_region(df))
    write_json("by_country_origin.json",     build_by_country_origin(df))
    write_json("trends_monthly.json",        build_trends_monthly(df))
    write_json("recent_alerts.json",         build_recent_alerts(df))
    write_json("risk_breakdown.json",        build_risk_breakdown(df))

    # --- Injury / hazard analysis outputs ---
    write_json("by_hazard_type.json",        build_by_hazard_type(df))
    write_json("by_injury_type.json",        build_by_injury_type(df))
    write_json("by_severity.json",           build_by_severity(df))
    write_json("severity_trends_monthly.json", build_severity_trends_monthly(df))
    write_json("hazard_trends_monthly.json", build_hazard_trends_monthly(df))
    write_json("injury_alerts.json",         build_injury_alerts(df))

    log.info("=== Safety Dashboard Pipeline — DONE ===")


if __name__ == "__main__":
    run()
