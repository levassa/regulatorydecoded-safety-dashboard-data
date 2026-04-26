"""
Product Safety Dashboard — Data Collection & Normalisation Pipeline

Fetches product recall / safety-alert data from eight public sources,
normalises everything into a shared schema, and writes pre-aggregated
JSON files + a SQLite database to /data/ and /db/.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
HOW TO UPDATE THIS FILE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  API endpoint / dataset changed?
    → Edit the SOURCE_CONFIG block below (Section 1).
      Every URL, dataset slug, and field-name mapping lives there.

  A field name in the response changed?
    → Find the source in SOURCE_CONFIG and update its f_* key.
      Most f_* keys accept a list — first non-empty value wins.

  Add a new taxonomy keyword?
    → Edit CATEGORY_MAP, HAZARD_TYPE_MAP or INJURY_TYPE_MAP (Section 2).

  Add a brand-new source?
    → (1) Add an entry to SOURCE_CONFIG.
      (2) Add fetch_<name>() + normalise_<name>() functions.
      (3) Append to SOURCES at the bottom of the file.

  Nothing else should need changing for routine maintenance.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Shared schema
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  id                 SHA-1 of (source + reference)  — deduplication key
  source             human-readable source name
  date               ISO date YYYY-MM-DD
  region             broad region label
  notifying_country  country that issued the alert
  country_of_origin  manufacturing / origin country
  product_category   normalised product taxonomy  (see CATEGORY_MAP)
  product_desc       free-text product description
  risk_type          legacy field — kept for dashboard backwards-compat
  hazard_type        root-cause mechanism  (see HAZARD_TYPE_MAP)
  injury_type        injury to the person  (see INJURY_TYPE_MAP)
  injury_description free-text incident / injury narrative
  severity           Fatal | Serious | Moderate | Minor | Not Reported
  injury_flag        True when at least one injury has been reported
  injury_count       number of reported injuries (0 = unknown / none)
  corrective_action  recall / withdrawal / ban description
  reference          source-specific alert identifier
"""

import csv
import hashlib
import io
import json
import logging
import os
import sqlite3
import sys
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, date

import pandas as pd
import requests


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — SOURCE CONFIG
# ══════════════════════════════════════════════════════════════════════════════
#
# This is the ONLY block you need to edit for routine maintenance.
#
# Keys used across all sources:
#   url / base_url  API endpoint or ZIP download URL
#   type            "ods" | "rest" | "zip" | "rss" | "custom"
#   page_size       records per request (REST / ODS)
#   max_records     hard cap on total records fetched
#   region          region label written into every normalised record
#
# Field-mapping keys (f_*):
#   Each is a list of candidate field names, tried left-to-right.
#   The first non-empty value found in the raw record is used.
#   Update the list when an API renames a field.
# ══════════════════════════════════════════════════════════════════════════════

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
DB_DIR     = os.path.join(os.path.dirname(__file__), "..", "db")

REQUEST_TIMEOUT = 45   # seconds — used for all HTTP calls

SOURCE_CONFIG = {

    # ── 1. EU Safety Gate (RAPEX) ────────────────────────────────────────────
    # OpenDataSoft mirror of the European Commission's RAPEX / Safety Gate feed.
    # Dataset slug: drop the @public suffix when using public.opendatasoft.com.
    # If this returns 400, check the order_by field name in the ODS schema.
    "EU Safety Gate": dict(
        type        = "ods",
        base_url    = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets",
        dataset     = "healthref-europe-rapex-en",        # ← no @public suffix here
        order_by    = "alert_date desc",                  # ← ODS field, not "date"
        page_size   = 100,
        max_records = 5000,
        region      = "EU / EEA",
        # Field mappings — first non-empty value in the raw record is used
        f_ref       = ["reference", "alert_number"],
        f_date      = ["alert_date", "date", "publication_date"],
        f_country   = ["alert_country", "notifying_country"],
        f_origin    = ["product_country", "country_of_origin"],
        f_category  = ["product_category", "product_type"],
        f_desc      = ["product_description", "product_name", "title"],
        f_risk      = ["risk_type", "risk"],              # short risk label
        f_risk_long = ["risk", "risk_type"],              # long risk description
        f_action    = ["measures_taken", "corrective_action"],
    ),

    # ── 2. CPSC (US Consumer Product Safety Commission) ──────────────────────
    # SaferProducts.gov REST API. Returns nested arrays for injuries/hazards.
    # 403 errors → the User-Agent header in get_json() should fix them.
    "CPSC": dict(
        type        = "custom",
        url         = "https://www.saferproducts.gov/RestWebServices/Recall",
        max_records = 2000,
        date_start  = "2020-01-01",
        region      = "USA",
    ),

    # ── 3. Health Canada ─────────────────────────────────────────────────────
    # Single JSON file containing all recalls. Large (~33k records).
    # The dedup key uses recallId; if that field is absent a composite key
    # of title + date + category is used to avoid all records collapsing to one.
    "Health Canada": dict(
        type   = "custom",
        url    = (
            "https://recalls-rappels.canada.ca/sites/default/files/"
            "opendata-donneesouvertes/HCRSAMOpenData.json"
        ),
        region = "Canada",
    ),

    # ── 4. RappelConso (France) ───────────────────────────────────────────────
    # OpenDataSoft — French Ministry of Economy recall portal.
    # V1 (rappelconso0) was decommissioned end-2025. Now on V2.
    # If V2 field names differ from V1, update the f_* keys here.
    "RappelConso": dict(
        type        = "ods",
        base_url    = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets",
        dataset     = "rappelconso-v2-gtin-espaces",     # ← V2 slug (V1: rappelconso0)
        order_by    = "date_de_publication desc",
        page_size   = 100,
        max_records = 3000,
        region      = "France",
        f_ref       = ["reference_fiche", "numero_de_version"],
        f_date      = ["date_de_publication"],
        f_brand     = ["nom_de_la_marque_du_produit"],
        f_model     = ["noms_des_modeles_ou_references", "produits_ou_sous_categories"],
        f_category  = ["categorie_de_produit", "sous_categorie_de_produit"],
        f_origin    = ["pays_fabricant", "pays_de_fabrication"],
        f_risk      = ["risques_encourus_par_le_consommateur"],
        f_risk_long = ["description_complementaire_du_risque",
                       "risques_encourus_par_le_consommateur"],
        f_action    = ["conduites_a_tenir_par_le_consommateur"],
    ),

    # ── 5. FDA openFDA (device + food enforcement) ────────────────────────────
    # Two endpoints combined. Uses date range search.
    # If you get 500 errors, narrow the date window (e.g. raise date_start).
    # Far-future end dates (e.g. 29991231) cause 500 — always use a near date.
    "FDA": dict(
        type        = "custom",
        device_url  = "https://api.fda.gov/device/enforcement.json",
        food_url    = "https://api.fda.gov/food/enforcement.json",
        page_size   = 100,
        max_records = 2000,
        date_start  = "20200101",    # YYYYMMDD — openFDA format
        date_end    = "20261231",    # ← update each year; never use far future
        region      = "USA",
    ),

    # ── 6. NHTSA Vehicle Recalls (flat-file ZIP) ──────────────────────────────
    # Pipe-delimited flat file. NHTSA occasionally renames the ZIP.
    # Current file: FLAT_RCL_POST_2010.zip (was FLAT_RCL.zip — 404 after rename)
    "NHTSA": dict(
        type        = "custom",
        url         = "https://static.nhtsa.gov/odi/ffdd/rcl/FLAT_RCL_POST_2010.zip",
        filename    = "FLAT_RCL.txt",    # file inside the ZIP
        min_year    = 2020,
        region      = "USA",
    ),

    # ── 7. OPSS UK (GOV.UK search API) ───────────────────────────────────────
    # Uses the GOV.UK general search API filtered to product safety alerts.
    # Hard-capped at 1000 records (GOV.UK limit).
    "OPSS": dict(
        type        = "custom",
        url         = "https://www.gov.uk/api/search.json",
        page_size   = 100,
        max_records = 1000,
        doc_type    = "product_safety_alert_report_recall",
        region      = "United Kingdom",
    ),

    # ── 8. ACCC Australia (RSS feed) ─────────────────────────────────────────
    # RSS 2.0 feed — note the .xml extension (omitting it gives 404).
    "ACCC": dict(
        type   = "custom",
        url    = "https://www.productsafety.gov.au/rss/recalls.xml",  # ← .xml required
        region = "Australia",
    ),
}


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — TAXONOMY MAPS
# ══════════════════════════════════════════════════════════════════════════════
# Add keywords here to improve classification without touching any logic code.

CATEGORY_MAP = {
    # Cosmetics & Personal Care
    "cosmetics": "Cosmetics & Personal Care",
    "cosmetic":  "Cosmetics & Personal Care",
    "personal care": "Cosmetics & Personal Care",
    "hygiene":   "Cosmetics & Personal Care",
    "beauty":    "Cosmetics & Personal Care",
    "perfume":   "Cosmetics & Personal Care",
    "skincare":  "Cosmetics & Personal Care",

    # Toys & Childcare
    "toys":      "Toys & Childcare",
    "toy":       "Toys & Childcare",
    "childcare": "Toys & Childcare",
    "child care":"Toys & Childcare",
    "articles for children": "Toys & Childcare",
    "children":  "Toys & Childcare",
    "infant":    "Toys & Childcare",
    "baby":      "Toys & Childcare",

    # Electrical & Electronics
    "electrical":          "Electrical & Electronics",
    "electronics":         "Electrical & Electronics",
    "electronic":          "Electrical & Electronics",
    "lighting":            "Electrical & Electronics",
    "batteries":           "Electrical & Electronics",
    "battery":             "Electrical & Electronics",
    "appliances":          "Electrical & Electronics",
    "household appliances":"Electrical & Electronics",
    "charger":             "Electrical & Electronics",
    "power":               "Electrical & Electronics",
    "laser":               "Electrical & Electronics",

    # Motor Vehicles
    "motor vehicles": "Motor Vehicles",
    "motor vehicle":  "Motor Vehicles",
    "vehicles":       "Motor Vehicles",
    "vehicle":        "Motor Vehicles",
    "automotive":     "Motor Vehicles",
    "automobile":     "Motor Vehicles",
    "bicycle":        "Motor Vehicles",
    "bicycles":       "Motor Vehicles",
    "e-bike":         "Motor Vehicles",
    "e-scooter":      "Motor Vehicles",
    "scooter":        "Motor Vehicles",
    "tire":           "Motor Vehicles",
    "tyre":           "Motor Vehicles",
    "airbag":         "Motor Vehicles",
    "seat belt":      "Motor Vehicles",
    "child seat":     "Motor Vehicles",

    # Chemicals
    "chemical":  "Chemicals",
    "chemicals": "Chemicals",

    # Clothing & Textiles
    "clothing":  "Clothing & Textiles",
    "textile":   "Clothing & Textiles",
    "textiles":  "Clothing & Textiles",
    "garment":   "Clothing & Textiles",
    "footwear":  "Clothing & Textiles",
    "apparel":   "Clothing & Textiles",
    "fashion":   "Clothing & Textiles",

    # Food & Food Contact
    "food":          "Food & Food Contact",
    "food contact":  "Food & Food Contact",
    "allergen":      "Food & Food Contact",
    "dietary":       "Food & Food Contact",

    # Medical Devices
    "medical":         "Medical Devices",
    "medical device":  "Medical Devices",
    "medical devices": "Medical Devices",
    "device":          "Medical Devices",

    # Furniture & Home
    "furniture":       "Furniture & Home",
    "home":            "Furniture & Home",
    "home furnishings":"Furniture & Home",
    "bedding":         "Furniture & Home",

    # Sports & Leisure
    "sport":   "Sports & Leisure",
    "sports":  "Sports & Leisure",
    "leisure": "Sports & Leisure",
    "outdoor": "Sports & Leisure",
    "camping": "Sports & Leisure",
    "fitness": "Sports & Leisure",

    # Tools & Machinery
    "machinery": "Tools & Machinery",
    "tools":     "Tools & Machinery",
    "tool":      "Tools & Machinery",
    "equipment": "Tools & Machinery",
}

# Root-cause mechanism — "What went wrong with the product?"
HAZARD_TYPE_MAP = {
    # Electrical
    "electrical fault": "Electrical Fault",
    "electric shock":   "Electrical Fault",
    "electrocution":    "Electrical Fault",
    "short circuit":    "Electrical Fault",
    "electrical":       "Electrical Fault",
    "wiring":           "Electrical Fault",
    "insulation":       "Electrical Fault",
    "voltage":          "Electrical Fault",
    "overload":         "Electrical Fault",

    # Flammability / overheating
    "fire":             "Flammability / Overheating",
    "flammab":          "Flammability / Overheating",
    "overheating":      "Flammability / Overheating",
    "overheat":         "Flammability / Overheating",
    "ignit":            "Flammability / Overheating",
    "combustion":       "Flammability / Overheating",
    "smoke":            "Flammability / Overheating",
    "thermal runaway":  "Flammability / Overheating",

    # Explosion
    "explosion":        "Explosion",
    "explosive":        "Explosion",
    "burst":            "Explosion",
    "pressur":          "Explosion",

    # Sharp edges / points
    "sharp edge":       "Sharp Edges / Points",
    "sharp point":      "Sharp Edges / Points",
    "sharp":            "Sharp Edges / Points",
    "laceration":       "Sharp Edges / Points",
    "puncture":         "Sharp Edges / Points",
    "blade":            "Sharp Edges / Points",

    # Mechanical failure
    "mechanical failure":  "Mechanical Failure",
    "structural failure":  "Mechanical Failure",
    "collapse":            "Mechanical Failure",
    "breakage":            "Mechanical Failure",
    "fracture":            "Mechanical Failure",
    "defect":              "Mechanical Failure",
    "malfunction":         "Mechanical Failure",
    "detach":              "Mechanical Failure",
    "loose part":          "Mechanical Failure",

    # Small parts / choking
    "small part":       "Small Parts / Choking Hazard",
    "choking":          "Small Parts / Choking Hazard",
    "suffocation":      "Small Parts / Choking Hazard",
    "strangulation":    "Small Parts / Choking Hazard",
    "asphyxia":         "Small Parts / Choking Hazard",

    # Chemical / toxic
    "chemical":         "Chemical / Toxic Substance",
    "toxic":            "Chemical / Toxic Substance",
    "hazardous substance": "Chemical / Toxic Substance",
    "carcinogen":       "Chemical / Toxic Substance",
    "heavy metal":      "Chemical / Toxic Substance",
    "lead":             "Chemical / Toxic Substance",
    "cadmium":          "Chemical / Toxic Substance",
    "phthalate":        "Chemical / Toxic Substance",
    "bpa":              "Chemical / Toxic Substance",
    "formaldehyde":     "Chemical / Toxic Substance",

    # Microbiological
    "microbiological":  "Microbiological Contamination",
    "bacteria":         "Microbiological Contamination",
    "listeria":         "Microbiological Contamination",
    "salmonella":       "Microbiological Contamination",
    "e. coli":          "Microbiological Contamination",
    "mould":            "Microbiological Contamination",
    "mold":             "Microbiological Contamination",
    "pathogen":         "Microbiological Contamination",

    # Allergen
    "allergen":         "Allergen",
    "allergy":          "Allergen",
    "anaphyla":         "Allergen",
    "undeclared":       "Allergen",

    # Instability / fall risk
    "instability":      "Instability / Fall Risk",
    "tip-over":         "Instability / Fall Risk",
    "tip over":         "Instability / Fall Risk",
    "topple":           "Instability / Fall Risk",
    "fall":             "Instability / Fall Risk",

    # Radiation
    "radiation":        "Radiation",
    "uv":               "Radiation",
    "radioactive":      "Radiation",

    # Drowning
    "drowning":         "Drowning Risk",
    "water":            "Drowning Risk",
    "submersion":       "Drowning Risk",

    # Entrapment
    "entrapment":       "Entrapment",
    "entrap":           "Entrapment",
    "pinch":            "Entrapment",
    "crush":            "Entrapment",
    "trap":             "Entrapment",
}

# Injury category — "What happens to the person?"
INJURY_TYPE_MAP = {
    "burn":             "Burns",
    "burns":            "Burns",
    "scald":            "Burns",
    "thermal injury":   "Burns",
    "skin burn":        "Burns",

    "electric shock":   "Electric Shock",
    "electrocution":    "Electric Shock",
    "shock":            "Electric Shock",

    "laceration":       "Laceration / Cut",
    "cut":              "Laceration / Cut",
    "slash":            "Laceration / Cut",
    "abrasion":         "Laceration / Cut",
    "wound":            "Laceration / Cut",

    "fracture":         "Fracture / Blunt Trauma",
    "broken bone":      "Fracture / Blunt Trauma",
    "broken":           "Fracture / Blunt Trauma",
    "contusion":        "Fracture / Blunt Trauma",
    "bruise":           "Fracture / Blunt Trauma",
    "impact":           "Fracture / Blunt Trauma",
    "blunt":            "Fracture / Blunt Trauma",

    "choking":          "Choking / Suffocation",
    "choke":            "Choking / Suffocation",
    "suffocation":      "Choking / Suffocation",
    "suffocate":        "Choking / Suffocation",
    "strangulation":    "Choking / Suffocation",
    "asphyxia":         "Choking / Suffocation",
    "asphyxiation":     "Choking / Suffocation",

    "poisoning":        "Poisoning / Ingestion",
    "poison":           "Poisoning / Ingestion",
    "ingestion":        "Poisoning / Ingestion",
    "toxic":            "Poisoning / Ingestion",
    "intoxication":     "Poisoning / Ingestion",
    "overdose":         "Poisoning / Ingestion",

    "allerg":           "Allergic Reaction",
    "anaphyla":         "Allergic Reaction",

    "drowning":         "Drowning",
    "drown":            "Drowning",

    "eye injury":       "Eye Injury",
    "eye":              "Eye Injury",
    "vision":           "Eye Injury",
    "blindness":        "Eye Injury",

    "skin irritation":  "Skin Irritation",
    "dermatitis":       "Skin Irritation",
    "rash":             "Skin Irritation",
    "irritation":       "Skin Irritation",

    "respiratory":      "Respiratory",
    "inhalation":       "Respiratory",
    "lung":             "Respiratory",
    "breathing":        "Respiratory",
    "asthma":           "Respiratory",

    "fall":             "Fall Injury",
    "tip-over":         "Fall Injury",
    "tip over":         "Fall Injury",

    "entrapment":       "Entrapment",
    "entrap":           "Entrapment",
    "crush":            "Entrapment",
    "pinch":            "Entrapment",

    "radiation":        "Radiation Exposure",
    "uv exposure":      "Radiation Exposure",
    "laser":            "Radiation Exposure",
}

# Severity keywords — checked in order Fatal → Serious → Moderate → Minor
SEVERITY_FATAL_KEYWORDS = [
    "fatal", "death", "fatality", "fatalities", "died", "killed",
    "kill", "deadly", "lethal",
]
SEVERITY_SERIOUS_KEYWORDS = [
    "hospitaliz", "hospitalis", "surgery", "surgic",
    "serious injur", "severe injur", "permanent", "disabilit",
    "life-threatening", "life threatening", "critical", "intensive care",
    "icu", "emergency room", "emergency department", "amputation",
    "class i", "danger 1", "class 1",
]
SEVERITY_MODERATE_KEYWORDS = [
    "moderate", "medical treatment", "medical attention", "doctor",
    "physician", "treatment required", "injured", "injury reported",
    "class ii", "class 2", "danger 2",
]
SEVERITY_MINOR_KEYWORDS = [
    "minor", "slight", "low risk", "mild", "superficial",
    "no injury", "no injuries", "potential", "risk of",
    "class iii", "class 3", "warning", "caution",
]

# Legacy risk_type — kept for dashboard backwards-compatibility
RISK_MAP = {
    "chemical":     "Chemical",
    "fire":         "Fire",
    "fire hazard":  "Fire",
    "flammab":      "Fire",
    "burns":        "Burns",
    "burn":         "Burns",
    "electric shock":"Electric Shock",
    "electrical":   "Electric Shock",
    "electrocution":"Electric Shock",
    "injury":       "Injury",
    "laceration":   "Injury",
    "choking":      "Choking",
    "suffocation":  "Choking",
    "strangulation":"Choking",
    "explosion":    "Explosion",
    "environmental":"Environmental",
    "contamination":"Contamination",
    "microbiological":"Contamination",
    "drowning":     "Drowning",
    "fall":         "Falls",
    "radiation":    "Radiation",
    "allergen":     "Contamination",
    "danger 1":     "High Risk",
    "danger 2":     "Moderate Risk",
    "warning":      "Low Risk",
}


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — LOGGING
# ══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s [%(levelname)s] %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

def make_id(source: str, reference: str) -> str:
    """Stable deduplication key — SHA-1 of source + reference."""
    raw = f"{source}::{reference}".encode("utf-8")
    return hashlib.sha1(raw).hexdigest()


def safe_date(value) -> str:
    """Return an ISO date string YYYY-MM-DD, or empty string if unparseable."""
    if not value:
        return ""
    if isinstance(value, (datetime, date)):
        return value.strftime("%Y-%m-%d")
    s = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d",
                "%d-%m-%Y", "%B %d, %Y", "%d %B %Y",
                "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(s[:len(fmt)], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return s[:10] if len(s) >= 10 else ""


def first_val(record, keys: list, default: str = "") -> str:
    """Return the first non-empty value found in record for any key in keys."""
    for k in keys:
        v = record.get(k)
        if v and str(v).strip() not in ("", "nan", "None"):
            return str(v).strip()
    return default


# User-Agent avoids 403s from APIs that block headless requests (e.g. CPSC).
_HEADERS = {"User-Agent": "SafetyDashboard/1.0 (regulatorydecoded.com; research)"}


def get_json(url: str, params: dict = None) -> dict:
    r = requests.get(url, params=params, headers=_HEADERS, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


# ── Normalisation helpers ─────────────────────────────────────────────────────

def normalise_category(raw: str) -> str:
    if not raw:
        return "Other"
    lower = raw.lower().strip()
    for key, value in CATEGORY_MAP.items():
        if key in lower:
            return value
    return raw.title()


def normalise_risk(raw: str) -> str:
    """Legacy risk_type — kept for backwards-compatibility."""
    if not raw:
        return "Other"
    lower = raw.lower().strip()
    for key, value in RISK_MAP.items():
        if key in lower:
            return value
    return raw.title()


def normalise_hazard_type(text: str) -> str:
    """Extract root-cause mechanism from free text. Longer keys checked first."""
    if not text:
        return "Other"
    lower = text.lower()
    for key in sorted(HAZARD_TYPE_MAP, key=len, reverse=True):
        if key in lower:
            return HAZARD_TYPE_MAP[key]
    return "Other"


def normalise_injury_type(text: str) -> str:
    """Extract injury category from free text. Longer keys checked first."""
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
    Determine gravity level from keyword scan + optional structured signals.
    Returns: Fatal | Serious | Moderate | Minor | Not Reported
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
    return "Moderate" if has_injuries else "Not Reported"


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — GENERIC FETCHERS
# ══════════════════════════════════════════════════════════════════════════════

def fetch_ods_paginated(cfg: dict) -> pd.DataFrame:
    """
    Generic paginated fetcher for any OpenDataSoft v2.1 dataset.
    Used by EU Safety Gate and RappelConso — both run on ODS.
    Pass the SOURCE_CONFIG entry for the relevant source.
    """
    name    = cfg.get("_name", "ODS")
    url     = f"{cfg['base_url']}/{cfg['dataset']}/records"
    records = []
    offset  = 0
    while offset < cfg["max_records"]:
        params = {
            "limit":    cfg["page_size"],
            "offset":   offset,
            "order_by": cfg["order_by"],
        }
        try:
            payload = get_json(url, params)
        except Exception as exc:
            log.warning("%s error at offset %d: %s", name, offset, exc)
            break
        batch = payload.get("results", [])
        if not batch:
            break
        records.extend(batch)
        log.info("  %s: %d fetched (total so far: %d)", name, len(batch), len(records))
        if len(batch) < cfg["page_size"]:
            break
        offset += cfg["page_size"]
    log.info("%s: %d raw records.", name, len(records))
    return pd.DataFrame(records)


def normalise_ods(df: pd.DataFrame, cfg: dict, source_label: str) -> pd.DataFrame:
    """
    Generic normaliser for ODS sources.
    Field mappings are read from cfg["f_*"] lists.
    Works for EU Safety Gate; override for RappelConso (bespoke product_desc).
    """
    if df.empty:
        return pd.DataFrame()
    rows = []
    for _, r in df.iterrows():
        ref        = first_val(r, cfg["f_ref"])
        risk_short = first_val(r, cfg["f_risk"])
        risk_long  = first_val(r, cfg["f_risk_long"]) or risk_short
        full_text  = f"{risk_short} {risk_long}"

        rows.append({
            "id":                make_id(source_label, ref),
            "source":            source_label,
            "date":              safe_date(first_val(r, cfg["f_date"])),
            "region":            cfg["region"],
            "notifying_country": first_val(r, cfg["f_country"]),
            "country_of_origin": first_val(r, cfg["f_origin"]),
            "product_category":  normalise_category(first_val(r, cfg["f_category"])),
            "product_desc":      first_val(r, cfg["f_desc"]),
            "risk_type":         normalise_risk(risk_short),
            "hazard_type":       normalise_hazard_type(full_text),
            "injury_type":       normalise_injury_type(full_text),
            "injury_description": risk_long[:500],
            "severity":          extract_severity(full_text),
            "injury_flag":       False,
            "injury_count":      0,
            "corrective_action": first_val(r, cfg["f_action"]),
            "reference":         ref,
        })
    out = pd.DataFrame(rows)
    log.info("%s: %d normalised.", source_label, len(out))
    return out


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — SOURCE IMPLEMENTATIONS
# ══════════════════════════════════════════════════════════════════════════════
# Each source has a fetch_*() and normalise_*() function.
# URLs / field names are read from SOURCE_CONFIG — don't hard-code them here.

# ── 1. EU Safety Gate ────────────────────────────────────────────────────────

def fetch_safety_gate() -> pd.DataFrame:
    cfg = SOURCE_CONFIG["EU Safety Gate"]
    cfg["_name"] = "EU Safety Gate"
    return fetch_ods_paginated(cfg)


def normalise_safety_gate(df: pd.DataFrame) -> pd.DataFrame:
    cfg = SOURCE_CONFIG["EU Safety Gate"]
    return normalise_ods(df, cfg, "EU Safety Gate")


# ── 2. CPSC ──────────────────────────────────────────────────────────────────
# Bespoke normaliser: CPSC wraps injuries, hazards, products in nested arrays.

def fetch_cpsc() -> pd.DataFrame:
    cfg    = SOURCE_CONFIG["CPSC"]
    params = {"format": "json", "RecallDateStart": cfg["date_start"], "Limit": cfg["max_records"]}
    log.info("Fetching CPSC recall data...")
    try:
        data = get_json(cfg["url"], params)
    except Exception as exc:
        log.warning("CPSC fetch error: %s", exc)
        return pd.DataFrame()
    records = data if isinstance(data, list) else data.get("Recalls", data.get("recalls", []))
    log.info("CPSC: %d raw records.", len(records))
    return pd.DataFrame(records)


def normalise_cpsc(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    def _first_name(items, *fallback_keys):
        if items and isinstance(items, list):
            return items[0].get("Name", items[0].get("Description", ""))
        for k in fallback_keys:
            if df.columns.tolist() and k in df.columns:
                pass
        return ""

    rows = []
    for _, r in df.iterrows():
        ref          = str(r.get("RecallNumber", r.get("recall_number", ""))).strip()
        products     = r.get("Products", [])
        product_desc = (products[0].get("Name", products[0].get("Description", ""))
                        if products and isinstance(products, list)
                        else str(r.get("ProductName", "")).strip())
        hazards      = r.get("Hazards", [])
        risk_raw     = (hazards[0].get("Name", hazards[0].get("Description", ""))
                        if hazards and isinstance(hazards, list)
                        else str(r.get("Hazard", "")).strip())
        categories   = r.get("ProductTypes", [])
        cat_raw      = (categories[0].get("Name", "")
                        if categories and isinstance(categories, list)
                        else str(r.get("ProductType", "")).strip())
        remedy       = r.get("Remedies", [])
        action       = (remedy[0].get("Name", "")
                        if remedy and isinstance(remedy, list)
                        else str(r.get("Remedy", "")).strip())

        # Injury extraction — CPSC often reports actual injury counts
        injuries        = r.get("Injuries", [])
        has_injuries    = bool(injuries and isinstance(injuries, list))
        injury_count    = 0
        injury_parts    = []
        if has_injuries:
            for inj in injuries:
                try:
                    injury_count += int(inj.get("NumInjuries", inj.get("Count", 0)) or 0)
                except (ValueError, TypeError):
                    pass
                desc = inj.get("Description", inj.get("Name", ""))
                if desc:
                    injury_parts.append(str(desc))
        injury_desc = "; ".join(injury_parts) if injury_parts else risk_raw
        full_text   = f"{risk_raw} {injury_desc}"

        rows.append({
            "id":                make_id("CPSC", ref),
            "source":            "CPSC",
            "date":              safe_date(r.get("RecallDate", r.get("recall_date", ""))),
            "region":            SOURCE_CONFIG["CPSC"]["region"],
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


# ── 3. Health Canada ─────────────────────────────────────────────────────────

def fetch_health_canada() -> pd.DataFrame:
    cfg = SOURCE_CONFIG["Health Canada"]
    log.info("Fetching Health Canada recall data...")
    try:
        data = get_json(cfg["url"])
    except Exception as exc:
        log.warning("Health Canada fetch error: %s", exc)
        return pd.DataFrame()
    records = data if isinstance(data, list) else data.get("results", data.get("data", []))
    log.info("Health Canada: %d raw records.", len(records))
    return pd.DataFrame(records)


def normalise_health_canada(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    # Keep only consumer/product recalls (exclude drug, food, vehicle recalls)
    if "recallType" in df.columns:
        mask = df["recallType"].astype(str).str.lower().str.contains(
            "consumer|product", na=False
        )
        df = df[mask].copy()
        log.info("Health Canada: %d after consumer-product filter.", len(df))

    rows = []
    for _, r in df.iterrows():
        ref = str(r.get("recallId", r.get("recall_id", r.get("id", "")))).strip()
        # If recallId is missing every record gets the same hash → dedup kills all but one.
        # Fallback to a composite key so each record stays unique.
        if not ref:
            ref = f"{r.get('title','')}::{r.get('datePublished','')}::{r.get('recallCategory','')}"

        hc_class  = str(r.get("hazardClassification", r.get("hazard", ""))).strip()
        desc      = str(r.get("description", r.get("summary", ""))).strip()
        full_text = f"{hc_class} {desc}"

        rows.append({
            "id":                make_id("Health Canada", ref),
            "source":            "Health Canada",
            "date":              safe_date(r.get("datePublished", r.get("recallDate", r.get("date", "")))),
            "region":            SOURCE_CONFIG["Health Canada"]["region"],
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


# ── 4. RappelConso (France) ───────────────────────────────────────────────────
# Uses the generic ODS fetcher but needs a custom normaliser for product_desc
# because brand + model are in separate fields.

def fetch_rappelconso() -> pd.DataFrame:
    cfg = SOURCE_CONFIG["RappelConso"]
    cfg["_name"] = "RappelConso"
    return fetch_ods_paginated(cfg)


def normalise_rappelconso(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    cfg  = SOURCE_CONFIG["RappelConso"]
    rows = []
    for _, r in df.iterrows():
        ref   = first_val(r, cfg["f_ref"])
        brand = first_val(r, cfg["f_brand"])
        model = first_val(r, cfg["f_model"])
        product_desc = f"{brand} — {model}".strip(" —") if (brand or model) else ""

        risk_short = first_val(r, cfg["f_risk"])
        risk_long  = first_val(r, cfg["f_risk_long"]) or risk_short
        full_text  = f"{risk_short} {risk_long}"

        rows.append({
            "id":                make_id("RappelConso", ref),
            "source":            "RappelConso",
            "date":              safe_date(first_val(r, cfg["f_date"])),
            "region":            cfg["region"],
            "notifying_country": "France",
            "country_of_origin": first_val(r, cfg["f_origin"]),
            "product_category":  normalise_category(first_val(r, cfg["f_category"])),
            "product_desc":      product_desc,
            "risk_type":         normalise_risk(risk_short),
            "hazard_type":       normalise_hazard_type(full_text),
            "injury_type":       normalise_injury_type(full_text),
            "injury_description": risk_long[:500],
            "severity":          extract_severity(full_text),
            "injury_flag":       False,
            "injury_count":      0,
            "corrective_action": first_val(r, cfg["f_action"]),
            "reference":         ref,
        })
    out = pd.DataFrame(rows)
    log.info("RappelConso: %d normalised.", len(out))
    return out


# ── 5. FDA openFDA ───────────────────────────────────────────────────────────
# Two endpoints (device + food) merged. Uses date-range search.

def _fetch_fda_endpoint(url: str, label: str) -> list:
    cfg    = SOURCE_CONFIG["FDA"]
    search = f"report_date:[{cfg['date_start']}+TO+{cfg['date_end']}]"
    records, skip = [], 0
    while skip < cfg["max_records"]:
        try:
            payload = get_json(url, {"search": search, "limit": cfg["page_size"], "skip": skip})
        except Exception as exc:
            log.warning("FDA %s error at skip=%d: %s", label, skip, exc)
            break
        batch = payload.get("results", [])
        if not batch:
            break
        records.extend(batch)
        log.info("  FDA %s: %d fetched (total: %d)", label, len(batch), len(records))
        total = payload.get("meta", {}).get("results", {}).get("total", 0)
        if len(records) >= total or len(batch) < cfg["page_size"]:
            break
        skip += cfg["page_size"]
    return records


def fetch_fda() -> pd.DataFrame:
    log.info("Fetching FDA openFDA enforcement data...")
    cfg     = SOURCE_CONFIG["FDA"]
    records = _fetch_fda_endpoint(cfg["device_url"], "device") + \
              _fetch_fda_endpoint(cfg["food_url"],   "food")
    log.info("FDA openFDA: %d total raw records.", len(records))
    return pd.DataFrame(records)


def normalise_fda(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
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
            "region":            SOURCE_CONFIG["FDA"]["region"],
            "notifying_country": "United States",
            "country_of_origin": str(r.get("country", "")).strip(),
            "product_category":  normalise_category(str(r.get("product_type", product_desc))),
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


# ── 6. NHTSA (flat-file ZIP) ─────────────────────────────────────────────────

def fetch_nhtsa() -> pd.DataFrame:
    """
    Downloads and parses NHTSA's pipe-delimited recall flat file.
    Key columns: CAMPNO, MAKETXT, MODELTXT, YEARTXT, DESC_DEFECT,
                 CONEQUENCE_DEFECT, CORRECTIVE_ACTION, BGMAN (begin date)
    """
    cfg = SOURCE_CONFIG["NHTSA"]
    log.info("Downloading NHTSA recall flat file...")
    try:
        response = requests.get(cfg["url"], headers=_HEADERS, timeout=120, stream=True)
        response.raise_for_status()
        zip_bytes = io.BytesIO(response.content)
    except Exception as exc:
        log.warning("NHTSA ZIP download failed: %s", exc)
        return pd.DataFrame()

    try:
        with zipfile.ZipFile(zip_bytes) as zf:
            names  = zf.namelist()
            target = cfg["filename"] if cfg["filename"] in names else names[0]
            with zf.open(target) as fh:
                content = fh.read().decode("latin-1", errors="replace")
    except Exception as exc:
        log.warning("NHTSA ZIP extraction failed: %s", exc)
        return pd.DataFrame()

    reader  = csv.DictReader(io.StringIO(content), delimiter="|")
    records = []
    min_yr  = str(cfg["min_year"])
    for row in reader:
        campno = row.get("CAMPNO", "")
        bgman  = row.get("BGMAN", "").strip()
        if bgman and len(bgman) == 8 and bgman.isdigit():
            if bgman[:4] < min_yr:
                continue
        else:
            try:
                if int("20" + campno[:2]) < cfg["min_year"]:
                    continue
            except (ValueError, IndexError):
                pass
        records.append(dict(row))

    log.info("NHTSA: %d records after year >= %d filter.", len(records), cfg["min_year"])
    return pd.DataFrame(records)


def normalise_nhtsa(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    rows = []
    for _, r in df.iterrows():
        ref         = str(r.get("CAMPNO", "")).strip()
        make        = str(r.get("MAKETXT", "")).strip().title()
        model       = str(r.get("MODELTXT", "")).strip().title()
        year        = str(r.get("YEARTXT", "")).strip()
        defect      = str(r.get("DESC_DEFECT", "")).strip()
        consequence = str(r.get("CONEQUENCE_DEFECT", "")).strip()
        full_text   = f"{defect} {consequence}"

        rows.append({
            "id":                make_id("NHTSA", ref),
            "source":            "NHTSA",
            "date":              safe_date(str(r.get("BGMAN", "")).strip()),
            "region":            SOURCE_CONFIG["NHTSA"]["region"],
            "notifying_country": "United States",
            "country_of_origin": str(r.get("MFGNAME", "")).strip(),
            "product_category":  "Motor Vehicles",
            "product_desc":      f"{year} {make} {model}".strip(),
            "risk_type":         normalise_risk(full_text),
            "hazard_type":       normalise_hazard_type(defect),
            "injury_type":       normalise_injury_type(consequence),
            "injury_description": consequence[:500],
            "severity":          extract_severity(full_text),
            "injury_flag":       any(kw in full_text.lower()
                                     for kw in ["injur", "death", "fatal", "crash"]),
            "injury_count":      0,
            "corrective_action": str(r.get("CORRECTIVE_ACTION", "")).strip(),
            "reference":         ref,
        })
    out = pd.DataFrame(rows)
    log.info("NHTSA: %d normalised.", len(out))
    return out


# ── 7. OPSS UK ───────────────────────────────────────────────────────────────

def fetch_opss() -> pd.DataFrame:
    cfg = SOURCE_CONFIG["OPSS"]
    log.info("Fetching OPSS UK from GOV.UK search API...")
    records, start = [], 0
    while start < cfg["max_records"]:
        params = {
            "filter_content_store_document_type": cfg["doc_type"],
            "count": cfg["page_size"],
            "start": start,
            "order": "-public_timestamp",
        }
        try:
            payload = get_json(cfg["url"], params)
        except Exception as exc:
            log.warning("OPSS UK error at start=%d: %s", start, exc)
            break
        batch = payload.get("results", [])
        if not batch:
            break
        records.extend(batch)
        log.info("  OPSS UK: %d fetched (total: %d)", len(batch), len(records))
        total = payload.get("total", 0)
        if len(records) >= total or len(batch) < cfg["page_size"]:
            break
        start += cfg["page_size"]
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
            "region":            SOURCE_CONFIG["OPSS"]["region"],
            "notifying_country": "United Kingdom",
            "country_of_origin": "",
            "product_category":  normalise_category(str((r.get("filter_topics") or [""])[0])),
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


# ── 8. ACCC Australia ────────────────────────────────────────────────────────

def fetch_accc() -> pd.DataFrame:
    cfg = SOURCE_CONFIG["ACCC"]
    log.info("Fetching ACCC Australia recall RSS feed...")
    try:
        response = requests.get(cfg["url"], headers=_HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        root = ET.fromstring(response.content)
    except Exception as exc:
        log.warning("ACCC RSS fetch/parse failed: %s", exc)
        return pd.DataFrame()

    items   = root.findall(".//item")
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
            "region":            SOURCE_CONFIG["ACCC"]["region"],
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


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 7 — DEDUPLICATION
# ══════════════════════════════════════════════════════════════════════════════

def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df     = df.drop_duplicates(subset=["id"])
    after  = len(df)
    if before != after:
        log.info("Deduplication: removed %d duplicates.", before - after)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 8 — AGGREGATION HELPERS
# ══════════════════════════════════════════════════════════════════════════════

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
    grouped = (
        df.groupby(group_col)
        .agg(total=("id", "count"))
        .reset_index()
        .sort_values("total", ascending=False)
    )
    src_counts = df.groupby([group_col, "source"]).size().unstack(fill_value=0)
    result = []
    for _, row in grouped.iterrows():
        key   = row[group_col]
        entry = {group_col: key, "total": int(row["total"]), "by_source": {}}
        if key in src_counts.index:
            entry["by_source"] = {s: int(src_counts.loc[key, s]) for s in src_counts.columns}
        result.append(entry)
    return result


def build_by_category(df: pd.DataFrame) -> list:
    return _breakdown_with_sources(df, "product_category")


def build_by_region(df: pd.DataFrame) -> list:
    grouped    = df.groupby("region").agg(total=("id", "count")).reset_index()
    cat_counts = df.groupby(["region", "product_category"]).size().unstack(fill_value=0)
    sev_counts = df.groupby(["region", "severity"]).size().unstack(fill_value=0)
    result = []
    for _, row in grouped.iterrows():
        reg   = row["region"]
        entry = {"region": reg, "total": int(row["total"]), "by_category": {}, "by_severity": {}}
        if reg in cat_counts.index:
            entry["by_category"] = {c: int(cat_counts.loc[reg, c]) for c in cat_counts.columns}
        if reg in sev_counts.index:
            entry["by_severity"] = {s: int(sev_counts.loc[reg, s]) for s in sev_counts.columns}
        result.append(entry)
    return result


def build_by_hazard_type(df: pd.DataFrame) -> list:
    return _breakdown_with_sources(df, "hazard_type")


def build_by_injury_type(df: pd.DataFrame) -> list:
    return _breakdown_with_sources(df, "injury_type")


def build_by_severity(df: pd.DataFrame) -> list:
    order   = ["Fatal", "Serious", "Moderate", "Minor", "Not Reported"]
    grouped = (
        df.groupby("severity")
        .agg(total=("id", "count"), injuries=("injury_count", "sum"))
        .reset_index()
    )
    grouped["_order"] = grouped["severity"].apply(
        lambda s: order.index(s) if s in order else len(order)
    )
    grouped = grouped.sort_values("_order").drop(columns=["_order"])

    reg_counts = df.groupby(["severity", "region"]).size().unstack(fill_value=0)
    cat_counts = df.groupby(["severity", "product_category"]).size().unstack(fill_value=0)
    haz_counts = df.groupby(["severity", "hazard_type"]).size().unstack(fill_value=0)
    result = []
    for _, row in grouped.iterrows():
        sev   = row["severity"]
        entry = {
            "severity":       sev,
            "total":          int(row["total"]),
            "total_injuries": int(row["injuries"]),
            "by_region":      {},
            "by_category":    {},
            "by_hazard_type": {},
        }
        if sev in reg_counts.index:
            entry["by_region"]      = {r: int(reg_counts.loc[sev, r]) for r in reg_counts.columns}
        if sev in cat_counts.index:
            entry["by_category"]    = {c: int(cat_counts.loc[sev, c]) for c in cat_counts.columns}
        if sev in haz_counts.index:
            entry["by_hazard_type"] = {h: int(haz_counts.loc[sev, h]) for h in haz_counts.columns}
        result.append(entry)
    return result


def build_by_country_origin(df: pd.DataFrame) -> list:
    counts = (
        df[df["country_of_origin"].str.len() > 0]
        .groupby("country_of_origin").size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .head(30)
    )
    return counts.rename(columns={"country_of_origin": "country"}).to_dict("records")


def _monthly_pivot(df: pd.DataFrame, value_col: str) -> list:
    df2  = df.copy()
    df2["month"] = pd.to_datetime(df2["date"], errors="coerce").dt.to_period("M")
    df2  = df2.dropna(subset=["month"])
    pivot = (
        df2.groupby(["month", value_col]).size()
        .unstack(fill_value=0)
        .reset_index()
        .sort_values("month")
    )
    result = []
    for _, row in pivot.iterrows():
        entry = {"month": str(row["month"])}
        for col in [c for c in pivot.columns if c != "month"]:
            entry[col] = int(row[col])
        result.append(entry)
    return result


def build_trends_monthly(df: pd.DataFrame) -> list:
    return _monthly_pivot(df, "source")


def build_severity_trends_monthly(df: pd.DataFrame) -> list:
    return _monthly_pivot(df, "severity")


def build_hazard_trends_monthly(df: pd.DataFrame) -> list:
    """Top-8 hazard types by month — keeps the JSON small."""
    df2 = df.copy()
    df2["month"] = pd.to_datetime(df2["date"], errors="coerce").dt.to_period("M")
    df2 = df2.dropna(subset=["month"])
    top8 = (
        df2.groupby("hazard_type").size()
        .sort_values(ascending=False)
        .head(8).index.tolist()
    )
    return _monthly_pivot(df2[df2["hazard_type"].isin(top8)], "hazard_type")


def build_recent_alerts(df: pd.DataFrame, n: int = 100) -> list:
    return df.sort_values("date", ascending=False).head(n).fillna("").to_dict("records")


def build_risk_breakdown(df: pd.DataFrame) -> list:
    counts = (
        df.groupby("risk_type").size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    total = counts["count"].sum()
    counts["percentage"] = (counts["count"] / total * 100).round(1)
    return counts.to_dict("records")


def build_injury_alerts(df: pd.DataFrame) -> list:
    """Records where injury_flag is True, sorted by severity then date."""
    sev_order = {"Fatal": 0, "Serious": 1, "Moderate": 2, "Minor": 3, "Not Reported": 4}
    injured   = df[df["injury_flag"] == True].copy().fillna("")
    injured["_order"] = injured["severity"].map(sev_order).fillna(5)
    injured   = injured.sort_values(["_order", "date"], ascending=[True, False])
    return injured.drop(columns=["_order"]).to_dict("records")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 9 — OUTPUT WRITERS
# ══════════════════════════════════════════════════════════════════════════════

def write_json(filename: str, data) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    log.info("Written: %s", path)


_DB_COLUMNS = [
    "id", "source", "date", "year", "month", "region",
    "notifying_country", "country_of_origin", "product_category",
    "product_desc", "risk_type", "hazard_type", "injury_type",
    "injury_description", "severity", "injury_flag", "injury_count",
    "corrective_action", "reference",
]
_DB_INDEXES = [
    ("idx_date",             "date"),
    ("idx_year",             "year"),
    ("idx_region",           "region"),
    ("idx_source",           "source"),
    ("idx_product_category", "product_category"),
    ("idx_hazard_type",      "hazard_type"),
    ("idx_injury_type",      "injury_type"),
    ("idx_severity",         "severity"),
    ("idx_country_origin",   "country_of_origin"),
    ("idx_injury_flag",      "injury_flag"),
]


def _prepare_db_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy().fillna("")
    dt  = pd.to_datetime(out["date"], errors="coerce")
    out["year"]         = dt.dt.year.fillna(0).astype(int)
    out["month"]        = dt.dt.to_period("M").astype(str).replace("NaT", "")
    out["injury_flag"]  = out["injury_flag"].astype(int)
    out["injury_count"] = out["injury_count"].astype(int)
    return out


def write_sqlite(df: pd.DataFrame) -> None:
    os.makedirs(DB_DIR, exist_ok=True)
    db_path = os.path.join(DB_DIR, "safety_dashboard.db")
    conn    = sqlite3.connect(db_path)
    cur     = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS safety_incidents")
    cur.execute("""
        CREATE TABLE safety_incidents (
            id TEXT PRIMARY KEY, source TEXT, date TEXT, year INTEGER, month TEXT,
            region TEXT, notifying_country TEXT, country_of_origin TEXT,
            product_category TEXT, product_desc TEXT, risk_type TEXT,
            hazard_type TEXT, injury_type TEXT, injury_description TEXT,
            severity TEXT, injury_flag INTEGER, injury_count INTEGER,
            corrective_action TEXT, reference TEXT
        )
    """)
    out  = _prepare_db_df(df)
    rows = out[_DB_COLUMNS].values.tolist()
    cur.executemany(
        f"INSERT OR REPLACE INTO safety_incidents VALUES ({','.join(['?'] * len(_DB_COLUMNS))})",
        rows,
    )
    for name, col in _DB_INDEXES:
        cur.execute(f"CREATE INDEX IF NOT EXISTS {name} ON safety_incidents ({col})")
    conn.commit()
    conn.close()
    log.info("Written: %s (%d records)", db_path, len(out))


def write_sql_dump(df: pd.DataFrame) -> None:
    os.makedirs(DB_DIR, exist_ok=True)
    out  = _prepare_db_df(df)
    path = os.path.join(DB_DIR, "safety_dashboard.sql")

    def esc(v):
        return str(v).replace("'", "''")

    header = [
        "-- Safety Dashboard — Full Normalised Dataset",
        f"-- Generated : {datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}",
        f"-- Records   : {len(out)}",
        f"-- Sources   : {', '.join(sorted(out['source'].unique()))}",
        "", "DROP TABLE IF EXISTS safety_incidents;", "",
        "CREATE TABLE safety_incidents (",
        "    id TEXT PRIMARY KEY, source VARCHAR(50), date DATE, year INTEGER, month VARCHAR(7),",
        "    region VARCHAR(50), notifying_country VARCHAR(100), country_of_origin VARCHAR(100),",
        "    product_category VARCHAR(100), product_desc TEXT, risk_type VARCHAR(100),",
        "    hazard_type VARCHAR(100), injury_type VARCHAR(100), injury_description TEXT,",
        "    severity VARCHAR(20), injury_flag SMALLINT, injury_count INTEGER,",
        "    corrective_action TEXT, reference VARCHAR(100)",
        ");", "",
    ]
    for name, col in _DB_INDEXES:
        header.append(f"CREATE INDEX {name} ON safety_incidents ({col});")
    header.append("")

    inserts = []
    for _, row in out[_DB_COLUMNS].iterrows():
        vals = ", ".join(
            str(row[c]) if isinstance(row[c], (int, float)) else f"'{esc(row[c])}'"
            for c in _DB_COLUMNS
        )
        inserts.append(f"INSERT INTO safety_incidents VALUES ({vals});")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(header + inserts))
    log.info("Written: %s (%d records)", path, len(out))


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 10 — SOURCE REGISTRY + MAIN
# ══════════════════════════════════════════════════════════════════════════════
# To add a new source: implement fetch_<name>() and normalise_<name>(),
# add an entry to SOURCE_CONFIG, then append a tuple here.

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
    df["injury_flag"]  = df["injury_flag"].fillna(False).astype(bool)
    df["injury_count"] = pd.to_numeric(df["injury_count"], errors="coerce").fillna(0).astype(int)

    log.info("Combined dataset: %d records from %d source(s).", len(df), len(frames))

    write_json("meta.json",                    build_meta(df))
    write_json("by_category.json",             build_by_category(df))
    write_json("by_region.json",               build_by_region(df))
    write_json("by_country_origin.json",       build_by_country_origin(df))
    write_json("trends_monthly.json",          build_trends_monthly(df))
    write_json("recent_alerts.json",           build_recent_alerts(df))
    write_json("risk_breakdown.json",          build_risk_breakdown(df))
    write_json("by_hazard_type.json",          build_by_hazard_type(df))
    write_json("by_injury_type.json",          build_by_injury_type(df))
    write_json("by_severity.json",             build_by_severity(df))
    write_json("severity_trends_monthly.json", build_severity_trends_monthly(df))
    write_json("hazard_trends_monthly.json",   build_hazard_trends_monthly(df))
    write_json("injury_alerts.json",           build_injury_alerts(df))
    write_json("full_dataset.json",            df.fillna("").to_dict("records"))
    write_sqlite(df)
    write_sql_dump(df)

    log.info("=== Safety Dashboard Pipeline — DONE ===")


if __name__ == "__main__":
    run()
