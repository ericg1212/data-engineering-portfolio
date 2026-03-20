"""
Central config for the Pharma Patent Cliff pipeline.
Single source of truth for company list, tickers, S3 locations, EDGAR CIKs.

Import from here — never define these constants in individual extractors.
"""

import os

# ── Company universe ───────────────────────────────────────────────────────────
# sponsor_names: strings matched against ClinicalTrials.gov lead sponsor field
# Roche excluded: Ocrevus (key cliff drug) is a biologic → Purple Book, not Orange Book
# GSK/Sanofi/Novo Nordisk excluded: biggest cliff drugs are biologics (same reason)
COMPANY_PIPELINE: dict[str, dict] = {
    "MRK":  {"company": "Merck",               "sponsor_names": ["Merck", "MSD"]},
    "BMY":  {"company": "Bristol-Myers Squibb", "sponsor_names": ["Bristol-Myers", "BMS"]},
    "AZN":  {"company": "AstraZeneca",          "sponsor_names": ["AstraZeneca"]},
    "LLY":  {"company": "Eli Lilly",            "sponsor_names": ["Eli Lilly", "Lilly"]},
    "PFE":  {"company": "Pfizer",               "sponsor_names": ["Pfizer"]},
    "JNJ":  {"company": "Johnson & Johnson",    "sponsor_names": ["Janssen", "Johnson"]},
    "ABBV": {"company": "AbbVie",               "sponsor_names": ["AbbVie"]},
    "NVS":  {"company": "Novartis",             "sponsor_names": ["Novartis"]},
}

TICKERS: list[str] = list(COMPANY_PIPELINE.keys())

# ── EDGAR CIK map ──────────────────────────────────────────────────────────────
# CIK = SEC Central Index Key — zero-padded to 10 digits in API calls
# Source: https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company=...
# NVS files as Novartis AG (foreign private issuer on Form 20-F, not 10-K)
EDGAR_CIKS: dict[str, str] = {
    "MRK":  "0000310158",
    "BMY":  "0000014272",
    "AZN":  "0000901832",  # AstraZeneca PLC — verified Mar 20 2026 (0001474735 = Generac, wrong)
    "LLY":  "0000059478",
    "PFE":  "0000078003",
    "JNJ":  "0000200406",
    "ABBV": "0001551152",
    "NVS":  "0001114448",
}

# Foreign private issuers — file Form 20-F under IFRS, not 10-K under us-gaap
# Revenue concept also differs per issuer within IFRS
EDGAR_IFRS_FILERS: dict[str, dict] = {
    "AZN": {"form": "20-F", "revenue_concept": "Revenue"},
    "NVS": {"form": "20-F", "revenue_concept": "RevenueFromSaleOfGoods"},
}

# ── S3 / storage ───────────────────────────────────────────────────────────────
S3_BUCKET: str = os.environ.get("S3_BUCKET", "data-engineering-portfolio-ericg")

S3_PREFIXES: dict[str, str] = {
    "orange_book":     "raw/orange_book",
    "yfinance":        "raw/yfinance",
    "clinical_trials": "raw/clinical_trials",
    "edgar":           "raw/edgar",
}

# If set, write Parquet to this local directory instead of S3.
# Useful for local dev before AWS account is fully active.
# Usage: LOCAL_OUT=/tmp/pharma_raw python yfinance_extractor.py
LOCAL_OUT: str | None = os.environ.get("LOCAL_OUT")

# ── FDA Orange Book ────────────────────────────────────────────────────────────
# Orange Book exclusivity file — FDA updates monthly
ORANGE_BOOK_URL: str = "https://www.fda.gov/media/76860/download?attachment"
# The download is a ZIP containing: exclusivity.txt, patent.txt, products.txt
# exclusivity.txt has 5 cols (no drug name) — must join with products.txt on Appl_No+Product_No
# products.txt has Trade_Name, Ingredient, Applicant_Full_Name (useful for sponsor matching in v2)
NCE_CODES: frozenset[str] = frozenset({"NCE", "NCE-1"})

# ── ClinicalTrials.gov API ─────────────────────────────────────────────────────
CT_API_BASE: str = "https://clinicaltrials.gov/api/v2/studies"
CT_PAGE_SIZE: int = 100
CT_REQUEST_DELAY: float = 0.5  # seconds between pages

# ── yFinance ───────────────────────────────────────────────────────────────────
HISTORY_YEARS: int = 5

# ── EDGAR API ─────────────────────────────────────────────────────────────────
EDGAR_API_BASE: str = "https://data.sec.gov/api/xbrl/companyfacts"
EDGAR_REQUEST_DELAY: float = 0.1  # SEC fair-use: max 10 req/sec
EDGAR_USER_AGENT: str = os.environ.get(
    "EDGAR_USER_AGENT",
    "pharma-patent-cliff-pipeline contact@example.com",  # SEC requires User-Agent
)
# XBRL concepts to pull for revenue/R&D analysis
# us-gaap labels vary by company — pull all, filter in dbt
EDGAR_REVENUE_CONCEPTS: list[str] = [
    "Revenues",
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "SalesRevenueNet",
]
EDGAR_RD_CONCEPTS: list[str] = [
    "ResearchAndDevelopmentExpense",
    "ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost",
]
