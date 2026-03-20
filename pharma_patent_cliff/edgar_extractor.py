"""
SEC EDGAR Extractor
Pulls annual revenue + R&D expense for all 8 pharma companies via the
EDGAR XBRL company facts API. Writes Parquet to S3 (or LOCAL_OUT for dev).

Key design decisions learned from live API exploration (Mar 20 2026):
  1. Revenue concept varies by company — no single XBRL label works for all 8.
     Pull all candidate concepts; in dbt use COALESCE ordered by recency.
  2. BMY and ABBV file under `Revenues` for recent years (not SalesRevenueNet).
     Always rank by most recent 10-K end date, not by count of historical filings.
  3. NVS (Novartis) is a foreign private issuer — files Form 20-F under IFRS,
     not us-gaap. Revenue concept is ifrs-full/RevenueFromSaleOfGoods (USD).
  4. SEC fair-use limit: ≤10 req/sec. EDGAR_REQUEST_DELAY = 0.1s between calls.
     User-Agent header required — SEC blocks requests without it.

EDGAR API: https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json
"""

import logging
import time

import pandas as pd
import requests

from pharma_patent_cliff.config import (
    EDGAR_API_BASE,
    EDGAR_CIKS,
    EDGAR_IFRS_FILERS,
    EDGAR_RD_CONCEPTS,
    EDGAR_REQUEST_DELAY,
    EDGAR_REVENUE_CONCEPTS,
    EDGAR_USER_AGENT,
    S3_PREFIXES,
)
from pharma_patent_cliff.s3_utils import make_monthly_key, write_parquet

logger = logging.getLogger(__name__)

DEFAULT_FORM_TYPE = "10-K"

# How many years of annual history to keep
ANNUAL_HISTORY_YEARS = 10


def _fetch_company_facts(cik: str) -> dict:
    """Fetch the full XBRL facts JSON for one company from EDGAR."""
    url = f"{EDGAR_API_BASE}/CIK{cik}.json"
    resp = requests.get(
        url,
        headers={"User-Agent": EDGAR_USER_AGENT},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _extract_annual_series(
    facts: dict,
    concepts: list[str],
    namespace: str = "us-gaap",
    form_type: str = DEFAULT_FORM_TYPE,
    currency: str = "USD",
) -> list[dict]:
    """
    Extract annual values for a list of XBRL concepts.
    Returns all entries, preserving concept label for downstream COALESCE in dbt.
    Deduplicates by (concept, end_date) keeping the latest filing.
    """
    ns_facts = facts.get("facts", {}).get(namespace, {})
    rows: list[dict] = []
    seen: set[tuple] = set()

    for concept in concepts:
        entries = (
            ns_facts
            .get(concept, {})
            .get("units", {})
            .get(currency, [])
        )
        annual = [e for e in entries if e.get("form") == form_type]
        # Sort by end then filed so latest amendment wins
        annual.sort(key=lambda x: (x["end"], x.get("filed", "")))

        for e in annual:
            key = (concept, e["end"])
            if key not in seen:
                seen.add(key)
                rows.append({
                    "concept":    concept,
                    "end_date":   e["end"],
                    "value_usd":  e["val"],
                    "filed":      e.get("filed"),
                    "accn":       e.get("accn"),  # accession number for traceability
                    "form":       e.get("form"),
                })

    return rows


def _build_company_df(ticker: str, cik: str) -> pd.DataFrame:
    """
    Fetch and parse annual revenue + R&D for one company.
    Returns a tidy DataFrame with one row per (ticker, concept, end_date).

    IFRS filers (AZN, NVS) use ifrs-full namespace and Form 20-F.
    Each IFRS filer has its own revenue concept — defined in EDGAR_IFRS_FILERS.
    R&D for IFRS filers is deferred to v2 (IFRS labels vary more than us-gaap).
    """
    logger.info("Fetching EDGAR facts for %s (CIK %s)", ticker, cik)
    facts = _fetch_company_facts(cik)

    ifrs_cfg = EDGAR_IFRS_FILERS.get(ticker)
    is_ifrs = ifrs_cfg is not None
    namespace = "ifrs-full" if is_ifrs else "us-gaap"
    form_type = ifrs_cfg["form"] if is_ifrs else DEFAULT_FORM_TYPE

    revenue_concepts = [ifrs_cfg["revenue_concept"]] if is_ifrs else EDGAR_REVENUE_CONCEPTS
    rd_concepts = [] if is_ifrs else EDGAR_RD_CONCEPTS

    rows: list[dict] = []
    for metric, concepts in [("revenue", revenue_concepts), ("rd_expense", rd_concepts)]:
        series = _extract_annual_series(
            facts, concepts, namespace=namespace, form_type=form_type
        )
        for r in series:
            r["ticker"] = ticker
            r["metric"] = metric
            rows.append(r)

    df = pd.DataFrame(rows)
    if df.empty:
        logger.warning("No annual data found for %s", ticker)
        return df

    # Keep only ANNUAL_HISTORY_YEARS of data
    from datetime import UTC, datetime
    cutoff = str(datetime.now(UTC).year - ANNUAL_HISTORY_YEARS)
    df = df[df["end_date"] >= cutoff].copy()
    df["end_date"] = pd.to_datetime(df["end_date"])

    logger.info(
        "%s: %d rows (%d revenue, %d R&D)",
        ticker,
        len(df),
        len(df[df["metric"] == "revenue"]),
        len(df[df["metric"] == "rd_expense"]),
    )
    return df


def fetch_all_companies() -> pd.DataFrame:
    """Fetch annual revenue + R&D for all 8 companies."""
    frames: list[pd.DataFrame] = []
    for ticker, cik in EDGAR_CIKS.items():
        try:
            df = _build_company_df(ticker, cik)
            if not df.empty:
                frames.append(df)
        except Exception as exc:
            logger.error("Failed to fetch EDGAR data for %s: %s", ticker, exc)
        time.sleep(EDGAR_REQUEST_DELAY)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    logger.info("Total EDGAR rows fetched: %d", len(combined))
    return combined


def run() -> str:
    """Full extract: fetch all companies → write Parquet."""
    df = fetch_all_companies()
    if df.empty:
        logger.warning("No EDGAR data fetched — skipping write")
        return ""
    s3_key = make_monthly_key(S3_PREFIXES["edgar"], "edgar_financials.parquet")
    return write_parquet(df, s3_key)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uri = run()
    print(f"EDGAR financials written to: {uri}")
