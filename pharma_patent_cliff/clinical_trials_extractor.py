"""
ClinicalTrials.gov Extractor
Queries the ClinicalTrials.gov REST API v2 for Phase III trials
by sponsor name for each company in scope.
Writes Parquet to S3 (or LOCAL_OUT for dev).

API docs: https://clinicaltrials.gov/data-api/api
"""

import logging
import time

import pandas as pd
import requests

from pharma_patent_cliff.config import (
    COMPANY_PIPELINE,
    CT_API_BASE,
    CT_PAGE_SIZE,
    CT_REQUEST_DELAY,
    S3_PREFIXES,
)
from pharma_patent_cliff.s3_utils import make_monthly_key, write_parquet

logger = logging.getLogger(__name__)


def _parse_study(study: dict, ticker: str) -> dict | None:
    """Extract relevant fields from a single ClinicalTrials study record."""
    try:
        proto = study.get("protocolSection", {})
        id_mod     = proto.get("identificationModule", {})
        status_mod = proto.get("statusModule", {})
        design_mod = proto.get("designModule", {})
        sponsor_mod = proto.get("sponsorCollaboratorsModule", {})

        phase_list = design_mod.get("phases", [])
        interventions = proto.get("armsInterventionsModule", {}).get("interventions", [])
        drug_name = next(
            (i.get("name") for i in interventions if i.get("type") == "DRUG"),
            id_mod.get("briefTitle", ""),
        )

        return {
            "ticker":          ticker,
            "nct_id":          id_mod.get("nctId"),
            "sponsor":         sponsor_mod.get("leadSponsor", {}).get("name"),
            "phase":           phase_list[0] if phase_list else None,
            "drug_name":       drug_name,
            "status":          status_mod.get("overallStatus"),
            "start_date":      status_mod.get("startDateStruct", {}).get("date"),
            "completion_date": status_mod.get("primaryCompletionDateStruct", {}).get("date"),
        }
    except Exception as exc:
        logger.debug("Could not parse study: %s", exc)
        return None


def fetch_trials_for_sponsor(sponsor_name: str, ticker: str) -> list[dict]:
    """Page through ClinicalTrials API for one sponsor name, Phase 3 only."""
    records: list[dict] = []
    params: dict = {
        "query.spons": sponsor_name,
        "filter.phase": "PHASE3",
        "pageSize": CT_PAGE_SIZE,
        "format": "json",
    }
    next_token: str | None = None

    while True:
        if next_token:
            params["pageToken"] = next_token
        resp = requests.get(CT_API_BASE, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        for study in data.get("studies", []):
            parsed = _parse_study(study, ticker)
            if parsed:
                records.append(parsed)

        next_token = data.get("nextPageToken")
        if not next_token:
            break
        time.sleep(CT_REQUEST_DELAY)

    logger.info("Fetched %d Phase III records for '%s' (%s)", len(records), sponsor_name, ticker)
    return records


def fetch_all_companies() -> pd.DataFrame:
    """Fetch trials for all companies; deduplicate by nct_id."""
    all_records: list[dict] = []
    seen_nct_ids: set[str] = set()

    for ticker, info in COMPANY_PIPELINE.items():
        for sponsor_name in info["sponsor_names"]:
            for r in fetch_trials_for_sponsor(sponsor_name, ticker):
                if r["nct_id"] not in seen_nct_ids:
                    seen_nct_ids.add(r["nct_id"])
                    all_records.append(r)

    df = pd.DataFrame(all_records)
    logger.info("Total unique Phase III records: %d", len(df))
    return df


def run() -> str:
    """Full extract: fetch all → deduplicate → write Parquet."""
    df = fetch_all_companies()
    if df.empty:
        logger.warning("No clinical trials data fetched — skipping write")
        return ""
    s3_key = make_monthly_key(S3_PREFIXES["clinical_trials"], "clinical_trials.parquet")
    return write_parquet(df, s3_key)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uri = run()
    print(f"Clinical trials written to: {uri}")
