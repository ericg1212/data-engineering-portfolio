"""
ClinicalTrials.gov Extractor
Queries the ClinicalTrials.gov REST API v2 for Phase III trials
by sponsor name for each company in scope.
Writes Parquet to S3 raw landing zone.

API docs: https://clinicaltrials.gov/data-api/api
"""

import io
import logging
import os
import time
from datetime import datetime

import boto3
import pandas as pd
import requests

logger = logging.getLogger(__name__)

CT_API_BASE = "https://clinicaltrials.gov/api/v2/studies"
CT_PAGE_SIZE = 100
CT_REQUEST_DELAY = 0.5  # seconds between pages — respect rate limits

COMPANY_PIPELINE = {
    "MRK":  {"company": "Merck",               "sponsor_names": ["Merck", "MSD"]},
    "BMY":  {"company": "Bristol-Myers Squibb", "sponsor_names": ["Bristol-Myers", "BMS"]},
    "AZN":  {"company": "AstraZeneca",          "sponsor_names": ["AstraZeneca"]},
    "LLY":  {"company": "Eli Lilly",            "sponsor_names": ["Eli Lilly", "Lilly"]},
    "PFE":  {"company": "Pfizer",               "sponsor_names": ["Pfizer"]},
    "JNJ":  {"company": "Johnson & Johnson",    "sponsor_names": ["Janssen", "Johnson"]},
    "ABBV": {"company": "AbbVie",               "sponsor_names": ["AbbVie"]},
}

S3_BUCKET = os.environ.get("S3_BUCKET", "data-engineering-portfolio-ericg")
S3_PREFIX = "raw/clinical_trials"


def _parse_study(study: dict, ticker: str) -> dict | None:
    """Extract relevant fields from a single ClinicalTrials study record."""
    try:
        proto = study.get("protocolSection", {})
        id_mod = proto.get("identificationModule", {})
        status_mod = proto.get("statusModule", {})
        design_mod = proto.get("designModule", {})
        sponsor_mod = proto.get("sponsorCollaboratorsModule", {})

        phase_list = design_mod.get("phases", [])
        phase = phase_list[0] if phase_list else None

        interventions = proto.get("armsInterventionsModule", {}).get("interventions", [])
        drug_name = next(
            (i.get("name") for i in interventions if i.get("type") == "DRUG"),
            id_mod.get("briefTitle", ""),
        )

        return {
            "ticker": ticker,
            "nct_id": id_mod.get("nctId"),
            "sponsor": sponsor_mod.get("leadSponsor", {}).get("name"),
            "phase": phase,
            "drug_name": drug_name,
            "status": status_mod.get("overallStatus"),
            "start_date": status_mod.get("startDateStruct", {}).get("date"),
            "completion_date": status_mod.get("primaryCompletionDateStruct", {}).get("date"),
        }
    except Exception as exc:
        logger.debug("Could not parse study: %s", exc)
        return None


def fetch_trials_for_sponsor(sponsor_name: str, ticker: str) -> list[dict]:
    """Page through ClinicalTrials API for one sponsor name, Phase 3 only."""
    records = []
    params = {
        "query.spons": sponsor_name,
        "filter.phase": "PHASE3",
        "pageSize": CT_PAGE_SIZE,
        "format": "json",
    }
    next_token = None

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

    logger.info(
        "Fetched %d Phase III records for sponsor '%s' (%s)",
        len(records), sponsor_name, ticker,
    )
    return records


def fetch_all_companies() -> pd.DataFrame:
    """Fetch trials for all companies; deduplicate by nct_id."""
    all_records: list[dict] = []
    seen_nct_ids: set[str] = set()

    for ticker, info in COMPANY_PIPELINE.items():
        for sponsor_name in info["sponsor_names"]:
            records = fetch_trials_for_sponsor(sponsor_name, ticker)
            for r in records:
                if r["nct_id"] not in seen_nct_ids:
                    seen_nct_ids.add(r["nct_id"])
                    all_records.append(r)

    df = pd.DataFrame(all_records)
    logger.info("Total unique Phase III records: %d", len(df))
    return df


def write_to_s3(df: pd.DataFrame, bucket: str, prefix: str) -> str:
    """Write clinical trials DataFrame as Parquet to S3."""
    now = datetime.utcnow()
    s3_key = f"{prefix}/year={now.year}/month={now.month:02d}/clinical_trials.parquet"

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=s3_key, Body=buffer.getvalue())
    s3_uri = f"s3://{bucket}/{s3_key}"
    logger.info("Wrote %d rows to %s", len(df), s3_uri)
    return s3_uri


def run(bucket: str = S3_BUCKET) -> str:
    """Full extract: fetch all → deduplicate → S3."""
    df = fetch_all_companies()
    if df.empty:
        logger.warning("No clinical trials data fetched — skipping S3 write")
        return ""
    uri = write_to_s3(df, bucket, S3_PREFIX)
    return uri


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uri = run()
    print(f"Clinical trials written to: {uri}")
