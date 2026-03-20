"""
Orange Book Extractor
Downloads FDA Orange Book NCE exclusivity data, filters to NCE codes,
writes Parquet to S3 raw landing zone.

Design: NCE exclusivity expiry (not patent expiry) is the binding constraint
for generic entry. When NCE exclusivity expires, generics can file immediately.
"""

import io
import logging
import os
from datetime import datetime

import boto3
import pandas as pd
import requests

logger = logging.getLogger(__name__)

ORANGE_BOOK_URL = (
    "https://www.fda.gov/media/76860/download"  # exclusivity.txt (TSV)
)

# NCE = New Chemical Entity exclusivity — the primary cliff proxy
NCE_CODES = {"NCE", "NCE-1"}

S3_BUCKET = os.environ.get("S3_BUCKET", "data-engineering-portfolio-ericg")
S3_PREFIX = "raw/orange_book"

# Canonical column names after parsing FDA TSV
EXPECTED_COLS = [
    "Appl_Type",
    "Appl_No",
    "Product_No",
    "Exclusivity_Code",
    "Exclusivity_Date",
    "Drug_Name",
    "Active_Ingredient",
]


def download_orange_book() -> pd.DataFrame:
    """Download Orange Book exclusivity TSV and return as DataFrame."""
    logger.info("Downloading Orange Book exclusivity data from FDA...")
    resp = requests.get(ORANGE_BOOK_URL, timeout=60)
    resp.raise_for_status()

    # FDA file is pipe-delimited with a header row
    df = pd.read_csv(
        io.StringIO(resp.text),
        sep="~",
        dtype=str,
        on_bad_lines="skip",
    )
    df.columns = [c.strip() for c in df.columns]
    logger.info("Downloaded %d rows from Orange Book", len(df))
    return df


def filter_nce(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only NCE exclusivity records.
    - Filter Exclusivity_Code to NCE set
    - Parse Exclusivity_Date to date
    - Drop records with null drug name or expiry date
    """
    df = df[df["Exclusivity_Code"].isin(NCE_CODES)].copy()
    df["Exclusivity_Date"] = pd.to_datetime(
        df["Exclusivity_Date"], format="%b %d, %Y", errors="coerce"
    )
    df = df.dropna(subset=["Drug_Name", "Exclusivity_Date"])
    df["Drug_Name"] = df["Drug_Name"].str.strip().str.upper()
    logger.info("%d NCE records after filtering", len(df))
    return df


def write_to_s3(df: pd.DataFrame, bucket: str, prefix: str) -> str:
    """Write DataFrame as Parquet to S3 with Hive partitioning by year/month."""
    now = datetime.utcnow()
    s3_key = f"{prefix}/year={now.year}/month={now.month:02d}/orange_book.parquet"

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=s3_key, Body=buffer.getvalue())
    s3_uri = f"s3://{bucket}/{s3_key}"
    logger.info("Wrote %d rows to %s", len(df), s3_uri)
    return s3_uri


def run(bucket: str = S3_BUCKET) -> str:
    """Full extract: download → filter → S3."""
    raw = download_orange_book()
    nce = filter_nce(raw)
    uri = write_to_s3(nce, bucket, S3_PREFIX)
    return uri


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uri = run()
    print(f"Orange Book written to: {uri}")
