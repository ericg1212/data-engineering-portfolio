"""
Orange Book Extractor
Downloads FDA Orange Book ZIP, extracts exclusivity.txt + products.txt,
joins to get drug names, filters to NCE codes, writes Parquet.

Key facts learned from live API exploration (Mar 20 2026):
  - Download is a ZIP (not bare TSV) at /media/76860/download?attachment
  - ZIP contains: exclusivity.txt, patent.txt, products.txt (all ~-delimited)
  - exclusivity.txt has 5 cols: Appl_No, Product_No, Exclusivity_Code,
    Exclusivity_Date — NO drug name
  - Drug name (Trade_Name) and active ingredient come from products.txt,
    joined on (Appl_No, Product_No)
  - Applicant_Full_Name in products.txt can improve sponsor→ticker mapping in v2

Design: NCE exclusivity expiry (not patent expiry) is the binding constraint
for first generic entry. Orange Book NCE code = 5-year exclusivity for New
Chemical Entities. NCE-1 = 4-year exclusivity variant.
"""

import io
import logging
import zipfile

import pandas as pd
import requests

from pharma_patent_cliff.config import NCE_CODES, ORANGE_BOOK_URL, S3_PREFIXES
from pharma_patent_cliff.s3_utils import make_monthly_key, write_parquet

logger = logging.getLogger(__name__)

# Columns to keep from products.txt after join
PRODUCT_COLS = ["Appl_No", "Product_No", "Trade_Name", "Ingredient", "Applicant_Full_Name"]


def download_orange_book_zip() -> zipfile.ZipFile:
    """Download Orange Book ZIP and return as ZipFile object."""
    logger.info("Downloading Orange Book ZIP from FDA...")
    resp = requests.get(
        ORANGE_BOOK_URL,
        headers={"User-Agent": "pharma-patent-cliff-pipeline contact@example.com"},
        timeout=60,
    )
    resp.raise_for_status()
    logger.info("Downloaded %d bytes", len(resp.content))
    return zipfile.ZipFile(io.BytesIO(resp.content))


def _read_tsv_from_zip(z: zipfile.ZipFile, filename: str) -> pd.DataFrame:
    """Read a ~-delimited file from the ZIP into a DataFrame."""
    with z.open(filename) as f:
        content = f.read().decode("utf-8", errors="replace").replace("\r", "")
    df = pd.read_csv(io.StringIO(content), sep="~", dtype=str, on_bad_lines="skip")
    df.columns = [c.strip() for c in df.columns]
    return df


def filter_nce(excl: pd.DataFrame, prod: pd.DataFrame) -> pd.DataFrame:
    """
    Join exclusivity + products, filter to NCE codes, parse dates.
    Join key: Appl_No + Product_No (both present in both files).
    """
    # Filter exclusivity to NCE before join (performance)
    nce_excl = excl[excl["Exclusivity_Code"].isin(NCE_CODES)].copy()

    # Join to get drug name and active ingredient
    merged = nce_excl.merge(
        prod[PRODUCT_COLS],
        on=["Appl_No", "Product_No"],
        how="left",
    )

    # Parse and validate
    merged["Exclusivity_Date"] = pd.to_datetime(
        merged["Exclusivity_Date"], format="%b %d, %Y", errors="coerce"
    )
    merged = merged.dropna(subset=["Exclusivity_Date"])
    merged["Trade_Name"] = merged["Trade_Name"].str.strip().str.upper()
    merged["Ingredient"] = merged["Ingredient"].str.strip().str.upper()

    logger.info(
        "%d NCE records after filter+join (%d without drug name)",
        len(merged),
        merged["Trade_Name"].isna().sum(),
    )
    return merged


def run() -> str:
    """Full extract: download ZIP → join → filter NCE → write Parquet."""
    z = download_orange_book_zip()
    excl = _read_tsv_from_zip(z, "exclusivity.txt")
    prod = _read_tsv_from_zip(z, "products.txt")
    nce = filter_nce(excl, prod)
    s3_key = make_monthly_key(S3_PREFIXES["orange_book"], "orange_book.parquet")
    return write_parquet(nce, s3_key)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uri = run()
    print(f"Orange Book written to: {uri}")
