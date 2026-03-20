"""
Shared S3 / local write utilities for the Pharma Patent Cliff pipeline.

All extractors call write_parquet() — it routes to S3 or a local directory
depending on the LOCAL_OUT environment variable. This keeps extractor logic
free of I/O branching and makes local dev possible before AWS is active.
"""

import io
import logging
import os
from pathlib import Path

import pandas as pd

from pharma_patent_cliff.config import LOCAL_OUT, S3_BUCKET

logger = logging.getLogger(__name__)


def write_parquet(
    df: pd.DataFrame,
    s3_key: str,
    bucket: str = S3_BUCKET,
    local_out: str | None = LOCAL_OUT,
) -> str:
    """
    Write df as Parquet (Snappy) to S3 or a local directory.

    Args:
        df:        DataFrame to write.
        s3_key:    Full S3 key, e.g. "raw/yfinance/ticker=MRK/year=2026/month=03/data.parquet".
                   Also used as the relative path under local_out when writing locally.
        bucket:    S3 bucket name (ignored when writing locally).
        local_out: Local root directory. If None, writes to S3.

    Returns:
        URI string: "s3://bucket/key" or "/abs/path/to/file.parquet"
    """
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow", compression="snappy")
    buffer.seek(0)

    if local_out:
        local_path = Path(local_out) / s3_key
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(buffer.getvalue())
        logger.info("Wrote %d rows to %s", len(df), local_path)
        return str(local_path)

    import boto3
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=s3_key, Body=buffer.getvalue())
    uri = f"s3://{bucket}/{s3_key}"
    logger.info("Wrote %d rows to %s", len(df), uri)
    return uri


def make_monthly_key(prefix: str, filename: str) -> str:
    """Build a Hive-partitioned S3 key for the current UTC month."""
    from datetime import UTC, datetime
    now = datetime.now(UTC)
    return f"{prefix}/year={now.year}/month={now.month:02d}/{filename}"


def make_ticker_key(prefix: str, ticker: str, filename: str = "data.parquet") -> str:
    """Build a ticker-partitioned, month-stamped S3 key."""
    from datetime import UTC, datetime
    now = datetime.now(UTC)
    return f"{prefix}/ticker={ticker}/year={now.year}/month={now.month:02d}/{filename}"
