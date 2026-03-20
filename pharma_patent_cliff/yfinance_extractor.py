"""
yFinance Extractor
Pulls 5-year daily OHLCV + market cap for the 7 pharma tickers,
writes Parquet to S3 raw landing zone partitioned by ticker.

Market cap is used as a revenue proxy for drawdown analysis
(actual revenue requires 10-K parsing — deferred to v2).
"""

import io
import logging
import os
from datetime import datetime, timedelta

import boto3
import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

TICKERS = ["MRK", "BMY", "AZN", "LLY", "PFE", "JNJ", "ABBV"]
HISTORY_YEARS = 5

S3_BUCKET = os.environ.get("S3_BUCKET", "data-engineering-portfolio-ericg")
S3_PREFIX = "raw/yfinance"


def pull_ticker(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Download OHLCV history for a single ticker.
    Adds market_cap column (close * shares_outstanding).
    """
    t = yf.Ticker(ticker)
    df = t.history(start=start, end=end, auto_adjust=True)
    if df.empty:
        logger.warning("No data returned for %s", ticker)
        return pd.DataFrame()

    df = df.reset_index()
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    df["ticker"] = ticker

    # market_cap: shares_outstanding * close (snapshot approximation)
    info = t.fast_info
    shares = getattr(info, "shares_outstanding", None)
    df["market_cap"] = df["close"] * shares if shares else None

    # Normalize date column (timezone-naive date)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    keep = ["ticker", "date", "open", "high", "low", "close", "volume", "market_cap"]
    return df[[c for c in keep if c in df.columns]]


def write_ticker_to_s3(df: pd.DataFrame, ticker: str, bucket: str, prefix: str) -> str:
    """Write one ticker's data to S3 as Parquet."""
    now = datetime.utcnow()
    s3_key = (
        f"{prefix}/ticker={ticker}/year={now.year}/month={now.month:02d}/data.parquet"
    )
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=s3_key, Body=buffer.getvalue())
    s3_uri = f"s3://{bucket}/{s3_key}"
    logger.info("Wrote %d rows for %s to %s", len(df), ticker, s3_uri)
    return s3_uri


def run(
    tickers: list[str] = TICKERS,
    bucket: str = S3_BUCKET,
) -> dict[str, str]:
    """Pull all tickers and write each to S3. Returns {ticker: s3_uri}."""
    end = datetime.utcnow().strftime("%Y-%m-%d")
    start = (datetime.utcnow() - timedelta(days=365 * HISTORY_YEARS)).strftime(
        "%Y-%m-%d"
    )
    logger.info("Pulling %d tickers from %s to %s", len(tickers), start, end)

    results = {}
    for ticker in tickers:
        try:
            df = pull_ticker(ticker, start, end)
            if not df.empty:
                uri = write_ticker_to_s3(df, ticker, bucket, S3_PREFIX)
                results[ticker] = uri
        except Exception as exc:
            logger.error("Failed to pull %s: %s", ticker, exc)

    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uris = run()
    for ticker, uri in uris.items():
        print(f"{ticker}: {uri}")
