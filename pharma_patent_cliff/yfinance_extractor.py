"""
yFinance Extractor
Pulls 5-year daily OHLCV + market cap for all tickers in COMPANY_PIPELINE,
writes one Parquet file per ticker to S3 (or LOCAL_OUT for dev).

Market cap is used as a revenue proxy for drawdown analysis.
Actual revenue from SEC EDGAR is planned for v2.
"""

import logging
from datetime import UTC, datetime, timedelta

import pandas as pd
import yfinance as yf

from pharma_patent_cliff.config import HISTORY_YEARS, S3_PREFIXES, TICKERS
from pharma_patent_cliff.s3_utils import make_ticker_key, write_parquet

logger = logging.getLogger(__name__)


def pull_ticker(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    Download OHLCV history for a single ticker.
    Adds market_cap column (close * shares_outstanding).
    Returns empty DataFrame if yFinance returns no data.
    """
    t = yf.Ticker(ticker)
    df = t.history(start=start, end=end, auto_adjust=True)
    if df.empty:
        logger.warning("No data returned for %s", ticker)
        return pd.DataFrame()

    df = df.reset_index()
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    df["ticker"] = ticker
    # Keep as datetime64[ns] — .dt.date produces object dtype in Parquet
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None).dt.normalize()

    # fast_info.shares = current shares outstanding (approximation for historical market cap)
    shares = getattr(t.fast_info, "shares", None)
    df["market_cap"] = (df["close"] * shares).round(0) if shares else None

    keep = ["ticker", "date", "open", "high", "low", "close", "volume", "market_cap"]
    return df[[c for c in keep if c in df.columns]]


def run(
    tickers: list[str] = TICKERS,
) -> dict[str, str]:
    """
    Pull all tickers and write each to S3 / LOCAL_OUT.
    Returns {ticker: uri}.
    """
    now = datetime.now(UTC)
    end = now.strftime("%Y-%m-%d")
    start = (now - timedelta(days=365 * HISTORY_YEARS)).strftime("%Y-%m-%d")
    logger.info("Pulling %d tickers from %s to %s", len(tickers), start, end)

    results: dict[str, str] = {}
    for ticker in tickers:
        try:
            df = pull_ticker(ticker, start, end)
            if not df.empty:
                s3_key = make_ticker_key(S3_PREFIXES["yfinance"], ticker)
                uri = write_parquet(df, s3_key)
                results[ticker] = uri
        except Exception as exc:
            logger.error("Failed to pull %s: %s", ticker, exc)

    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uris = run()
    for ticker, uri in uris.items():
        print(f"{ticker}: {uri}")
