"""
Pharma Patent Cliff — Unit Tests
Tests extractor logic with mocked S3 and API calls.
Run: pytest tests/test_pharma_cliff.py -v
"""

import io
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# ── Orange Book Extractor ──────────────────────────────────────────────────────

class TestOrangeBookExtractor:
    """Tests for orange_book_extractor.py"""

    SAMPLE_TSV = (
        "Appl_Type~Appl_No~Product_No~Exclusivity_Code~Exclusivity_Date~Drug_Name~Active_Ingredient\n"
        "N~210736~001~NCE~Jan 01, 2028~KEYTRUDA~PEMBROLIZUMAB\n"
        "N~210736~002~ODE~Jan 01, 2028~KEYTRUDA~PEMBROLIZUMAB\n"  # non-NCE, should be filtered
        "N~210737~001~NCE-1~Mar 15, 2029~OPDIVO~NIVOLUMAB\n"
        "N~210738~001~NCE~Jun 30, 2025~BADROW~\n"  # null active_ingredient OK — drug_name non-null
    )

    def test_filter_nce_keeps_nce_codes(self):
        from pharma_patent_cliff.orange_book_extractor import filter_nce
        df = pd.read_csv(io.StringIO(self.SAMPLE_TSV), sep="~", dtype=str)
        result = filter_nce(df)
        # ODE row should be dropped
        assert set(result["Exclusivity_Code"].unique()) <= {"NCE", "NCE-1"}

    def test_filter_nce_drops_null_drug_name(self):
        from pharma_patent_cliff.orange_book_extractor import filter_nce
        tsv = (
            "Appl_Type~Appl_No~Product_No~Exclusivity_Code~Exclusivity_Date~Drug_Name~Active_Ingredient\n"
            "N~999~001~NCE~Jan 01, 2029~~SOMEINGREDIENT\n"  # null drug_name
        )
        df = pd.read_csv(io.StringIO(tsv), sep="~", dtype=str)
        result = filter_nce(df)
        assert len(result) == 0

    def test_filter_nce_parses_date(self):
        from pharma_patent_cliff.orange_book_extractor import filter_nce
        df = pd.read_csv(io.StringIO(self.SAMPLE_TSV), sep="~", dtype=str)
        result = filter_nce(df)
        assert pd.api.types.is_datetime64_any_dtype(result["Exclusivity_Date"])

    def test_filter_nce_uppercases_drug_name(self):
        from pharma_patent_cliff.orange_book_extractor import filter_nce
        tsv = (
            "Appl_Type~Appl_No~Product_No~Exclusivity_Code~Exclusivity_Date~Drug_Name~Active_Ingredient\n"
            "N~210736~001~NCE~Jan 01, 2028~keytruda~pembrolizumab\n"
        )
        df = pd.read_csv(io.StringIO(tsv), sep="~", dtype=str)
        result = filter_nce(df)
        assert result.iloc[0]["Drug_Name"] == "KEYTRUDA"

    @patch("pharma_patent_cliff.orange_book_extractor.boto3.client")
    def test_write_to_s3_calls_put_object(self, mock_boto):
        from pharma_patent_cliff.orange_book_extractor import write_to_s3
        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3

        df = pd.DataFrame({
            "Drug_Name": ["KEYTRUDA"],
            "Exclusivity_Date": [datetime(2028, 1, 1)],
        })
        uri = write_to_s3(df, "test-bucket", "raw/orange_book")

        assert mock_s3.put_object.called
        assert uri.startswith("s3://test-bucket/raw/orange_book/")

    @patch("pharma_patent_cliff.orange_book_extractor.requests.get")
    def test_download_raises_on_http_error(self, mock_get):
        from pharma_patent_cliff.orange_book_extractor import download_orange_book
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = Exception("404 Not Found")
        mock_get.return_value = mock_resp

        with pytest.raises(Exception, match="404"):
            download_orange_book()


# ── yFinance Extractor ─────────────────────────────────────────────────────────

class TestYFinanceExtractor:
    """Tests for yfinance_extractor.py"""

    def _make_sample_history(self, ticker: str = "MRK") -> pd.DataFrame:
        dates = pd.date_range("2024-01-01", periods=5, freq="B", tz="UTC")
        return pd.DataFrame({
            "Date": dates,
            "Open":  [100.0, 101.0, 102.0, 103.0, 104.0],
            "High":  [105.0, 106.0, 107.0, 108.0, 109.0],
            "Low":   [99.0,  100.0, 101.0, 102.0, 103.0],
            "Close": [102.0, 103.0, 104.0, 105.0, 106.0],
            "Volume":[1000000, 1100000, 1200000, 1300000, 1400000],
        }).set_index("Date")

    @patch("pharma_patent_cliff.yfinance_extractor.yf.Ticker")
    def test_pull_ticker_returns_expected_columns(self, mock_ticker_cls):
        from pharma_patent_cliff.yfinance_extractor import pull_ticker
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = self._make_sample_history()
        mock_ticker.fast_info.shares_outstanding = 2_000_000_000
        mock_ticker_cls.return_value = mock_ticker

        df = pull_ticker("MRK", "2024-01-01", "2024-12-31")

        assert "ticker" in df.columns
        assert "close" in df.columns
        assert "market_cap" in df.columns
        assert (df["close"] > 0).all()

    @patch("pharma_patent_cliff.yfinance_extractor.yf.Ticker")
    def test_pull_ticker_empty_history_returns_empty(self, mock_ticker_cls):
        from pharma_patent_cliff.yfinance_extractor import pull_ticker
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = pd.DataFrame()
        mock_ticker_cls.return_value = mock_ticker

        df = pull_ticker("FAKE", "2024-01-01", "2024-12-31")
        assert df.empty

    @patch("pharma_patent_cliff.yfinance_extractor.boto3.client")
    @patch("pharma_patent_cliff.yfinance_extractor.yf.Ticker")
    def test_run_writes_all_tickers(self, mock_ticker_cls, mock_boto):
        from pharma_patent_cliff.yfinance_extractor import run, TICKERS
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = self._make_sample_history()
        mock_ticker.fast_info.shares_outstanding = 1_000_000_000
        mock_ticker_cls.return_value = mock_ticker

        mock_s3 = MagicMock()
        mock_boto.return_value = mock_s3

        results = run(tickers=TICKERS, bucket="test-bucket")
        assert len(results) == len(TICKERS)  # TICKERS drives count — add NVS there, test stays correct
        assert mock_s3.put_object.call_count == len(TICKERS)


# ── Clinical Trials Extractor ──────────────────────────────────────────────────

class TestClinicalTrialsExtractor:
    """Tests for clinical_trials_extractor.py"""

    SAMPLE_API_RESPONSE = {
        "studies": [
            {
                "protocolSection": {
                    "identificationModule": {"nctId": "NCT00000001", "briefTitle": "Drug A Trial"},
                    "statusModule": {
                        "overallStatus": "RECRUITING",
                        "startDateStruct": {"date": "2023-01"},
                        "primaryCompletionDateStruct": {"date": "2026-12"},
                    },
                    "designModule": {"phases": ["PHASE3"]},
                    "sponsorCollaboratorsModule": {"leadSponsor": {"name": "Merck"}},
                    "armsInterventionsModule": {
                        "interventions": [{"type": "DRUG", "name": "Pembrolizumab"}]
                    },
                }
            }
        ],
        "nextPageToken": None,
    }

    @patch("pharma_patent_cliff.clinical_trials_extractor.requests.get")
    def test_fetch_trials_parses_record(self, mock_get):
        from pharma_patent_cliff.clinical_trials_extractor import fetch_trials_for_sponsor
        mock_resp = MagicMock()
        mock_resp.json.return_value = self.SAMPLE_API_RESPONSE
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        records = fetch_trials_for_sponsor("Merck", "MRK")
        assert len(records) == 1
        assert records[0]["nct_id"] == "NCT00000001"
        assert records[0]["ticker"] == "MRK"
        assert records[0]["drug_name"] == "Pembrolizumab"

    @patch("pharma_patent_cliff.clinical_trials_extractor.requests.get")
    def test_fetch_trials_deduplicates_by_nct_id(self, mock_get):
        from pharma_patent_cliff.clinical_trials_extractor import fetch_all_companies
        # Return same study for every call (simulates overlap across sponsor_names)
        mock_resp = MagicMock()
        mock_resp.json.return_value = self.SAMPLE_API_RESPONSE
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        df = fetch_all_companies()
        # NCT00000001 should appear only once despite multiple sponsor lookups
        assert df["nct_id"].value_counts().max() == 1


# ── Snowflake Loader ───────────────────────────────────────────────────────────

class TestSnowflakeLoader:
    """Tests for snowflake_loader.py"""

    @patch("pharma_patent_cliff.snowflake_loader.snowflake.connector.connect")
    def test_ensure_tables_exist_runs_ddl(self, mock_connect):
        from pharma_patent_cliff.snowflake_loader import ensure_tables_exist, DDL_STATEMENTS
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        ensure_tables_exist(mock_conn)
        # Should execute one DDL per table
        assert mock_cursor.execute.call_count == len(DDL_STATEMENTS)

    @patch("pharma_patent_cliff.snowflake_loader.snowflake.connector.connect")
    def test_copy_into_returns_row_count(self, mock_connect):
        from pharma_patent_cliff.snowflake_loader import copy_into
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # Simulate COPY INTO result: (file, status, rows_parsed, rows_loaded, ...)
        mock_cursor.fetchall.return_value = [
            ("file1.parquet", "LOADED", 100, 100, 0, 0, None, None, None, None)
        ]
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        rows = copy_into(mock_conn, "orange_book", "raw/orange_book/year=2025/month=03")
        assert rows == 100
