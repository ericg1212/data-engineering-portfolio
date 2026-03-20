"""
Snowflake Loader
COPY INTO helpers for loading S3 Parquet files into Snowflake RAW schema.

Pattern: S3 → Snowflake COPY INTO (production-standard ingestion).
S3 stays as raw landing zone; Snowflake handles transform + query.
Credentials sourced from environment variables — never hardcoded.
"""

import logging
import os

import snowflake.connector

logger = logging.getLogger(__name__)

# --- Connection config (from environment / Airflow Variables) ---
SNOWFLAKE_CONFIG = {
    "account":   os.environ.get("SNOWFLAKE_ACCOUNT"),
    "user":      os.environ.get("SNOWFLAKE_USER"),
    "password":  os.environ.get("SNOWFLAKE_PASSWORD"),
    "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database":  os.environ.get("SNOWFLAKE_DATABASE", "PHARMA_CLIFF"),
    "schema":    "RAW",
    "role":      os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
}

S3_BUCKET = os.environ.get("S3_BUCKET", "data-engineering-portfolio-ericg")
S3_STAGE  = os.environ.get("SNOWFLAKE_S3_STAGE", "pharma_s3_stage")  # pre-created stage

# DDL for RAW tables — run once on first deploy
DDL_STATEMENTS = {
    "orange_book": """
        CREATE TABLE IF NOT EXISTS RAW.ORANGE_BOOK (
            appl_type         VARCHAR,
            appl_no           VARCHAR,
            product_no        VARCHAR,
            exclusivity_code  VARCHAR,
            exclusivity_date  DATE,
            drug_name         VARCHAR,
            active_ingredient VARCHAR,
            _loaded_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """,
    "yfinance": """
        CREATE TABLE IF NOT EXISTS RAW.YFINANCE (
            ticker      VARCHAR,
            date        DATE,
            open        FLOAT,
            high        FLOAT,
            low         FLOAT,
            close       FLOAT,
            volume      BIGINT,
            market_cap  FLOAT,
            _loaded_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """,
    "clinical_trials": """
        CREATE TABLE IF NOT EXISTS RAW.CLINICAL_TRIALS (
            ticker           VARCHAR,
            nct_id           VARCHAR,
            sponsor          VARCHAR,
            phase            VARCHAR,
            drug_name        VARCHAR,
            status           VARCHAR,
            start_date       DATE,
            completion_date  DATE,
            _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """,
}

# COPY INTO templates — %s placeholders: stage, s3_path, table
COPY_TEMPLATE = """
    COPY INTO RAW.{table}
    FROM @{stage}/{s3_path}/
    FILE_FORMAT = (TYPE = 'PARQUET' SNAPPY_COMPRESSION = TRUE)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = CONTINUE
    PURGE = FALSE
"""


def get_connection() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


def ensure_tables_exist(conn: snowflake.connector.SnowflakeConnection) -> None:
    """Create RAW tables if they don't exist (idempotent)."""
    with conn.cursor() as cur:
        for table, ddl in DDL_STATEMENTS.items():
            logger.info("Ensuring table RAW.%s exists", table.upper())
            cur.execute(ddl)


def copy_into(
    conn: snowflake.connector.SnowflakeConnection,
    table: str,
    s3_path: str,
) -> int:
    """
    Run COPY INTO for one table from S3 path.
    Returns row count loaded.
    """
    sql = COPY_TEMPLATE.format(table=table.upper(), stage=S3_STAGE, s3_path=s3_path)
    logger.info("COPY INTO RAW.%s from %s", table.upper(), s3_path)
    with conn.cursor() as cur:
        cur.execute(sql)
        result = cur.fetchall()
        rows_loaded = sum(r[3] for r in result if r[3])  # rows_loaded column
        logger.info("Loaded %d rows into RAW.%s", rows_loaded, table.upper())
        return rows_loaded


def load_orange_book(s3_path: str | None = None) -> int:
    """Load latest Orange Book Parquet into RAW.ORANGE_BOOK."""
    from datetime import datetime
    now = datetime.utcnow()
    path = s3_path or f"raw/orange_book/year={now.year}/month={now.month:02d}"
    with get_connection() as conn:
        ensure_tables_exist(conn)
        return copy_into(conn, "orange_book", path)


def load_yfinance(s3_path: str | None = None) -> int:
    """Load yFinance Parquet files into RAW.YFINANCE (all tickers)."""
    from datetime import datetime
    now = datetime.utcnow()
    path = s3_path or f"raw/yfinance"
    with get_connection() as conn:
        ensure_tables_exist(conn)
        return copy_into(conn, "yfinance", path)


def load_clinical_trials(s3_path: str | None = None) -> int:
    """Load Clinical Trials Parquet into RAW.CLINICAL_TRIALS."""
    from datetime import datetime
    now = datetime.utcnow()
    path = s3_path or f"raw/clinical_trials/year={now.year}/month={now.month:02d}"
    with get_connection() as conn:
        ensure_tables_exist(conn)
        return copy_into(conn, "clinical_trials", path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    n = load_orange_book()
    print(f"Orange Book: {n} rows loaded")
    n = load_yfinance()
    print(f"yFinance: {n} rows loaded")
    n = load_clinical_trials()
    print(f"Clinical Trials: {n} rows loaded")
