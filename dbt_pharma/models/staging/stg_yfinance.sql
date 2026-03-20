-- stg_yfinance.sql
-- Clean and type-cast yFinance OHLCV + market cap data.
-- Source: RAW.YFINANCE (loaded via COPY INTO from S3 Parquet)
--
-- Market cap is a revenue proxy (actual revenue requires 10-K parsing,
-- deferred to v2). Null market_cap rows are kept — they still contribute
-- to drawdown and OHLCV analysis.

with source as (
    select * from {{ source('raw', 'yfinance') }}
),

cleaned as (
    select
        upper(trim(ticker))        as ticker,
        try_cast(date as date)     as date,
        try_cast(open as float)    as open,
        try_cast(high as float)    as high,
        try_cast(low as float)     as low,
        try_cast(close as float)   as close,
        try_cast(volume as bigint) as volume,
        try_cast(market_cap as float) as market_cap,
        _loaded_at
    from source
    where ticker is not null
      and date is not null
      -- Basic sanity: prices must be positive
      and try_cast(close as float) > 0
      and try_cast(open as float) > 0
      -- No future dates
      and try_cast(date as date) <= current_date()
)

select * from cleaned
