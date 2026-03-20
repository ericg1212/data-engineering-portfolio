-- stg_orange_book.sql
-- Clean and type-cast Orange Book NCE exclusivity records.
-- Source: RAW.ORANGE_BOOK (loaded via COPY INTO from S3 Parquet)
--
-- Why filter to NCE only here: the raw table may contain other exclusivity
-- codes (e.g., ODE, NDF) that are not patent cliff proxies. NCE is the
-- binding constraint for small-molecule generic entry.

with source as (
    select * from {{ source('raw', 'orange_book') }}
),

nce_only as (
    select
        appl_type,
        appl_no,
        product_no,
        upper(trim(drug_name))         as drug_name,
        upper(trim(active_ingredient)) as active_ingredient,
        upper(trim(exclusivity_code))  as exclusivity_code,
        try_cast(exclusivity_date as date) as exclusivity_date,
        _loaded_at
    from source
    where exclusivity_code in ('NCE', 'NCE-1')
      and drug_name is not null
      and exclusivity_date is not null
)

select * from nce_only
