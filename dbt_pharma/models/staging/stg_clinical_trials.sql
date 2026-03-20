-- stg_clinical_trials.sql
-- Clean and type-cast ClinicalTrials.gov Phase III trial records.
-- Source: RAW.CLINICAL_TRIALS (loaded via COPY INTO from S3 Parquet)
--
-- Only Phase III records were fetched at extract time, but we re-assert
-- the filter here for defensive staging hygiene.

with source as (
    select * from {{ source('raw', 'clinical_trials') }}
),

cleaned as (
    select
        upper(trim(ticker))           as ticker,
        upper(trim(nct_id))           as nct_id,
        trim(sponsor)                 as sponsor,
        upper(trim(phase))            as phase,
        trim(drug_name)               as drug_name,
        upper(trim(status))           as status,
        try_cast(start_date as date)  as start_date,
        try_cast(completion_date as date) as completion_date,
        _loaded_at
    from source
    where nct_id is not null
      and ticker is not null
      and phase = 'PHASE3'
)

select * from cleaned
