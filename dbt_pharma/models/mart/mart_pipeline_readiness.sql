-- mart_pipeline_readiness.sql
-- Final output for Page 3 (Pipeline Readiness).
-- One row per company: cliff drug count, Phase III pipeline count,
-- and readiness score (phase3_drugs / cliff_drugs, bounded [0, 1]).
--
-- Readiness score interpretation:
--   > 1.0 → pipeline exceeds cliff exposure (green)
--   0.5-1.0 → partial coverage (yellow)
--   < 0.5 → under-covered (red)
-- Bounded at 1.0 because >1x pipeline doesn't eliminate cliff risk.

with cliff as (
    select
        ticker,
        company_name,
        count(distinct drug_name) as cliff_drug_count
    from {{ ref('int_cliff_events') }}
    -- Count only near-term and imminent cliffs (≤5 years)
    where cliff_category in ('imminent', 'near_term', 'past')
    group by 1, 2
),

pipeline as (
    select
        ticker,
        count(distinct drug_name) as phase3_drug_count
    from {{ ref('stg_clinical_trials') }}
    -- Active or recruiting Phase III trials only
    where status in ('RECRUITING', 'ACTIVE_NOT_RECRUITING', 'ENROLLING_BY_INVITATION')
    group by 1
),

-- Ensure all 7 companies appear even if they have no cliff or no pipeline
all_tickers as (
    select * from (values
        ('MRK',  'Merck'),
        ('BMY',  'Bristol-Myers Squibb'),
        ('AZN',  'AstraZeneca'),
        ('LLY',  'Eli Lilly'),
        ('PFE',  'Pfizer'),
        ('JNJ',  'Johnson & Johnson'),
        ('ABBV', 'AbbVie')
    ) as t(ticker, company_name)
),

final as (
    select
        at.ticker,
        at.company_name,
        coalesce(c.cliff_drug_count, 0)   as cliff_drug_count,
        coalesce(p.phase3_drug_count, 0)  as phase3_drug_count,
        -- Readiness score: bounded [0, 1]
        case
            when coalesce(c.cliff_drug_count, 0) = 0 then 1.0
            else least(
                round(
                    coalesce(p.phase3_drug_count, 0)::float
                    / nullif(c.cliff_drug_count, 0),
                    3
                ),
                1.0
            )
        end as readiness_score,
        -- Traffic light classification
        case
            when coalesce(c.cliff_drug_count, 0) = 0 then 'green'
            when least(
                coalesce(p.phase3_drug_count, 0)::float / nullif(c.cliff_drug_count, 0),
                1.0
            ) >= 1.0 then 'green'
            when least(
                coalesce(p.phase3_drug_count, 0)::float / nullif(c.cliff_drug_count, 0),
                1.0
            ) >= 0.5 then 'yellow'
            else 'red'
        end as readiness_flag
    from all_tickers at
    left join cliff c using (ticker)
    left join pipeline p using (ticker)
)

select * from final
order by readiness_score desc
