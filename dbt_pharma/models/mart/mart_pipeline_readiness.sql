-- mart_pipeline_readiness.sql
-- Final output for Page 3 (Pipeline Readiness).
-- One row per company: cliff drug count, Phase III pipeline count,
-- and readiness score (phase3_drugs / cliff_drugs, bounded [0, 1]).
--
-- Readiness score interpretation:
--   > 1.0 → pipeline exceeds cliff exposure (green)  — bounded at 1.0
--   0.5-1.0 → partial coverage (yellow)
--   < 0.5 → under-covered (red)
--
-- Note on biologics: Roche, Novartis (Cosentyx, Zolgensma), and other
-- biologic drugs use BLA exclusivity tracked in the FDA Purple Book, not
-- the Orange Book. This model only captures small-molecule NCE cliff exposure.
-- Novartis is included because Entresto and Kisqali are Orange Book drugs.

with cliff as (
    select
        ticker,
        company_name,
        count(distinct drug_name) as cliff_drug_count
    from {{ ref('int_cliff_events') }}
    -- Count only near-term and imminent cliffs (≤5 years) plus past cliffs
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

-- Ensure all 8 companies appear even if they have no cliff or no pipeline data
all_tickers as (
    select * from (values
        ('MRK',  'Merck'),
        ('BMY',  'Bristol-Myers Squibb'),
        ('AZN',  'AstraZeneca'),
        ('LLY',  'Eli Lilly'),
        ('PFE',  'Pfizer'),
        ('JNJ',  'Johnson & Johnson'),
        ('ABBV', 'AbbVie'),
        ('NVS',  'Novartis')
    ) as t(ticker, company_name)
),

-- Compute raw score once to avoid duplicating threshold logic
scored as (
    select
        at.ticker,
        at.company_name,
        coalesce(c.cliff_drug_count, 0)  as cliff_drug_count,
        coalesce(p.phase3_drug_count, 0) as phase3_drug_count,
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
        end as readiness_score
    from all_tickers at
    left join cliff c using (ticker)
    left join pipeline p using (ticker)
),

final as (
    select
        ticker,
        company_name,
        cliff_drug_count,
        phase3_drug_count,
        readiness_score,
        -- Single source of truth for thresholds — change here, applies everywhere
        case
            when readiness_score >= 1.0 then 'green'
            when readiness_score >= 0.5 then 'yellow'
            else 'red'
        end as readiness_flag
    from scored
)

select * from final
order by readiness_score desc
