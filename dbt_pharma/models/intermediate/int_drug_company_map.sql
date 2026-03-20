-- int_drug_company_map.sql
-- Join Orange Book drugs to ticker via hardcoded sponsor-to-ticker lookup.
--
-- Design rationale: fuzzy matching sponsor names → tickers is an NLP entity
-- resolution problem with zero analytical value for 7 known companies.
-- The lookup table is transparent and auditable. A production version would
-- use an NLP entity resolution layer (e.g., spaCy + embedding similarity).

with ob as (
    select * from {{ ref('stg_orange_book') }}
),

-- Hardcoded company lookup (mirrors COMPANY_PIPELINE in extractors)
company_lookup as (
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

-- Sponsor name patterns used for fuzzy match on drug_name / active_ingredient
-- This is a simplification: Orange Book doesn't have sponsor names directly,
-- so we map by known drug name prefixes and NCE applicant context.
-- For MVP: map all NCE drugs, join on ticker via manual drug→company assignments.
-- v2: integrate FDA applicant data for exact sponsor→company join.
mapped as (
    select
        ob.drug_name,
        ob.active_ingredient,
        ob.appl_no,
        ob.exclusivity_code,
        ob.exclusivity_date,
        -- Assign ticker based on known drug-to-company mappings
        case
            when ob.drug_name ilike '%keytruda%'    then 'MRK'
            when ob.drug_name ilike '%januvia%'     then 'MRK'
            when ob.drug_name ilike '%opdivo%'      then 'BMY'
            when ob.drug_name ilike '%eliquis%'     then 'BMY'
            when ob.drug_name ilike '%tagrisso%'    then 'AZN'
            when ob.drug_name ilike '%farxiga%'     then 'AZN'
            when ob.drug_name ilike '%mounjaro%'    then 'LLY'
            when ob.drug_name ilike '%trulicity%'   then 'LLY'
            when ob.drug_name ilike '%paxlovid%'    then 'PFE'
            when ob.drug_name ilike '%xeljanz%'     then 'PFE'
            when ob.drug_name ilike '%stelara%'     then 'JNJ'
            when ob.drug_name ilike '%darzalex%'    then 'JNJ'
            when ob.drug_name ilike '%humira%'      then 'ABBV'
            when ob.drug_name ilike '%skyrizi%'     then 'ABBV'
            when ob.drug_name ilike '%rinvoq%'      then 'ABBV'
            -- Novartis: Entresto + Kisqali are small-molecule NCE drugs in Orange Book
            -- Cosentyx (secukinumab) and Zolgensma are biologics — Purple Book only
            when ob.drug_name ilike '%entresto%'    then 'NVS'
            when ob.drug_name ilike '%kisqali%'     then 'NVS'
            else null  -- unmapped drugs excluded from analysis
        end as ticker
    from ob
),

final as (
    select
        m.drug_name,
        m.active_ingredient,
        m.appl_no,
        m.exclusivity_code,
        m.exclusivity_date,
        m.ticker,
        cl.company_name
    from mapped m
    inner join company_lookup cl on cl.ticker = m.ticker
    where m.ticker is not null
)

select * from final
