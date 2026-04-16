{{ config(materialized='table') }}

with base as (
    select *
    from {{ ref('stg_faers') }}
),

reaction_counts as (
    select
        drug_name,
        reaction,
        count(*) as reaction_cnt,
        row_number() over (partition by drug_name order by count(*) desc) as rn
    from base
    where reaction is not null
    group by drug_name, reaction
),

top_reaction as (
    {% if target.type == 'snowflake' %}
    select
        drug_name,
        (reaction)::string as most_common_reaction
    from reaction_counts
    where rn = 1
    {% elif target.type == 'bigquery' %}
    select
        drug_name,
        cast(reaction as string) as most_common_reaction
    from reaction_counts
    where rn = 1
    {% endif %}
),

age_group_counts as (
    select
        drug_name,
        age_group,
        count(*) as ag_cnt
    from base
    group by drug_name, age_group
),

age_group_agg as (
    {% if target.type == 'snowflake' %}
    select
        drug_name,
        object_agg(age_group, ag_cnt::variant)::string as age_group_distribution
    from age_group_counts
    group by drug_name
    {% elif target.type == 'bigquery' %}
    select
        drug_name,
        to_json_string(array_agg(struct(age_group, ag_cnt))) as age_group_distribution
    from age_group_counts
    group by drug_name
    {% endif %}
),

metrics as (
    select
        drug_name,
        count(*) as total_reports,
        sum(case when is_serious then 1 else 0 end) as serious_reports,
        sum(case when hospitalization = 1 then 1 else 0 end) as hospitalization_reports,
        sum(case when death = 1 then 1 else 0 end) as death_reports,
        sum(case when disability = 1 then 1 else 0 end) as disability_reports,
        avg(age) as avg_age
    from base
    group by drug_name
)

select
    m.drug_name,
    m.total_reports,
    m.serious_reports,
    m.hospitalization_reports,
    m.death_reports,
    m.disability_reports,
    m.avg_age,
    tr.most_common_reaction,
    aga.age_group_distribution
from metrics m
left join top_reaction tr on m.drug_name = tr.drug_name
left join age_group_agg aga on m.drug_name = aga.drug_name
