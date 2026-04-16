{{ config(materialized='table') }}

with base as (
    select *
    from {{ ref('stg_reddit') }}
    where drug_mentions is not null
),

exploded as (
    {% if target.type == 'snowflake' %}
    select
        b.post_id,
        b.subreddit,
        b.score,
        b.body_length,
        trim(f.value::string) as drug_name
    from base b,
    lateral flatten(input => split(b.drug_mentions, ',')) f
    {% elif target.type == 'bigquery' %}
    select
        b.post_id,
        b.subreddit,
        b.score,
        b.body_length,
        trim(cast(mention as string)) as drug_name
    from base b
    cross join unnest(split(b.drug_mentions, ',')) as mention
    {% endif %}
)

select
    drug_name,
    count(*) as total_mentions,
    avg(score) as avg_score,
    count(distinct subreddit) as subreddit_count,
    avg(body_length) as avg_body_length
from exploded
where drug_name is not null and drug_name <> ''
group by drug_name
