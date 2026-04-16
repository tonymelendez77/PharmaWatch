{{ config(materialized='table') }}

{% if target.type == 'snowflake' %}
select
    drug_name,
    count(*) as total_papers,
    avg(abstract_length) as avg_abstract_length,
    max(publish_year) as latest_publish_year,
    min(publish_year) as earliest_publish_year
from {{ ref('stg_pubmed') }}
where drug_name is not null
group by drug_name
{% elif target.type == 'bigquery' %}
select
    drug_name,
    count(*) as total_papers,
    avg(abstract_length) as avg_abstract_length,
    max(publish_year) as latest_publish_year,
    min(publish_year) as earliest_publish_year
from {{ ref('stg_pubmed') }}
where drug_name is not null
group by drug_name
{% endif %}
