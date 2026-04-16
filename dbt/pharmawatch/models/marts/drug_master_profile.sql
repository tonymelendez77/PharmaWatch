{{ config(materialized='table') }}

select
    a.drug_name,
    a.total_reports as adv_total_reports,
    a.serious_reports as adv_serious_reports,
    a.hospitalization_reports as adv_hospitalization_reports,
    a.death_reports as adv_death_reports,
    a.disability_reports as adv_disability_reports,
    a.avg_age as adv_avg_age,
    a.most_common_reaction as adv_most_common_reaction,
    a.age_group_distribution as adv_age_group_distribution,
    coalesce(s.total_mentions, 0) as sent_total_mentions,
    s.avg_score as sent_avg_score,
    s.subreddit_count as sent_subreddit_count,
    s.avg_body_length as sent_avg_body_length,
    r.total_papers as res_total_papers,
    r.avg_abstract_length as res_avg_abstract_length,
    r.latest_publish_year as res_latest_publish_year,
    r.earliest_publish_year as res_earliest_publish_year,
    l.drug_id as lbl_drug_id,
    l.brand_name as lbl_brand_name,
    l.generic_name as lbl_generic_name,
    l.warnings as lbl_warnings,
    l.interactions as lbl_interactions,
    l.has_interactions as lbl_has_interactions,
    l.warnings_length as lbl_warnings_length,
    l.ingestion_ts as lbl_ingestion_ts
from {{ ref('drug_adverse_profile') }} a
left join {{ ref('drug_sentiment_profile') }} s on a.drug_name = s.drug_name
left join {{ ref('drug_research_profile') }} r on a.drug_name = r.drug_name
left join {{ ref('stg_drug_labels') }} l on a.drug_name = l.brand_name
