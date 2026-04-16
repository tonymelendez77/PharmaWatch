select *
from {{ ref('drug_adverse_profile') }}
where serious_reports > total_reports
