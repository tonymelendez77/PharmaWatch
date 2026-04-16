select *
from {{ source('pharmawatch_clean', 'faers_events') }}
