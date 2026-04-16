select *
from {{ source('pharmawatch_clean', 'drug_labels') }}
