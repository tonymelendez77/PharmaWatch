select *
from {{ source('pharmawatch_clean', 'pubmed_articles') }}
