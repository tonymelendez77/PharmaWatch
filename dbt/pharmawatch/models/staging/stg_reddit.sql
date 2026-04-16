select *
from {{ source('pharmawatch_clean', 'reddit_mentions') }}
