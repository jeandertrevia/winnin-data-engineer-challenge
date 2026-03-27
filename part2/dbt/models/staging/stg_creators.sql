with source as (
    select * from {{ source('raw', 'creators') }}
)

select
    channel_id,
    handle,
    title                        as channel_title,
    loaded_at
from source
