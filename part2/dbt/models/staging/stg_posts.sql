with source as (
    select * from {{ source('raw', 'posts') }}
)

select
    video_id,
    channel_id,
    title,
    published_at,
    views,
    likes,
    tags,
    to_char(published_at, 'YYYY/MM') as year_month,
    loaded_at
from source
where published_at is not null
