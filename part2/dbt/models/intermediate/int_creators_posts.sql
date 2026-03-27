with creators as (
    select * from {{ ref('stg_creators') }}
),

posts as (
    select * from {{ ref('stg_posts') }}
)

select
    c.handle,
    c.channel_title,
    p.video_id,
    p.title,
    p.published_at,
    p.year_month,
    p.views,
    p.likes,
    p.tags
from posts p
inner join creators c on p.channel_id = c.channel_id
