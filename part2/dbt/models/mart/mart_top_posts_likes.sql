with ranked as (
    select
        handle,
        title,
        likes,
        published_at,
        rank() over (partition by handle order by likes desc) as rank
    from {{ ref('int_creators_posts') }}
)

select
    handle,
    title,
    likes,
    published_at,
    rank
from ranked
where rank <= 3
order by handle, rank
