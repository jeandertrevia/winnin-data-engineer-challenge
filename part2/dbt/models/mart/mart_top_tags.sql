with exploded as (
    select
        handle,
        lower(trim(unnest(tags))) as tag
    from {{ ref('int_creators_posts') }}
    where tags is not null
        and array_length(tags, 1) > 0
),

counted as (
    select
        handle,
        tag,
        count(*) as total
    from exploded
    group by handle, tag
),

ranked as (
    select
        handle,
        tag,
        total,
        rank() over (partition by handle order by total desc) as rank
    from counted
)

select
    handle,
    tag,
    total,
    rank
from ranked
where rank <= 3
order by handle, rank
