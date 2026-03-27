with base as (
    select
        handle,
        year_month,
        count(*) as publicacoes
    from {{ ref('int_creators_posts') }}
    group by handle, year_month
),

all_months as (
    select distinct year_month
    from {{ ref('int_creators_posts') }}
),

all_creators as (
    select distinct handle
    from {{ ref('int_creators_posts') }}
),

combinations as (
    select c.handle, m.year_month
    from all_creators c
    cross join all_months m
)

select
    co.handle,
    co.year_month,
    coalesce(b.publicacoes, 0) as publicacoes
from combinations co
left join base b on co.handle = b.handle and co.year_month = b.year_month
order by co.handle, co.year_month
