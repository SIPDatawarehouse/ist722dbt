with
    f_title as (select * from {{ ref("fact_title_author") }}),
    d_authors as (select * from {{ ref("dim_authors") }}),
    d_date as (select * from {{ ref("dim_date") }}),
    d_publishers as (select * from {{ ref('dim_publishers') }}),
    d_title as (select * from {{ ref("dim_titles") }})

select
    f_title.*,
    d_authors.*,
    d_date.*,
    d_publishers.*,
    f.totalsalesrevenue,
    f.effectiveroyaltyearned,
    f.netearnings
from f_title f
left join d_title d on d.titlekey = f.titlekey
left join d_authors a on f.authorkey = a.authorkey
left join d_publishers p on f.publisherskey = p.publisherskey
left join d_date on f.pubdatekey = d_date.datekey
