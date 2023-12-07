with
    f_title as (select * from {{ ref("fact_title_author") }}),
    d_title as (select * from {{ ref("dim_title") }}),
    d_authors as (select * from {{ ref("dim_authors") }}),
    d_date as (select * from {{ ref("dim_date") }}),
    d_publishers as (select * from {{ ref("dim_publishers") }})
select
    d_title.*,
    d_authors.*,
    d_date.*,
    d_publishers.*,
    f.totalsalesrevenue_row,
    f.effectiveroyaltyearned_row,
    f.netearnings_row
from fact_title_author f
left join d_title t on f.titlekey = t.titlekey
left join d_authors a on f.authorkey = a.authorkey
left join d_publishers p on f.publisherskey = p.publisherskey
left join d_date on f.pubdatekey = d_date.datekey
