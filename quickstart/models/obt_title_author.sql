with
    fact_ta as (select * from {{ ref("fact_title_author") }}),
    d_titles as (select * from {{ ref("dim_titles") }}),
    d_authors as (select * from {{ ref("dim_authors") }}),
    d_date as (select * from {{ ref("dim_date") }}),
    d_publishers as (select * from {{ ref("dim_publishers") }})

select
    d_titles.*,
    d_authors.*,
    d_date.*,
    d_publishers.*,
    f.totalsalesrevenue_row,
    f.effectiveroyaltyearned_row,
    f.netearnings_row
from fact_title_author f
left join d_titles t on f.titlekey = t.titlekey
left join d_authors a on f.authorkey = a.authorkey
left join d_publishers p on f.publisherskey = p.publisherskey
