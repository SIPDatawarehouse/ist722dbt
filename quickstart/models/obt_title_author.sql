with
    f_title_author as (select * from {{ ref("fact_title_author") }}),
    d_authors as (select * from {{ ref("DIM_AUTHORS") }}),
    d_date as (select * from {{ ref("dim_date") }}),
    d_publishers as (select * from {{ ref("dim_publishers") }}),
    d_titles as (select * from {{ ref("DIM_TITLES") }})

select
    d_titles.*,
    d_authors.*,
    d_date.*,
    d_publishers.*,
    f.totalsalesrevenue_row,
    f.effectiveroyaltyearned_row,
    f.netearnings_row
from f_title_author as f
left join d_titles on f.titlekey = d_titles.titlekey
left join d_authors on f.authorkey = d_authors.authorkey
left join d_publishers on f.publisherskey = d_publishers.publisherskey
left join d_date on f.pubdatekey = d_date.datekey
