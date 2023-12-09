with
    f_title_author as (select * from {{ ref("fact_title_author") }}),
    d_authors as (select * from {{ ref("DIM_AUTHORS") }}),
    d_date as (select * from {{ ref("dim_date") }}),
    d_publishers as (select * from {{ ref("dim_publishers") }}),
    d_titles as (select * from {{ ref("DIM_TITLES") }})

select
    d.*,
    a.*,
    d_date.*,
    p.*,
    f.totalsalesrevenue_row,
    f.effectiveroyaltyearned_row,
    f.netearnings_row
from f_title_author f
left join d_titles d on f.titlekey = d.titlekey
left join d_authors a on f.authorkey = a.authorkey
left join d_publishers p on f.publisherskey = p.publisherskey
left join d_date on f.pubdatekey = d_date.datekey
