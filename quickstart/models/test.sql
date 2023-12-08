with
    f_title_author as (select * from {{ ref("fact_title_author") }}),
    d_authors as (select * from {{ ref("dim_authors") }}),
    d_date as (select * from {{ ref("dim_date") }})

select *
from f_title_author f
left join d_authors a on f.authorkey = a.authorkey
left join d_date d on f.pubdatekey = d.datekey
