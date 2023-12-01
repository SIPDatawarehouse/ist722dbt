with
    stg_sales as (
        select
            ord_num as orderid,
            title_id,
            stor_id,
            {{ dbt_utils.generate_surrogate_key(["title_id"]) }} as titlekey,
            {{ dbt_utils.generate_surrogate_key(["stor_id"]) }} as storekey,
            qty as quantity,
            case when stor_id = 8042 then 5 else 0 end as discount
        from {{ source("pubs", "Sales") }}
    ),
    stg_title as (
        select title_id, price, pub_id, ytd_sales from {{ source("pubs", "Titles") }}
    ),  -- Added pub_id here
    stg_publishers as (select * from {{ source("pubs", "Publishers") }})

select
    s.orderid,
    s.titlekey,
    s.storekey,
    s.quantity,
    s.quantity * t.price as income,
    s.quantity * (t.price * (1 - (s.discount / 100))) as discountedincome,
    t.ytd_sales
from stg_title t
join stg_publishers p on t.pub_id = p.pub_id
join stg_sales s on t.title_id = s.title_id
