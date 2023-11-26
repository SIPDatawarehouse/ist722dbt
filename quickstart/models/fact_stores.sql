with
    stg_sales as (
        select
            ord_num as orderid,
            title_id,
            stor_id,
            {{ dbt_utils.generate_surrogate_key(["title_id"]) }} as titlekey,
            {{ dbt_utils.generate_surrogate_key(["stor_id"]) }} as storekey,
            qty as quantity
        from {{ source("pubs", "Sales") }}
    ),
    stg_title as (select title_id, price from {{ source("pubs", "Titles") }}),
    stg_discounts as (select * from {{ source("pubs", "Discounts") }}),
    stg_stores as (select * from {{ source("pubs", "Stores") }})

select
    s.*,
    case when d.discount is not null then d.discount else 0 end as discount,
    s.quantity * t.price as income,
    s.quantity * (t.price * (1 - (d.discount / 100))) as discountedincome
from stg_sales s
join stg_discounts d on s.stor_id = d.stor_id
join stg_title t on t.title_id = s.title_id
join stg_stores so on so.stor_id = s.stor_id
