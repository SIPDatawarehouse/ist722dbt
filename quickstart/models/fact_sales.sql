with stg_sales as 
(
    select
        Ord_num as orderid,
        title_id,
        stor_id,
        {{ dbt_utils.generate_surrogate_key(['title_id']) }} as titlekey, 
        {{ dbt_utils.generate_surrogate_key(['stor_id']) }} as storekey,
        qty as quantity
    from {{source('pubs','Sales')}}
),
stg_titles_more as
(
    select 
        title_id,
        price 
    from {{source('pubs','Titles')}}
),
stg_discounts as (
    select * from {{source('pubs','Discounts')}}
)
select  
    s.orderid,
    s.titlekey,
    s.storekey,
    s.quantity,
    case when d.discount is not null then d.discount else 0 end as discount,
    s.quantity*t.price as income,
    s.quantity*(t.price*(1-(d.discount/100))) as discountedincome
from stg_sales s
    join stg_titles_more t on s.title_id = t.title_id
    join stg_discounts d on s.stor_id = d.stor_id
