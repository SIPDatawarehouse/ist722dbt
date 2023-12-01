with stg_discounts as (
    select * from {{ source('pubs','Discounts')}}
)
select  {{ dbt_utils.generate_surrogate_key(['stg_discounts.discounttype']) }} as discountkey, 
    stg_discounts.discounttype as discounttype,
    stg_discounts.discount as discountamount,
    stg_discounts.stor_id as storeid
from stg_discounts