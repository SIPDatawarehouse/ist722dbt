with stg_titles as (select * from {{ source("pubs", "Titles") }})
select
    {{ dbt_utils.generate_surrogate_key(["stg_titles.title_id"]) }} as titlekey,
    stg_titles.title_id as titleid,
    stg_titles.title as titlename,
    stg_titles.type as titletype,
    stg_titles.price as titleprice,
    stg_titles.notes as titlenotes,
    stg_titles.pubdate as titlepubdate,
    stg_titles.pub_id as pub_id
from stg_titles

