with stg_titles as (
    select 
        title_id,
        price,
        ytd_sales,
        royalty 
    from {{source('pubs','Titles')}}
),
stg_titleauthor as (
    select 
        title_id,
        au_id,
        royaltyper
    from {{source('pubs', 'Titleauthor')}}
)
select 
    ta.au_id,
    count(t.title_id) as num_titles,
    sum(t.ytd_sales) as yearly_qty,
    sum(t.ytd_sales*t.price*ta.royaltyper/100) as yearly_sales,
    sum(t.ytd_sales*t.price*((t.royalty*ta.royaltyper)/100)/100) as yearly_royalty
from stg_titles t
    join stg_titleauthor ta 
group by ta.au_id