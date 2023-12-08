with
    stg_title_author as (
        select
            {{ dbt_utils.generate_surrogate_key(["au_id"]) }} as authorkey,
            {{ dbt_utils.generate_surrogate_key(["title_id"]) }} as titlekey,
            *
        from {{ source("pubs", "Titleauthor") }}
    ),
    stg_title as (
        select
            {{ dbt_utils.generate_surrogate_key(["title_id"]) }} as titlekey,
            {{ dbt_utils.generate_surrogate_key(["pub_id"]) }} as publisherskey,
            *
        from {{ source("pubs", "Titles") }}
    ),
    title_author as (
        select
            a.authorkey,
            a.titlekey,
            t.publisherskey,
            a.title_id,
            a.au_ord,
            a.royaltyper,
            t.price,
            t.advance,
            t.royalty,
            t.ytd_sales,
            (t.price * t.ytd_sales) as totalsalesrevenue_row,
            (
                (a.royaltyper / 100) * t.price * t.ytd_sales
            ) as effectiveroyaltyearned_row,
            ((t.royalty * t.ytd_sales / 100) - t.advance) as netearnings_row,
            t.pubdate
        from stg_title_author a
        join stg_title t on a.title_id = t.title_id
    )
select
    authorkey,
    titlekey,
    publisherskey,
    title_id,
    au_ord,
    sum(totalsalesrevenue_row) as totalsalesrevenue,  -- Total Sales Revenue per Title
    sum(effectiveroyaltyearned_row) as effectiveroyaltyearned,  -- Effective Royalty Earned per Title
    sum(netearnings_row) as netearnings,  -- Net Earnings per Author 
    pubdate
from title_author
where title_id = 'BU1032'
group by authorkey, titlekey, publisherskey, title_id, au_ord, pubdate
