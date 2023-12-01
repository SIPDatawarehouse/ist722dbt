select * from {{ source("pubs", "Titleauthor") }} where title_id = 'BU1032'
