with
    inventory_parts as (select * from {{ source("lego", "inventory_parts") }}),
    inventories as (select * from {{ source("lego", "inventories") }}),
    parts as (select * from {{ source("lego", "parts") }}),
    sets as (select * from {{ source("lego", "sets") }}),
    themes as (select * from {{ source("lego", "themes") }})

select
    t.name as theme_name,
    s.name as set_name,
    s.year as set_year,
    case when up.part_num is null then 'Not Unique' else 'Unique' end as unique_part,
    count(p.part_num) as parts
from parts as p
left join {{ ref("unique_parts") }} as up on p.part_num = up.part_num
inner join inventory_parts as ip on p.part_num = ip.part_number
inner join inventories as i on i.id = ip.inventory_id
inner join sets as s on s.set_num = i.set_num
inner join themes as t on t.id = s.theme_id
group by 1, 2, 3, 4
