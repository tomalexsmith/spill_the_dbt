with inventory_parts as (
    select * from {{ source('lego','inventory_parts') }}
),
inventories as (
    select * from {{ source('lego','inventories') }}
),
sets as (
    select * from {{ source('lego','sets') }}
),
parts as (
    select * from {{ source('lego','parts') }}
)

SELECT 
	P.part_num
FROM parts as P
INNER JOIN inventory_parts as IP on P.part_num = IP.part_number
INNER JOIN inventories as I on I.id = IP.inventory_id
INNER JOIN sets as S on S.set_num = I.set_num
	GROUP BY P.part_num
	HAVING COUNT(*) = 1