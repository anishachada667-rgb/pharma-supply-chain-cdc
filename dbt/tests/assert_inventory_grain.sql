select
    product_id,
    warehouse_code,
    count(*) as record_count
from {{ ref('stg_inventory') }}
group by product_id, warehouse_code
having count(*) > 1