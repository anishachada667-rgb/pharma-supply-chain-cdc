select
    i.inventory_id,
    i.product_id,
    p.product_name,
    p.category,
    i.warehouse_code,
    i.stock_quantity,
    p.reorder_level,
    p.unit_cost,
    i.stock_quantity * p.unit_cost as inventory_value,
    greatest(p.reorder_level - i.stock_quantity, 0)
        as units_to_reorder_level,
    case
        when i.stock_quantity = 0 then 'OUT_OF_STOCK'
        when i.stock_quantity < p.reorder_level then 'LOW_STOCK'
        else 'SUFFICIENT'
    end as stock_status,
    i.source_updated_at
from {{ ref('int_inventory_current') }} i
join {{ ref('stg_products') }} p
    on i.product_id = p.product_id
where not i.is_deleted