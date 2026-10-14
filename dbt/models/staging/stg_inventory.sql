select
    record_id as inventory_id,
    (record_data ->> 'product_id')::integer as product_id,
    record_data ->> 'warehouse_code' as warehouse_code,
    (record_data ->> 'stock_quantity')::integer as stock_quantity,
    (record_data ->> 'updated_at')::timestamptz as source_updated_at,
    last_offset as cdc_offset
from {{ source('cdc', 'current_records') }}
where source_table = 'inventory'
  and not is_deleted