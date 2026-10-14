select
    record_id as product_id,
    record_data ->> 'product_name' as product_name,
    record_data ->> 'category' as category,
    (record_data ->> 'unit_cost')::numeric(12, 2) as unit_cost,
    (record_data ->> 'reorder_level')::integer as reorder_level,
    (record_data ->> 'updated_at')::timestamptz as source_updated_at,
    last_offset as cdc_offset
from {{ source('cdc', 'current_records') }}
where source_table = 'products'
  and not is_deleted