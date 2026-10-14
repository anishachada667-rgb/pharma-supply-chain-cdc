select
    record_id as purchase_order_id,
    (record_data ->> 'supplier_id')::integer as supplier_id,
    (record_data ->> 'product_id')::integer as product_id,
    date '1970-01-01'
        + (record_data ->> 'order_date')::integer as order_date,
    date '1970-01-01'
        + (record_data ->> 'expected_delivery_date')::integer
        as expected_delivery_date,
    (record_data ->> 'quantity')::integer as quantity,
    (record_data ->> 'unit_cost')::numeric(12, 2) as unit_cost,
    record_data ->> 'status' as status,
    (record_data ->> 'updated_at')::timestamptz as source_updated_at,
    last_offset as cdc_offset
from {{ source('cdc', 'current_records') }}
where source_table = 'purchase_orders'
  and not is_deleted