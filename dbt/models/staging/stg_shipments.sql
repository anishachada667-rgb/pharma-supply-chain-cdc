select
    record_id as shipment_id,
    (record_data ->> 'purchase_order_id')::integer
        as purchase_order_id,
    date '1970-01-01'
        + (record_data ->> 'shipped_date')::integer as shipped_date,
    date '1970-01-01'
        + (record_data ->> 'delivered_date')::integer as delivered_date,
    (record_data ->> 'quantity_shipped')::integer as quantity_shipped,
    record_data ->> 'status' as status,
    (record_data ->> 'updated_at')::timestamptz as source_updated_at,
    last_offset as cdc_offset
from {{ source('cdc', 'current_records') }}
where source_table = 'shipments'
  and not is_deleted