select
    record_id as supplier_id,
    record_data ->> 'supplier_name' as supplier_name,
    record_data ->> 'country' as country,
    (record_data ->> 'lead_time_days')::integer as lead_time_days,
    (record_data ->> 'is_active')::boolean as is_active,
    (record_data ->> 'updated_at')::timestamptz as source_updated_at,
    last_offset as cdc_offset
from {{ source('cdc', 'current_records') }}
where source_table = 'suppliers'
  and not is_deleted