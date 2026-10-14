with expected as (
    select
        record_id as inventory_id,
        (record_data ->> 'product_id')::integer as product_id,
        record_data ->> 'warehouse_code' as warehouse_code,
        (record_data ->> 'stock_quantity')::integer as stock_quantity,
        (record_data ->> 'updated_at')::timestamptz as source_updated_at,
        is_deleted,
        last_topic as cdc_topic,
        last_partition as cdc_partition,
        last_offset as cdc_offset
    from {{ source('cdc', 'current_records') }}
    where source_table = 'inventory'
),

actual as (
    select
        inventory_id,
        product_id,
        warehouse_code,
        stock_quantity,
        source_updated_at,
        is_deleted,
        cdc_topic,
        cdc_partition,
        cdc_offset
    from {{ ref('int_inventory_current') }}
)

select 'source_only' as difference, *
from (
    select * from expected
    except all
    select * from actual
) missing

union all

select 'incremental_only' as difference, *
from (
    select * from actual
    except all
    select * from expected
) extra