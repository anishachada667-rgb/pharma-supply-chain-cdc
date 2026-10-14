{{
    config(
        materialized='incremental',
        unique_key='inventory_id',
        incremental_strategy='delete+insert'
    )
}}

with inventory_changes as (
    select
        r.record_id as inventory_id,
        (r.record_data ->> 'product_id')::integer as product_id,
        r.record_data ->> 'warehouse_code' as warehouse_code,
        (r.record_data ->> 'stock_quantity')::integer
            as stock_quantity,
        (r.record_data ->> 'updated_at')::timestamptz
            as source_updated_at,
        r.is_deleted,
        r.last_topic as cdc_topic,
        r.last_partition as cdc_partition,
        r.last_offset as cdc_offset
    from {{ source('cdc', 'current_records') }} r

    {% if is_incremental() %}
    left join {{ this }} existing
        on r.record_id = existing.inventory_id
    {% endif %}

    where r.source_table = 'inventory'

    {% if is_incremental() %}
      and (
          existing.inventory_id is null
          or (
              r.last_topic = existing.cdc_topic
              and r.last_partition = existing.cdc_partition
              and r.last_offset > existing.cdc_offset
          )
      )
    {% endif %}
)

select *
from inventory_changes