-- Compare records in both directions to catch missing,
-- extra, or mismatched inventory rows.

with source_rows as (
    select inventory_id, product_id, warehouse_code, stock_quantity
    from {{ ref('stg_inventory') }}
),

mart_rows as (
    select inventory_id, product_id, warehouse_code, stock_quantity
    from {{ ref('mart_inventory_risk') }}
)

select 'source_only' as difference, *
from (
    select * from source_rows
    except all
    select * from mart_rows
) missing

union all

select 'mart_only' as difference, *
from (
    select * from mart_rows
    except all
    select * from source_rows
) extra