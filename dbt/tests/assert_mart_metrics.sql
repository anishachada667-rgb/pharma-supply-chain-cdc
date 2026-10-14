select 'supplier_metrics' as check_name, supplier_id as record_id
from {{ ref('mart_supplier_performance') }}
where completed_orders <> on_time_orders + late_orders
   or completed_orders > total_orders - cancelled_orders
   or on_time_delivery_pct < 0
   or on_time_delivery_pct > 100
   or (
       completed_orders = 0
       and on_time_delivery_pct is not null
   )
   or (
       completed_orders > 0
       and on_time_delivery_pct is distinct from
           round(100.0 * on_time_orders / completed_orders, 2)
   )

union all

select 'inventory_metrics', inventory_id
from {{ ref('mart_inventory_risk') }}
where stock_quantity is null
   or stock_quantity < 0
   or reorder_level is null
   or reorder_level < 0
   or units_to_reorder_level is distinct from
       greatest(reorder_level - stock_quantity, 0)
   or inventory_value is distinct from stock_quantity * unit_cost
   or stock_status is distinct from
       case
           when stock_quantity = 0 then 'OUT_OF_STOCK'
           when stock_quantity < reorder_level then 'LOW_STOCK'
           else 'SUFFICIENT'
       end