-- A dbt test passes when this query returns zero rows.

select 'supplier' as entity, supplier_id as record_id
from {{ ref('stg_suppliers') }}
where lead_time_days is null
   or lead_time_days < 0
   or nullif(trim(supplier_name), '') is null
   or nullif(trim(country), '') is null
   or is_active is null
   or source_updated_at is null

union all

select 'product', product_id
from {{ ref('stg_products') }}
where unit_cost is null
   or unit_cost <= 0
   or reorder_level is null
   or reorder_level < 0
   or nullif(trim(product_name), '') is null
   or nullif(trim(category), '') is null
   or source_updated_at is null

union all

select 'purchase_order', purchase_order_id
from {{ ref('stg_purchase_orders') }}
where quantity is null
   or quantity <= 0
   or unit_cost is null
   or unit_cost <= 0
   or order_date is null
   or expected_delivery_date is null
   or expected_delivery_date < order_date
   or source_updated_at is null

union all

select 'shipment', shipment_id
from {{ ref('stg_shipments') }}
where quantity_shipped is null
   or quantity_shipped <= 0
   or shipped_date is null
   or delivered_date < shipped_date
   or (status = 'DELIVERED' and delivered_date is null)
   or (status = 'IN_TRANSIT' and delivered_date is not null)
   or source_updated_at is null

union all

select 'inventory', inventory_id
from {{ ref('stg_inventory') }}
where stock_quantity is null
   or stock_quantity < 0
   or nullif(trim(warehouse_code), '') is null
   or source_updated_at is null