with source_totals as (
    select
        count(*) as record_count,
        coalesce(sum(quantity), 0) as ordered_units,
        coalesce(sum(quantity * unit_cost), 0) as order_value
    from {{ ref('stg_purchase_orders') }}
),

fact_totals as (
    select
        count(*) as record_count,
        count(distinct purchase_order_id) as unique_orders,
        coalesce(sum(ordered_quantity), 0) as ordered_units,
        coalesce(sum(order_value), 0) as order_value
    from {{ ref('fct_purchase_orders') }}
)

select
    s.record_count as source_records,
    f.record_count as fact_records,
    f.unique_orders,
    s.ordered_units as source_units,
    f.ordered_units as fact_units,
    s.order_value as source_value,
    f.order_value as fact_value
from source_totals s
cross join fact_totals f
where s.record_count <> f.record_count
   or f.record_count <> f.unique_orders
   or s.ordered_units <> f.ordered_units
   or s.order_value <> f.order_value