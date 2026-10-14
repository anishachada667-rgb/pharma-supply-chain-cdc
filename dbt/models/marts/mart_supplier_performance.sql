with supplier_metrics as (
    select
        supplier_id,
        count(*) as total_orders,
        count(*) filter (
            where order_status = 'CANCELLED'
        ) as cancelled_orders,
        count(*) filter (
            where completed_delivery_date is not null
        ) as completed_orders,
        count(*) filter (
            where delivered_on_time is true
        ) as on_time_orders,
        count(*) filter (
            where delivered_on_time is false
        ) as late_orders,
        sum(order_value) filter (
            where order_status <> 'CANCELLED'
        ) as non_cancelled_order_value,
        avg(actual_lead_time_days) as avg_actual_lead_time_days
    from {{ ref('fct_purchase_orders') }}
    group by supplier_id
)

select
    s.supplier_id,
    s.supplier_name,
    s.country,
    s.lead_time_days as current_supplier_lead_time_days,
    s.is_active,
    coalesce(m.total_orders, 0) as total_orders,
    coalesce(m.cancelled_orders, 0) as cancelled_orders,
    coalesce(m.completed_orders, 0) as completed_orders,
    coalesce(m.on_time_orders, 0) as on_time_orders,
    coalesce(m.late_orders, 0) as late_orders,
    round(
        100.0 * m.on_time_orders
        / nullif(m.completed_orders, 0),
        2
    ) as on_time_delivery_pct,
    round(m.avg_actual_lead_time_days, 2)
        as avg_actual_lead_time_days,
    coalesce(m.non_cancelled_order_value, 0)
        as non_cancelled_order_value
from {{ ref('stg_suppliers') }} s
left join supplier_metrics m
    on s.supplier_id = m.supplier_id