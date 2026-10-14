with shipment_summary as (
    select
        purchase_order_id,
        count(*) as shipment_count,
        sum(
            case
                when status = 'DELIVERED' then quantity_shipped
                else 0
            end
        ) as delivered_quantity,
        sum(
            case
                when status = 'IN_TRANSIT' then quantity_shipped
                else 0
            end
        ) as in_transit_quantity,
        max(delivered_date) as latest_delivery_date
    from {{ ref('stg_shipments') }}
    group by purchase_order_id
),

orders as (
    select *
    from {{ ref('stg_purchase_orders') }}
)

select
    o.purchase_order_id,
    o.supplier_id,
    o.product_id,
    o.order_date,
    o.expected_delivery_date,
    o.quantity as ordered_quantity,
    o.unit_cost,
    o.quantity * o.unit_cost as order_value,
    o.status as order_status,
    coalesce(s.shipment_count, 0) as shipment_count,
    coalesce(s.delivered_quantity, 0) as delivered_quantity,
    coalesce(s.in_transit_quantity, 0) as in_transit_quantity,
    greatest(
        o.quantity - coalesce(s.delivered_quantity, 0),
        0
    ) as outstanding_quantity,
    s.latest_delivery_date,
    case
        when o.status = 'RECEIVED'
         and coalesce(s.delivered_quantity, 0) >= o.quantity
        then s.latest_delivery_date
    end as completed_delivery_date,
    case
        when o.status = 'RECEIVED'
         and coalesce(s.delivered_quantity, 0) >= o.quantity
        then s.latest_delivery_date - o.order_date
    end as actual_lead_time_days,
    case
        when o.status = 'RECEIVED'
         and coalesce(s.delivered_quantity, 0) >= o.quantity
        then s.latest_delivery_date <= o.expected_delivery_date
    end as delivered_on_time,
    o.source_updated_at
from orders o
left join shipment_summary s
    on o.purchase_order_id = s.purchase_order_id