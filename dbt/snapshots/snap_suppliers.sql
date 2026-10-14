{% snapshot snap_suppliers %}

{{
    config(
        target_schema='history',
        unique_key='supplier_id',
        strategy='check',
        check_cols=[
            'supplier_name',
            'country',
            'lead_time_days',
            'is_active'
        ],
        invalidate_hard_deletes=true
    )
}}

select
    supplier_id,
    supplier_name,
    country,
    lead_time_days,
    is_active,
    source_updated_at
from {{ ref('stg_suppliers') }}

{% endsnapshot %}