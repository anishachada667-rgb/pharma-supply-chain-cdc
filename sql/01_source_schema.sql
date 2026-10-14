BEGIN;

CREATE SCHEMA IF NOT EXISTS operational;

CREATE TABLE IF NOT EXISTS operational.suppliers (
    supplier_id INTEGER PRIMARY KEY,
    supplier_name TEXT NOT NULL,
    country TEXT NOT NULL,
    lead_time_days INTEGER NOT NULL CHECK (lead_time_days >= 0),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS operational.products (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    unit_cost NUMERIC(12, 2) NOT NULL CHECK (unit_cost > 0),
    reorder_level INTEGER NOT NULL CHECK (reorder_level >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS operational.purchase_orders (
    purchase_order_id INTEGER PRIMARY KEY,
    supplier_id INTEGER NOT NULL
        REFERENCES operational.suppliers(supplier_id),
    product_id INTEGER NOT NULL
        REFERENCES operational.products(product_id),
    order_date DATE NOT NULL,
    expected_delivery_date DATE NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_cost NUMERIC(12, 2) NOT NULL CHECK (unit_cost > 0),
    status TEXT NOT NULL CHECK (
        status IN ('PLACED', 'PARTIALLY_RECEIVED', 'RECEIVED', 'CANCELLED')
    ),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (expected_delivery_date >= order_date)
);

CREATE TABLE IF NOT EXISTS operational.shipments (
    shipment_id INTEGER PRIMARY KEY,
    purchase_order_id INTEGER NOT NULL
        REFERENCES operational.purchase_orders(purchase_order_id),
    shipped_date DATE NOT NULL,
    delivered_date DATE,
    quantity_shipped INTEGER NOT NULL CHECK (quantity_shipped > 0),
    status TEXT NOT NULL CHECK (
        status IN ('IN_TRANSIT', 'DELIVERED')
    ),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (delivered_date IS NULL OR delivered_date >= shipped_date),
    CHECK (
        (status = 'DELIVERED' AND delivered_date IS NOT NULL)
        OR
        (status = 'IN_TRANSIT' AND delivered_date IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS operational.inventory (
    inventory_id INTEGER PRIMARY KEY,
    product_id INTEGER NOT NULL
        REFERENCES operational.products(product_id),
    warehouse_code TEXT NOT NULL,
    stock_quantity INTEGER NOT NULL CHECK (stock_quantity >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (product_id, warehouse_code)
);

-- Automatically timestamp updates for incremental processing.
CREATE OR REPLACE FUNCTION operational.set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$;

DO $$
DECLARE
    table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'suppliers', 'products', 'purchase_orders',
        'shipments', 'inventory'
    ]
    LOOP
        EXECUTE format(
            'CREATE OR REPLACE TRIGGER trg_set_updated_at
             BEFORE UPDATE ON operational.%I
             FOR EACH ROW
             EXECUTE FUNCTION operational.set_updated_at()',
            table_name
        );
    END LOOP;
END;
$$;

COMMIT;