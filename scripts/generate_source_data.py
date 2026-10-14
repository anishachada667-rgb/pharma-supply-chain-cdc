from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
import random

rng = random.Random(42)
project_root = Path(__file__).resolve().parents[1]
output_path = project_root / "sql" / "02_seed_source.sql"

AS_OF_DATE = date(2026, 10, 13)


def sql_value(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, Decimal)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def insert_statement(table, columns, rows):
    values = [
        "(" + ", ".join(sql_value(value) for value in row) + ")"
        for row in rows
    ]
    return (
        f"INSERT INTO operational.{table} ({', '.join(columns)}) VALUES\n"
        + ",\n".join(values)
        + ";\n"
    )


suppliers = []
for supplier_id in range(1, 51):
    suppliers.append((
        supplier_id,
        f"Synthetic Supplier {supplier_id:03d}",
        rng.choice(["USA", "India", "Germany", "Ireland"]),
        rng.randint(3, 14),
        True,
    ))

products = []
for product_id in range(1, 201):
    products.append((
        product_id,
        f"Synthetic Product {product_id:03d}",
        rng.choice(["Tablets", "Capsules", "Injectables", "Liquids"]),
        Decimal(rng.randint(500, 50000)) / Decimal(100),
        rng.randint(50, 200),
    ))

purchase_orders = []
shipments = []
shipment_id = 0

for order_id in range(1, 5001):
    supplier = rng.choice(suppliers)
    product = rng.choice(products)
    quantity = rng.randint(20, 500)

    order_date = AS_OF_DATE - timedelta(days=rng.randint(20, 180))
    expected_date = order_date + timedelta(days=supplier[3])

    status = rng.choices(
        ["RECEIVED", "PARTIALLY_RECEIVED", "PLACED", "CANCELLED"],
        weights=[65, 15, 15, 5],
        k=1,
    )[0]

    purchase_orders.append((
        order_id,
        supplier[0],
        product[0],
        order_date,
        expected_date,
        quantity,
        product[3],
        status,
    ))

    if status in {"RECEIVED", "PARTIALLY_RECEIVED"}:
        shipment_id += 1
        shipped_date = order_date + timedelta(
            days=rng.randint(1, supplier[3])
        )
        delivered_date = max(
            shipped_date,
            expected_date + timedelta(days=rng.randint(-2, 5)),
        )
        delivered_quantity = (
            quantity if status == "RECEIVED" else quantity // 2
        )

        shipments.append((
            shipment_id,
            order_id,
            shipped_date,
            delivered_date,
            delivered_quantity,
            "DELIVERED",
        ))

        if status == "PARTIALLY_RECEIVED":
            shipment_id += 1
            shipments.append((
                shipment_id,
                order_id,
                AS_OF_DATE - timedelta(days=1),
                None,
                quantity - delivered_quantity,
                "IN_TRANSIT",
            ))

inventory = []
inventory_id = 0

for product in products:
    for warehouse_code in ["WH-OH", "WH-NJ", "WH-TX"]:
        inventory_id += 1
        inventory.append((
            inventory_id,
            product[0],
            warehouse_code,
            rng.randint(0, 1000),
        ))

datasets = [
    ("suppliers",
     ["supplier_id", "supplier_name", "country",
      "lead_time_days", "is_active"], suppliers),
    ("products",
     ["product_id", "product_name", "category",
      "unit_cost", "reorder_level"], products),
    ("purchase_orders",
     ["purchase_order_id", "supplier_id", "product_id",
      "order_date", "expected_delivery_date", "quantity",
      "unit_cost", "status"], purchase_orders),
    ("shipments",
     ["shipment_id", "purchase_order_id", "shipped_date",
      "delivered_date", "quantity_shipped", "status"], shipments),
    ("inventory",
     ["inventory_id", "product_id", "warehouse_code",
      "stock_quantity"], inventory),
]

# Refuse accidental reloads into populated source tables.
statements = [
    "BEGIN;",
    """
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM operational.suppliers)
       OR EXISTS (SELECT 1 FROM operational.products)
       OR EXISTS (SELECT 1 FROM operational.purchase_orders)
       OR EXISTS (SELECT 1 FROM operational.shipments)
       OR EXISTS (SELECT 1 FROM operational.inventory)
    THEN
        RAISE EXCEPTION
            'Seed load requires empty source tables; existing data preserved.';
    END IF;
END;
$$;
""",
]

for table, columns, rows in datasets:
    statements.append(insert_statement(table, columns, rows))

statements.append("COMMIT;")
output_path.parent.mkdir(parents=True, exist_ok=True)
output_path.write_text("\n".join(statements), encoding="utf-8")

print(f"Generated: {output_path}")
print(f"Business reference date: {AS_OF_DATE}")
for table, _, rows in datasets:
    print(f"{table}: {len(rows):,} records")