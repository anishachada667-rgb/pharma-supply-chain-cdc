import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import plotly.express as px
import psycopg
import streamlit as st
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

st.set_page_config(
    page_title="Pharma Supply Chain Analytics",
    page_icon="📊",
    layout="wide",
)

st.title("Pharmaceutical Supply Chain Analytics")
st.caption(
    "Synthetic data | PostgreSQL CDC → Kafka → Warehouse → dbt → Airflow"
)

if st.button("Refresh warehouse data"):
    st.rerun()


def read_frame(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        columns = [column.name for column in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)


try:
    password = os.getenv("WAREHOUSE_DB_PASSWORD")
    if not password:
        raise ValueError("Warehouse password is missing")

    with psycopg.connect(
        host="127.0.0.1",
        port=5434,
        dbname="pharma_warehouse",
        user="warehouse",
        password=password,
        connect_timeout=10,
    ) as connection:
        # Read all dashboard tables from one consistent DB snapshot.
        connection.execute(
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
        )

        suppliers = read_frame(
            connection,
            "SELECT * FROM analytics.mart_supplier_performance",
        )
        inventory = read_frame(
            connection,
            "SELECT * FROM analytics.mart_inventory_risk",
        )
        orders = read_frame(
            connection,
            """
            SELECT
                purchase_order_id,
                order_date,
                order_status,
                ordered_quantity,
                order_value
            FROM analytics.fct_purchase_orders
            """,
        )

except Exception:
    st.error(
        "Unable to load warehouse tables. Check that Docker is running, "
        "the .env password is correct, and the dbt marts have been built."
    )
    st.stop()


for column in [
    "on_time_delivery_pct",
    "avg_actual_lead_time_days",
    "non_cancelled_order_value",
]:
    suppliers[column] = pd.to_numeric(suppliers[column], errors="coerce")

for column in ["unit_cost", "inventory_value"]:
    inventory[column] = pd.to_numeric(inventory[column], errors="coerce")

orders["order_value"] = pd.to_numeric(
    orders["order_value"], errors="coerce"
)
orders["order_date"] = pd.to_datetime(orders["order_date"])

completed = int(suppliers["completed_orders"].sum())
on_time = int(suppliers["on_time_orders"].sum())
late = int(suppliers["late_orders"].sum())

delivery_rate = (
    f"{100 * on_time / completed:.2f}%"
    if completed
    else "N/A"
)

stock_units = int(inventory["stock_quantity"].sum())
at_risk = int(
    inventory["stock_status"].isin(
        ["LOW_STOCK", "OUT_OF_STOCK"]
    ).sum()
)

st.caption(
    "Warehouse data read at "
    + datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    + ". This is the read time, not the last successful pipeline run."
)

cards = st.columns(4)
cards[0].metric("Purchase-order lines", f"{len(orders):,}")
cards[1].metric("Completed orders", f"{completed:,}")
cards[2].metric("On-time delivery", delivery_rate)
cards[3].metric("Inventory stock units", f"{stock_units:,}")

st.info(
    "Delivery rate = on-time completed orders ÷ all completed orders. "
    "Inventory risk is evaluated separately for each product and warehouse. "
    "Analytics tables refresh through Airflow every 15 minutes while "
    "the local services are running."
)

delivery_tab, inventory_tab, orders_tab = st.tabs(
    ["Supplier deliveries", "Inventory risk", "Purchase orders"]
)

with delivery_tab:
    left, right = st.columns(2)

    with left:
        st.subheader("Completed delivery outcomes")
        outcomes = pd.DataFrame({
            "Outcome": ["On time", "Late"],
            "Orders": [on_time, late],
        })
        st.plotly_chart(
            px.bar(
                outcomes,
                x="Outcome",
                y="Orders",
                color="Outcome",
                color_discrete_map={
                    "On time": "#16a34a",
                    "Late": "#dc2626",
                },
            ),
            use_container_width=True,
        )

    with right:
        st.subheader("10 lowest on-time delivery rates")
        ranked = (
            suppliers.dropna(subset=["on_time_delivery_pct"])
            .sort_values(["on_time_delivery_pct", "supplier_id"])
            .head(10)
        )
        st.plotly_chart(
            px.bar(
                ranked,
                x="on_time_delivery_pct",
                y="supplier_name",
                orientation="h",
                labels={
                    "on_time_delivery_pct": "On-time delivery (%)",
                    "supplier_name": "Supplier",
                },
                hover_data=["completed_orders"],
                range_x=[0, 100],
            ),
            use_container_width=True,
        )

    st.subheader("Supplier performance details")
    st.dataframe(
        suppliers[[
            "supplier_id",
            "supplier_name",
            "country",
            "completed_orders",
            "on_time_orders",
            "late_orders",
            "on_time_delivery_pct",
            "avg_actual_lead_time_days",
        ]].sort_values("supplier_id"),
        hide_index=True,
        use_container_width=True,
    )

with inventory_tab:
    st.metric("Product-warehouse records at risk", f"{at_risk:,}")

    warehouse_filter = st.selectbox(
        "Warehouse",
        ["All"] + sorted(inventory["warehouse_code"].unique().tolist()),
    )

    filtered = inventory.copy()
    if warehouse_filter != "All":
        filtered = filtered[
            filtered["warehouse_code"] == warehouse_filter
        ]

    left, right = st.columns(2)

    with left:
        status_counts = (
            filtered.groupby("stock_status")
            .size()
            .reset_index(name="records")
        )
        st.plotly_chart(
            px.bar(
                status_counts,
                x="stock_status",
                y="records",
                title="Inventory records by stock status",
                labels={
                    "stock_status": "Stock status",
                    "records": "Product-warehouse records",
                },
            ),
            use_container_width=True,
        )

    with right:
        category_stock = (
            filtered.groupby("category", as_index=False)["stock_quantity"]
            .sum()
        )
        st.plotly_chart(
            px.bar(
                category_stock,
                x="category",
                y="stock_quantity",
                title="Stock units by product category",
                labels={
                    "category": "Category",
                    "stock_quantity": "Stock units",
                },
            ),
            use_container_width=True,
        )

    st.subheader("Low-stock and out-of-stock records")
    risks = filtered[
        filtered["stock_status"] != "SUFFICIENT"
    ].sort_values("units_to_reorder_level", ascending=False)

    st.dataframe(
        risks[[
            "product_id",
            "product_name",
            "warehouse_code",
            "stock_quantity",
            "reorder_level",
            "units_to_reorder_level",
            "stock_status",
        ]],
        hide_index=True,
        use_container_width=True,
    )
    st.caption(
        "The shortfall to the reorder threshold is not a demand forecast. "
        "Inventory is an independent opening balance, not derived from "
        "the purchase-order dataset."
    )

with orders_tab:
    non_cancelled = orders[orders["order_status"] != "CANCELLED"].copy()

    st.metric(
        "Non-cancelled order value",
        f"{non_cancelled['order_value'].sum():,.2f}",
    )

    left, right = st.columns(2)

    with left:
        status_counts = (
            orders.groupby("order_status")
            .size()
            .reset_index(name="orders")
        )
        st.plotly_chart(
            px.bar(
                status_counts,
                x="order_status",
                y="orders",
                title="Purchase-order lines by status",
            ),
            use_container_width=True,
        )

    with right:
        non_cancelled["month"] = (
            non_cancelled["order_date"]
            .dt.to_period("M")
            .dt.to_timestamp()
        )
        monthly = (
            non_cancelled.groupby("month", as_index=False)["order_value"]
            .sum()
        )
        st.plotly_chart(
            px.line(
                monthly,
                x="month",
                y="order_value",
                markers=True,
                title="Non-cancelled order value by order month",
            ),
            use_container_width=True,
        )

    st.caption(
        "Order value uses quantities and unit costs recorded on purchase "
        "orders; it does not represent revenue or paid invoices. "
        "Currency is unspecified. The synthetic business reference date "
        "is October 13, 2026."
    )