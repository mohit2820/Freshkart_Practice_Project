from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    to_date
)


# ============================================================
# 1. CONFIGURATION
# ============================================================

dbutils.widgets.text("CATALOG", "fk_1393")
CATALOG = dbutils.widgets.get("CATALOG")


# ============================================================
# 2. TABLE LOCATIONS
# ============================================================

ORDER_ITEMS_SILVER = f"{CATALOG}.silver.order_items_silver"
ORDERS_SILVER = f"{CATALOG}.silver.orders_silver"

DIM_CUSTOMER = f"{CATALOG}.gold.dim_customer"
DIM_DATE = f"{CATALOG}.gold.dim_date"

FACT_TABLE = f"{CATALOG}.gold.fact_order_items"


# ============================================================
# 3. FACT ORDER ITEMS (line-item grain)
# ============================================================
# Joins order_items_silver with orders_silver for order-level
# measures, and looks up surrogate keys from gold dimension
# tables (customer_sk from dim_customer, date_key from dim_date).
# store_id and product_id are kept as business keys that link
# to dim_store and dim_product respectively.
# ============================================================

@dp.table(
    name=FACT_TABLE,
    comment="Fact table at order line-item grain with surrogate keys to gold dimensions"
)
def fact_order_items():

    # --- Read silver sources ---
    items_df = spark.read.table(ORDER_ITEMS_SILVER)

    orders_df = (
        spark.read.table(ORDERS_SILVER)
        .select(
            col("order_id"),
            col("updated_at"),
            col("promised_mins"),
            col("actual_mins"),
            col("delivery_fee"),
            col("payment_method"),
            col("txn_ref"),
            col("items_count"),
            col("total_qty"),
            col("gross_amount"),
            col("discount_amount"),
            col("net_amount"),
        )
    )

    # --- Read gold dimensions for surrogate key lookups ---
    dim_customer_df = (
        spark.read.table(DIM_CUSTOMER)
        .select(col("customer_id"), col("customer_sk"))
    )

    dim_date_df = (
        spark.read.table(DIM_DATE)
        .select(col("calendar_date"), col("date_key"))
    )

    # --- Join line items with order-level measures ---
    df = (
        items_df
        .join(orders_df, on="order_id", how="left")
    )

    # --- Look up customer surrogate key ---
    df = (
        df
        .join(dim_customer_df, on="customer_id", how="left")
    )

    # --- Look up date key from order date ---
    df = df.withColumn("_order_date", to_date(col("order_ts")))

    df = (
        df
        .join(
            dim_date_df,
            col("_order_date") == col("calendar_date"),
            how="left"
        )
        .drop("_order_date", "calendar_date")
    )

    # --- Final column selection ---
    df = df.select(
        # Foreign keys to dimensions
        col("date_key"),       # FK -> dim_date
        col("customer_sk"),    # FK -> dim_customer
        col("store_id"),       # FK -> dim_store
        col("product_id"),    # FK -> dim_product
        # Degenerate dimensions
        col("order_id"),
        col("line_number"),
        col("order_status"),
        col("payment_method"),
        col("txn_ref"),
        # Timestamps
        col("order_ts"),
        col("updated_at"),
        # Line-item measures
        col("qty"),
        col("unit_price"),
        col("discount"),
        col("line_amount"),
        # Order-level measures
        col("delivery_fee"),
        col("promised_mins"),
        col("actual_mins"),
        col("items_count"),
        col("total_qty"),
        col("gross_amount"),
        col("discount_amount"),
        col("net_amount"),
    )

    return df