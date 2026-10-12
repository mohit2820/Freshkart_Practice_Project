from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, trim, upper, when, lit, coalesce,
    to_timestamp, to_date, concat_ws, to_json,
    struct, current_timestamp, posexplode, expr,
    row_number
)
from pyspark.sql.window import Window


# =====================================================
# 1. CONFIGURATION
# =====================================================

dbutils.widgets.text("CATALOG", "fk_1393")
CATALOG = dbutils.widgets.get("CATALOG")
BRONZE_TABLE = f"{CATALOG}.bronze.orders"

# =====================================================
# 2. STANDARDIZATION + ORDER VALIDATION
# =====================================================

def prepare_orders():

    df = spark.read.table(BRONZE_TABLE)

    # Preserve original Bronze record
    df = df.withColumn(
        "raw_record",
        to_json(struct(*[col(c) for c in df.columns]))
    )

    # S1: Standardize IDs
    for c in ["order_id", "customer_id", "store_id"]:
        df = df.withColumn(
            c,
            when(
                col(c).isNull() | (trim(col(c).cast("string")) == ""),
                lit(None).cast("string")
            ).otherwise(upper(trim(col(c).cast("string"))))
        )

    # S2: Standardize order status
    df = df.withColumn(
        "order_status",
        upper(trim(col("order_status")))
    )

    df = df.withColumn(
        "order_status",
        when(col("order_status") == "CANCELED", "CANCELLED")
        .otherwise(col("order_status"))
    )

    # S3: Standardize payment method
    df = df.withColumn(
        "payment_method",
        upper(trim(col("payment.method")))
    )

    # S4: Standardize channel
    df = df.withColumn(
        "channel",
        when(
            col("channel").isNull()
            | (trim(col("channel").cast("string")) == ""),
            lit("UNKNOWN")
        ).otherwise(upper(trim(col("channel").cast("string"))))
    )

    # S5: Parse only the two permitted order_ts formats
    order_ts_raw = trim(col("order_ts").cast("string"))

    df = df.withColumn(
        "order_ts",
        coalesce(
            to_timestamp(order_ts_raw, "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(order_ts_raw, "yyyy-MM-dd'T'HH:mm:ss")
        )
    )

    # S6: Standardize updated_at timestamp
    updated_raw = trim(col("updated_at").cast("string"))

    df = df.withColumn(
        "updated_at",
        coalesce(
            to_timestamp(updated_raw, "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(updated_raw, "yyyy-MM-dd'T'HH:mm:ss")
        )
    )

    # Derive order_date from order_ts
    df = df.withColumn(
        "order_date",
        to_date(col("order_ts"))
    )

    # =================================================
    # 3. ORDER-LEVEL DATA QUALITY CHECKS
    # =================================================

    df = df.withColumn(
        "reject_reason",
        concat_ws(
            "; ",

            when(
                col("order_id").isNull(),
                "Missing order_id"
            ),

            when(
                col("customer_id").isNull(),
                "Missing customer_id"
            ),

            when(
                col("store_id").isNull(),
                "Missing store_id"
            ),

            when(
                col("order_ts").isNull(),
                "Invalid order_ts format"
            ),

            when(
                col("updated_at").isNull(),
                "Invalid updated_at format"
            ),

            when(
                ~col("order_status").isin(
                    "PLACED", "DELIVERED", "CANCELLED"
                ) | col("order_status").isNull(),
                "Invalid order_status"
            ),

            when(
                ~col("payment_method").isin(
                    "UPI", "CARD", "COD", "WALLET"
                ) | col("payment_method").isNull(),
                "Invalid payment_method"
            )
        )
    )

    # =================================================
    # 4. DEDUPLICATION
    # Keep the latest updated version of each order
    # =================================================

    window_spec = Window.partitionBy("order_id").orderBy(
        col("updated_at").desc_nulls_last()
    )

    df = df.withColumn(
        "row_num",
        row_number().over(window_spec)
    )

    df = df.withColumn(
        "reject_reason",
        when(
            col("row_num") > 1,
            concat_ws(
                "; ",
                col("reject_reason"),
                lit("Duplicate order_id")
            )
        ).otherwise(col("reject_reason"))
    )

    return df


# =====================================================
# 5. SILVER ORDERS
# One row per valid order
# =====================================================

@dp.table(name="fk_1393.silver.orders")
def orders():

    df = prepare_orders()

    return (
        df.filter(
            (col("reject_reason") == "")
            & (col("row_num") == 1)
        )
        .select(
            "order_id",
            "customer_id",
            "store_id",
            "order_ts",
            "order_date",
            "order_status",
            "payment_method",
            "channel",
            col("delivery.promised_mins").cast("int").alias("promised_mins"),
            col("delivery.actual_mins").cast("int").alias("actual_mins"),
            col("delivery.delivery_fee").cast("decimal(10,2)").alias("delivery_fee"),
            "updated_at"
        )
    )


# =====================================================
# 6. ORDERS QUARANTINE
# =====================================================

@dp.table(name="fk_1393.data_quality.orders_quarantine")
def orders_quarantine():

    df = prepare_orders()

    return (
        df.filter(
            (col("reject_reason") != "")
            | (col("row_num") > 1)
        )
        .select(
            "order_id",
            col("reject_reason"),
            "updated_at",
            "raw_record",
            current_timestamp().alias("quarantined_at")
        )
    )


# =====================================================
# 7. SILVER ORDER ITEMS
# S7, S8, S9, S10
# =====================================================

@dp.table(name="fk_1393.silver.order_items")
def order_items():

    df = prepare_orders()

    # Only use valid orders
    df = df.filter(
        (col("reject_reason") == "")
        & (col("row_num") == 1)
    )

    # Preserve original item position
    df = df.select(
        "order_id",
        posexplode("items").alias("item_pos", "item")
    )

    df = df.select(
        "order_id",
        (col("item_pos") + 1).cast("int").alias("line_number"),
        upper(trim(col("item.product_id"))).alias("product_id"),
        expr("try_cast(item.qty AS DOUBLE)").alias("qty_value"),
        expr("try_cast(item.unit_price AS DECIMAL(10,2))").alias("unit_price"),
        coalesce(
            expr("try_cast(item.discount AS DECIMAL(10,2))"),
            lit(0).cast("decimal(10,2)")
        ).alias("discount_amount")
    )

    # S7: Quantity must be a whole number greater than zero
    # S8: Unit price must be greater than zero
    # S9: Missing discount becomes zero
    # S10: Line number is the original array position

    df = df.filter(
        col("product_id").isNotNull()
        & (col("product_id") != "")
        & col("qty_value").isNotNull()
        & (col("qty_value") > 0)
        & (col("qty_value") == expr("floor(qty_value)"))
        & col("unit_price").isNotNull()
        & (col("unit_price") > 0)
    )

    df = df.withColumn("quantity", col("qty_value").cast("int"))

    df = df.withColumn(
        "gross_amount",
        (col("quantity") * col("unit_price")).cast("decimal(12,2)")
    )

    df = df.withColumn(
        "net_amount",
        (col("gross_amount") - col("discount_amount")).cast("decimal(12,2)")
    )

    return df.select(
        "order_id",
        "line_number",
        "product_id",
        "quantity",
        "unit_price",
        "discount_amount",
        "gross_amount",
        "net_amount"
    )