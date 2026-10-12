from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    lit,
    coalesce,
    when,
    concat_ws,
    try_to_timestamp,
    from_json,
    transform,
    exists,
    aggregate,
    size,
    struct,
    posexplode,
    explode,
    expr,
    row_number
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    ArrayType
)


# ============================================================
# 1. CONFIGURATION
# ============================================================

dbutils.widgets.text("CATALOG", "fk_1393")
CATALOG = dbutils.widgets.get("CATALOG")


# ============================================================
# 2. TABLE LOCATIONS
# ============================================================

BRONZE_TABLE = f"{CATALOG}.bronze.orders"
SILVER_TABLE = f"{CATALOG}.silver.orders_silver"
ITEMS_TABLE = f"{CATALOG}.silver.order_items_silver"
QUARANTINE_TABLE = f"{CATALOG}.data_quality.orders_quarantine"

# Reference tables for referential integrity checks
CUSTOMERS_SILVER_TABLE = f"{CATALOG}.silver.customers_silver"
STORES_SILVER_TABLE = f"{CATALOG}.silver.stores_silver"
PRODUCTS_SILVER_TABLE = f"{CATALOG}.silver.products_silver"


# ============================================================
# 3. JSON SCHEMAS FOR NESTED FIELDS
# ============================================================

delivery_schema = StructType([
    StructField("promised_mins", IntegerType()),
    StructField("actual_mins", IntegerType()),
    StructField("delivery_fee", DoubleType())
])

payment_schema = StructType([
    StructField("method", StringType()),
    StructField("txn_ref", StringType())
])

item_schema = StructType([
    StructField("product_id", StringType()),
    StructField("qty", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("discount", DoubleType())
])

items_schema = ArrayType(item_schema)


# ============================================================
# 4. READ BRONZE + STANDARDISATION
# ============================================================

def get_orders():

    df = spark.read.table(BRONZE_TABLE)

    # Parse the nested JSON string columns
    df = (
        df
        .withColumn("_delivery", from_json(col("delivery"), delivery_schema))
        .withColumn("_payment", from_json(col("payment"), payment_schema))
        .withColumn("_items", from_json(col("items"), items_schema))
    )

    df = (
        df
        # Order ID: trim and uppercase
        .withColumn(
            "order_id",
            upper(trim(col("order_id").cast("string")))
        )

        # Customer ID: trim and uppercase
        .withColumn(
            "customer_id",
            upper(trim(col("customer_id").cast("string")))
        )

        # Store ID: trim and uppercase
        .withColumn(
            "store_id",
            upper(trim(col("store_id").cast("string")))
        )

        # Order status: trim and uppercase
        .withColumn(
            "order_status",
            upper(trim(col("order_status")))
        )

        # Order timestamp: support 'yyyy-MM-dd HH:mm:ss' and ISO 'yyyy-MM-ddTHH:mm:ss'
        .withColumn(
            "order_ts",
            coalesce(
                try_to_timestamp(
                    trim(col("order_ts").cast("string")),
                    lit("yyyy-MM-dd HH:mm:ss")
                ),
                try_to_timestamp(
                    trim(col("order_ts").cast("string")),
                    lit("yyyy-MM-dd'T'HH:mm:ss")
                )
            )
        )

        # Updated timestamp: same two supported formats
        .withColumn(
            "updated_at",
            coalesce(
                try_to_timestamp(
                    trim(col("updated_at").cast("string")),
                    lit("yyyy-MM-dd HH:mm:ss")
                ),
                try_to_timestamp(
                    trim(col("updated_at").cast("string")),
                    lit("yyyy-MM-dd'T'HH:mm:ss")
                )
            )
        )

        # Delivery fields
        .withColumn("promised_mins", col("_delivery.promised_mins"))
        .withColumn("actual_mins", col("_delivery.actual_mins"))
        .withColumn(
            "delivery_fee",
            col("_delivery.delivery_fee").cast("decimal(10,2)")
        )

        # Payment fields
        .withColumn(
            "payment_method",
            upper(trim(col("_payment.method")))
        )
        .withColumn(
            "txn_ref",
            upper(trim(col("_payment.txn_ref")))
        )

        # Items: standardise each line item (missing discount defaults to 0)
        .withColumn(
            "items",
            transform(
                col("_items"),
                lambda item: struct(
                    upper(trim(item.getField("product_id"))).alias("product_id"),
                    item.getField("qty").cast("int").alias("qty"),
                    item.getField("unit_price").cast("decimal(10,2)").alias("unit_price"),
                    coalesce(
                        item.getField("discount").cast("decimal(10,2)"),
                        lit(0.0).cast("decimal(10,2)")
                    ).alias("discount")
                )
            )
        )

        # Fix missing actual delivery minutes: for delivered orders,
        # derive from the gap between order timestamp and last update
        .withColumn(
            "actual_mins",
            when(
                (col("order_status") == "DELIVERED")
                & col("actual_mins").isNull()
                & col("order_ts").isNotNull()
                & col("updated_at").isNotNull(),
                expr("timestampdiff(MINUTE, order_ts, updated_at)")
            ).otherwise(col("actual_mins"))
        )
    )

    return df


# ============================================================
# 5. RANK ORDER RECORDS
# ============================================================

def rank_order_records(df):

    order_window = (
        Window
        .partitionBy("order_id")
        .orderBy(
            col("_ingested_at").desc_nulls_last()
        )
    )

    return df.withColumn(
        "_row_number",
        row_number().over(order_window)
    )


# ============================================================
# 6. REFERENTIAL INTEGRITY CHECKS
# ============================================================

def apply_referential_checks(df):

    # Every order must reference a known customer
    valid_customers = (
        spark.read.table(CUSTOMERS_SILVER_TABLE)
        .select(col("customer_id").alias("_ref_customer_id"))
    )

    df = (
        df
        .join(
            valid_customers,
            col("customer_id") == col("_ref_customer_id"),
            "left"
        )
        .withColumn(
            "_customer_ref_missing",
            col("_ref_customer_id").isNull()
        )
        .drop("_ref_customer_id")
    )

    # Every order must reference a known store
    valid_stores = (
        spark.read.table(STORES_SILVER_TABLE)
        .select(col("store_id").alias("_ref_store_id"))
    )

    df = (
        df
        .join(
            valid_stores,
            col("store_id") == col("_ref_store_id"),
            "left"
        )
        .withColumn(
            "_store_ref_missing",
            col("_ref_store_id").isNull()
        )
        .drop("_ref_store_id")
    )

    # Every line item must reference a known product
    valid_products = (
        spark.read.table(PRODUCTS_SILVER_TABLE)
        .select(col("product_id").alias("_ref_product_id"))
    )

    order_products = (
        df
        .select("order_id", explode(col("items")).alias("_item"))
        .select("order_id", col("_item.product_id").alias("product_id"))
        .distinct()
    )

    unknown_product_orders = (
        order_products
        .join(
            valid_products,
            col("product_id") == col("_ref_product_id"),
            "left_anti"
        )
        .select("order_id")
        .withColumn("_product_ref_missing", lit(True))
    )

    df = (
        df
        .join(unknown_product_orders, on="order_id", how="left")
        .withColumn(
            "_product_ref_missing",
            coalesce(col("_product_ref_missing"), lit(False))
        )
    )

    return df


# ============================================================
# 6. DATA QUALITY RULES
# ============================================================

def apply_dq_rules(df):

    df = (
        df
        # DQ-001: Order ID must be FK-date-number
        .withColumn(
            "dq_order_id",
            when(
                col("order_id").isNull()
                | (trim(col("order_id")) == "")
                | (~col("order_id").rlike(r"^FK-[0-9]{8}-[0-9]+$")),
                "INVALID_ORDER_ID"
            )
        )

        # DQ-002: Customer ID must be C followed by digits
        .withColumn(
            "dq_customer_id",
            when(
                col("customer_id").isNull()
                | (trim(col("customer_id")) == "")
                | (~col("customer_id").rlike(r"^C[0-9]+$")),
                "INVALID_CUSTOMER_ID"
            )
        )

        # DQ-003: Store ID must be S followed by digits
        .withColumn(
            "dq_store_id",
            when(
                col("store_id").isNull()
                | (trim(col("store_id")) == "")
                | (~col("store_id").rlike(r"^S[0-9]+$")),
                "INVALID_STORE_ID"
            )
        )

        # DQ-004: Status must be a known order status
        .withColumn(
            "dq_order_status",
            when(
                col("order_status").isNull()
                | (~col("order_status").isin("PLACED", "DELIVERED", "CANCELLED")),
                "INVALID_ORDER_STATUS"
            )
        )

        # DQ-005: Order timestamp must parse successfully
        .withColumn(
            "dq_order_ts",
            when(
                col("order_ts").isNull(),
                "INVALID_ORDER_TS"
            )
        )

        # DQ-006: Updated timestamp must parse and not precede order timestamp
        .withColumn(
            "dq_updated_at",
            when(
                col("updated_at").isNull()
                | (
                    col("order_ts").isNotNull()
                    & (col("updated_at") < col("order_ts"))
                ),
                "INVALID_UPDATED_AT"
            )
        )

        # DQ-007: Promised minutes must be positive
        .withColumn(
            "dq_promised_mins",
            when(
                col("promised_mins").isNull()
                | (col("promised_mins") <= 0),
                "INVALID_PROMISED_MINS"
            )
        )

        # DQ-008: Delivery fee must not be negative
        .withColumn(
            "dq_delivery_fee",
            when(
                col("delivery_fee").isNull()
                | (col("delivery_fee") < 0),
                "INVALID_DELIVERY_FEE"
            )
        )

        # DQ-009: Payment method must be a known method
        .withColumn(
            "dq_payment_method",
            when(
                col("payment_method").isNull()
                | (~col("payment_method").isin("UPI", "CARD", "COD", "WALLET")),
                "INVALID_PAYMENT_METHOD"
            )
        )

        # DQ-010: Transaction reference must be TXN followed by digits
        .withColumn(
            "dq_txn_ref",
            when(
                col("txn_ref").isNull()
                | (~col("txn_ref").rlike(r"^TXN[0-9]+$")),
                "INVALID_TXN_REF"
            )
        )

        # DQ-011: Items must exist and each line item must be valid
        .withColumn(
            "dq_items",
            when(
                col("items").isNull()
                | (size(col("items")) == 0),
                "EMPTY_ITEMS"
            ).when(
                exists(
                    col("items"),
                    lambda item: (
                        item.getField("product_id").isNull()
                        | (~item.getField("product_id").rlike(r"^P[0-9]+$"))
                        | item.getField("qty").isNull()
                        | (item.getField("qty") <= 0)
                        | item.getField("unit_price").isNull()
                        | (item.getField("unit_price") <= 0)
                        | item.getField("discount").isNull()
                        | (item.getField("discount") < 0)
                    )
                ),
                "INVALID_ITEM"
            )
        )

        # DQ-012: Delivered orders must have an actual delivery time
        .withColumn(
            "dq_actual_mins",
            when(
                (col("order_status") == "DELIVERED")
                & col("actual_mins").isNull(),
                "MISSING_ACTUAL_MINS"
            )
        )

        # DQ-013: Order must reference a known customer
        .withColumn(
            "dq_customer_ref",
            when(
                col("_customer_ref_missing"),
                "UNKNOWN_CUSTOMER_REF"
            )
        )

        # DQ-014: Order must reference a known store
        .withColumn(
            "dq_store_ref",
            when(
                col("_store_ref_missing"),
                "UNKNOWN_STORE_REF"
            )
        )

        # DQ-015: All line items must reference known products
        .withColumn(
            "dq_product_ref",
            when(
                col("_product_ref_missing"),
                "UNKNOWN_PRODUCT_REF"
            )
        )

        # Combine all applicable DQ errors
        .withColumn(
            "dq_error",
            concat_ws(
                ", ",
                col("dq_order_id"),
                col("dq_customer_id"),
                col("dq_store_id"),
                col("dq_order_status"),
                col("dq_order_ts"),
                col("dq_updated_at"),
                col("dq_promised_mins"),
                col("dq_delivery_fee"),
                col("dq_payment_method"),
                col("dq_txn_ref"),
                col("dq_items"),
                col("dq_actual_mins"),
                col("dq_customer_ref"),
                col("dq_store_ref"),
                col("dq_product_ref"),
                when(
                    col("_row_number") > 1,
                    lit("OLDER_DUPLICATE_RECORD")
                )
            )
        )
    )

    return df


def get_valid_orders(df):

    return df.filter(
        col("dq_order_id").isNull()
        & col("dq_customer_id").isNull()
        & col("dq_store_id").isNull()
        & col("dq_order_status").isNull()
        & col("dq_order_ts").isNull()
        & col("dq_updated_at").isNull()
        & col("dq_promised_mins").isNull()
        & col("dq_delivery_fee").isNull()
        & col("dq_payment_method").isNull()
        & col("dq_txn_ref").isNull()
        & col("dq_items").isNull()
        & col("dq_actual_mins").isNull()
        & col("dq_customer_ref").isNull()
        & col("dq_store_ref").isNull()
        & col("dq_product_ref").isNull()
        & (col("_row_number") == 1)
    )


# ============================================================
# 7. QUARANTINE TABLE
# ============================================================

@dp.materialized_view(
    name=QUARANTINE_TABLE,
    comment="Invalid order records and older duplicate order records"
)
def orders_quarantine():

    df = get_orders()

    # Check referential integrity before applying DQ rules
    df = apply_referential_checks(df)

    # Rank records before applying DQ rules
    df = rank_order_records(df)

    df = apply_dq_rules(df)

    # Keep invalid records and older duplicate records
    df = df.filter(
        col("dq_order_id").isNotNull()
        | col("dq_customer_id").isNotNull()
        | col("dq_store_id").isNotNull()
        | col("dq_order_status").isNotNull()
        | col("dq_order_ts").isNotNull()
        | col("dq_updated_at").isNotNull()
        | col("dq_promised_mins").isNotNull()
        | col("dq_delivery_fee").isNotNull()
        | col("dq_payment_method").isNotNull()
        | col("dq_txn_ref").isNotNull()
        | col("dq_items").isNotNull()
        | col("dq_actual_mins").isNotNull()
        | col("dq_customer_ref").isNotNull()
        | col("dq_store_ref").isNotNull()
        | col("dq_product_ref").isNotNull()
        | (col("_row_number") > 1)
    )

    # Remove internal processing columns and raw nested JSON strings
    df = df.drop(
        "_row_number",
        "_delivery",
        "_payment",
        "_items",
        "delivery",
        "payment",
        "items",
        "_customer_ref_missing",
        "_store_ref_missing",
        "_product_ref_missing"
    )

    return df


# ============================================================
# 8. SILVER ORDERS TABLE
# ============================================================

@dp.materialized_view(
    name=SILVER_TABLE,
    comment="Standardized, validated and deduplicated orders with order-level amounts"
)
def orders_silver():

    df = get_orders()

    # Check referential integrity before applying DQ rules
    df = apply_referential_checks(df)

    # Rank records and apply DQ rules
    df = rank_order_records(df)
    df = apply_dq_rules(df)

    df = get_valid_orders(df)

    # Order-level amounts computed from the line items
    df = (
        df
        .withColumn(
            "items_count",
            size(col("items"))
        )
        .withColumn(
            "total_qty",
            aggregate(
                col("items"),
                lit(0).cast("int"),
                lambda acc, item: acc + item.getField("qty")
            )
        )
        .withColumn(
            "gross_amount",
            aggregate(
                col("items"),
                lit(0.0).cast("decimal(18,2)"),
                lambda acc, item: (
                    acc + (item.getField("qty") * item.getField("unit_price"))
                ).cast("decimal(18,2)")
            )
        )
        .withColumn(
            "discount_amount",
            aggregate(
                col("items"),
                lit(0.0).cast("decimal(18,2)"),
                lambda acc, item: (
                    acc + item.getField("discount")
                ).cast("decimal(18,2)")
            )
        )
    )

    df = df.withColumn(
        "net_amount",
        col("gross_amount")
        - col("discount_amount")
        + col("delivery_fee")
    )

    # Remove internal processing columns, raw JSON and nested columns
    df = df.drop(
        "_row_number",
        "_delivery",
        "_payment",
        "_items",
        "items",
        "delivery",
        "payment",
        "dq_order_id",
        "dq_customer_id",
        "dq_store_id",
        "dq_order_status",
        "dq_order_ts",
        "dq_updated_at",
        "dq_promised_mins",
        "dq_delivery_fee",
        "dq_payment_method",
        "dq_txn_ref",
        "dq_items",
        "dq_actual_mins",
        "dq_customer_ref",
        "dq_store_ref",
        "dq_product_ref",
        "dq_error",
        "_customer_ref_missing",
        "_store_ref_missing",
        "_product_ref_missing",
        "_rescued_data",
        "_source_file",
        "_ingested_at"
    )

    return df


# ============================================================
# 9. SILVER ORDER ITEMS TABLE
# ============================================================

@dp.materialized_view(
    name=ITEMS_TABLE,
    comment="Line items exploded from validated orders, one row per product line"
)
def order_items_silver():

    df = get_orders()

    # Check referential integrity before applying DQ rules
    df = apply_referential_checks(df)

    # Rank records and apply DQ rules
    df = rank_order_records(df)
    df = apply_dq_rules(df)

    df = get_valid_orders(df)

    # Explode items into one row per line item
    df = (
        df
        .select(
            "order_id",
            "customer_id",
            "store_id",
            "order_ts",
            "order_status",
            posexplode(col("items")).alias("line_index", "item")
        )
        .select(
            "order_id",
            "customer_id",
            "store_id",
            "order_ts",
            "order_status",
            (col("line_index") + 1).alias("line_number"),
            col("item.product_id").alias("product_id"),
            col("item.qty").alias("qty"),
            col("item.unit_price").alias("unit_price"),
            col("item.discount").alias("discount")
        )
        .withColumn(
            "line_amount",
            (col("qty") * col("unit_price")) - col("discount")
        )
    )

    return df
