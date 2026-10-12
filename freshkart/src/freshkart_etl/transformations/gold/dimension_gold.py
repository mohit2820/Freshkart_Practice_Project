from pyspark import pipelines as dp
from pyspark.sql import functions as F
@dp.table(name="fk_1393.gold.dim_date")
def dim_date():
    df = (
        spark.sql("SELECT sequence(to_date('2026-03-01'), to_date('2026-03-31'), interval 1 day) as cal_date")
        .select(F.explode("cal_date").alias("calendar_date"))
        .withColumn("date_key", F.date_format("calendar_date", "yyyyMMdd").cast("int"))
        .withColumn("day_name", F.date_format("calendar_date", "EEEE"))
        .withColumn("iso_week", F.weekofyear("calendar_date"))
        .withColumn("is_weekend", F.dayofweek("calendar_date").isin([1,7]))
    )
    return df

# GOLD DIM STORE: All columns from silver.stores_silver
@dp.table(name="fk_1393.gold.dim_store")
def dim_store():
    df = spark.read.table("fk_1393.silver.stores_silver")
    df = df
    # Column comments:
    # store_id: Unique store identifier
    # store_name: Store display name
    # city: Store city location
    # state: Store state
    # region: Store region
    # opening_date: Store opening date
    return df

# GOLD DIM PRODUCT: All columns from silver.products_silver except tags and last_changed_snapshot_date
@dp.table(name="fk_1393.gold.dim_product")
def dim_product():
    df = spark.read.table("fk_1393.silver.products_silver")
    cols = [
        'brand', # Column comment: Brand name of the product
        'is_active', # Column comment: Whether product is active
        'mrp', # Column comment: Maximum retail price
        'product_id', # Column comment: Unique product identifier
        'product_name', # Column comment: Product display name
        'selling_price', # Column comment: Actual selling price
        'snapshot_date', # Column comment: Product snapshot date
        'category_l1', # Column comment: Top-level product category
        'category_l2', # Column comment: Second-level product category
        'pack_size', # Column comment: Number of items per pack
        'pack_unit' # Column comment: Unit for pack size
    ]
    df = df.select(*cols)
    return df

# GOLD DIM CUSTOMER: customer_sk (unique & not null), plus all columns from silver.customers_silver
@dp.table(name="fk_1393.gold.dim_customer")
def dim_customer():
    df = spark.read.table("fk_1393.silver.customers_silver")
    df = df.withColumn("customer_sk", F.monotonically_increasing_id()) # Column comment: Surrogate unique key for each customer version
    # Column comments:
    # customer_sk: Surrogate unique key for each customer version
    # customer_id: Original customer identifier
    # full_name: Customer's full name
    # email: Customer email address
    # phone: Customer phone number
    # city: Customer city
    # tier: Customer tier
    # signup_date: Customer signup date
    return df