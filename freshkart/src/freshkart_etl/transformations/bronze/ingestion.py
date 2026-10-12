
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

# ============================================================
# 1. CONFIGURATION
# ============================================================

dbutils.widgets.text("CATALOG", "fk_1393")
CATALOG = dbutils.widgets.get("CATALOG")

# Source files stored in Unity Catalog volumes
LANDING_PATH = f"/Volumes/{CATALOG}/landing/raw"

# Auto Loader state stored in the ops schema's pipeline_state volume
STATE_PATH = f"/Volumes/{CATALOG}/ops/pipeline_state"

# ============================================================
# 2. SOURCE PATHS
# ============================================================

STORES_PATH = f"{LANDING_PATH}/stores"
CUSTOMERS_PATH = f"{LANDING_PATH}/customers"

PRODUCTS_PATH = f"{LANDING_PATH}/products"
ORDERS_PATH = f"{LANDING_PATH}/orders"

# ============================================================
# 3. B1 - BRONZE STORES
# All source columns must remain STRING
# ============================================================

@dp.table(
    name="stores",
    comment="Raw stores CSV data; all source columns are strings"
)
def bronze_stores():

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(STORES_PATH)
    )

    df = (
        df
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_ingested_at", current_timestamp())
    )

    return df


# ============================================================
# 4. B3 - BRONZE PRODUCTS
# Preserve category and pack as STRUCTs and tags as ARRAY
# ============================================================

@dp.table(
    name="products",
    comment="Raw product snapshots with nested JSON structures"
)
def bronze_products():

    df = (
        spark.read
        .format("json")
        .option("multiLine", "true")
        .load(PRODUCTS_PATH)
    )

    df = (
        df
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_ingested_at", current_timestamp())
    )

    return df


# ============================================================
# 5. B4 - BRONZE ORDERS
# Auto Loader with incremental file processing,
# schema evolution and rescued data
# ============================================================

@dp.table(
    name="orders",
    comment="Raw orders ingested incrementally using Auto Loader"
)
def bronze_orders():

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option(
            "cloudFiles.schemaLocation",
            f"{STATE_PATH}/orders_schema"
        )
        .option(
            "cloudFiles.schemaEvolutionMode",
            "addNewColumns"
        )
        .option(
            "rescuedDataColumn",
            "_rescued_data"
        )
        .load(ORDERS_PATH)
    )

    df = (
        df
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_ingested_at", current_timestamp())
    )

    return df
