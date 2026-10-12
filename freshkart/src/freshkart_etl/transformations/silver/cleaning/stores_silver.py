from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    initcap,
    to_date,
    when,
    concat_ws,
    row_number
)
from pyspark.sql.window import Window

# ============================================================
# 1. CONFIGURATION
# ============================================================

dbutils.widgets.text("CATALOG", "fk_1393")
CATALOG = dbutils.widgets.get("CATALOG")

# ============================================================
# 2. TABLE LOCATIONS
# ============================================================
BRONZE_TABLE = f"{CATALOG}.bronze.stores"
SILVER_TABLE = f"{CATALOG}.silver.stores_silver"
QUARANTINE_TABLE = f"{CATALOG}.data_quality.stores_quarantine"

# ============================================================
# 3. STANDARDISATION
# ============================================================

def standardize_stores(df):

    df = (
        df

        # Store ID: trim spaces and uppercase
        .withColumn(
            "store_id",
            upper(trim(col("store_id").cast("string")))
        )

        # Store name: remove surrounding spaces
        .withColumn(
            "store_name",
            trim(col("store_name"))
        )

        # City: trim and standardize capitalization
        .withColumn(
            "city",
            initcap(trim(col("city")))
        )

        # State: standardize only, no fixed allowed-state list
        .withColumn(
            "state",
            initcap(trim(col("state")))
        )

        # Region: trim and standardize capitalization
        .withColumn(
            "region",
            initcap(trim(col("region")))
        )

        # Opening date: parse yyyy-MM-dd into DATE
        .withColumn(
            "opening_date",
            to_date(
                trim(col("opening_date").cast("string")),
                "yyyy-MM-dd"
            )
        )
    )

    return df


# ============================================================
# 4. DEDUPLICATION - KEEP LATEST BY _ingested_at
# ============================================================

def deduplicate_stores(df):

    store_window = (
        Window
        .partitionBy("store_id")
        .orderBy(col("_ingested_at").desc_nulls_last())
    )

    df = (
        df
        .withColumn(
            "_row_num",
            row_number().over(store_window)
        )
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    return df


# ============================================================
# 5. DATA QUALITY RULES
# ============================================================

def apply_dq_rules(df):

    df = (
        df

        # DQ-STORE-001:
        # Store ID must start with S followed by minimum 2 digits
        .withColumn(
            "dq_store_id",
            when(
                col("store_id").isNull()
                | (trim(col("store_id")) == "")
                | (~col("store_id").rlike(r"^S[0-9]{2,}$")),
                "INVALID_STORE_ID"
            )
        )

        # DQ-STORE-002: Store name is mandatory
        .withColumn(
            "dq_store_name",
            when(
                col("store_name").isNull()
                | (trim(col("store_name")) == ""),
                "INVALID_STORE_NAME"
            )
        )

        # DQ-STORE-003: City is mandatory
        .withColumn(
            "dq_city",
            when(
                col("city").isNull()
                | (trim(col("city")) == ""),
                "INVALID_CITY"
            )
        )

        # DQ-STORE-004: Region must be an allowed value
        .withColumn(
            "dq_region",
            when(
                col("region").isNull()
                | (~col("region").isin(
                    "North",
                    "South",
                    "West",
                    "East"
                )),
                "INVALID_REGION"
            )
        )

        # DQ-STORE-005: Opening date must be valid
        .withColumn(
            "dq_opening_date",
            when(
                col("opening_date").isNull(),
                "INVALID_OPENING_DATE"
            )
        )

        # Combine all applicable DQ errors
        .withColumn(
            "dq_error",
            concat_ws(
                ", ",
                col("dq_store_id"),
                col("dq_store_name"),
                col("dq_city"),
                col("dq_region"),
                col("dq_opening_date")
            )
        )
    )

    return df


# ============================================================
# 6. VALID RECORDS
# ============================================================

def get_valid_records(df):

    return df.filter(
        col("dq_store_id").isNull()
        & col("dq_store_name").isNull()
        & col("dq_city").isNull()
        & col("dq_region").isNull()
        & col("dq_opening_date").isNull()
    )


# ============================================================
# 7. QUARANTINE TABLE
# ============================================================

@dp.materialized_view(name=QUARANTINE_TABLE)
def stores_quarantine():

    df = spark.read.table(BRONZE_TABLE)

    df = standardize_stores(df)
    df = deduplicate_stores(df)
    df = apply_dq_rules(df)

    # Keep records having one or more DQ failures
    df = df.filter(
        col("dq_store_id").isNotNull()
        | col("dq_store_name").isNotNull()
        | col("dq_city").isNotNull()
        | col("dq_region").isNotNull()
        | col("dq_opening_date").isNotNull()
    )

    return df


# ============================================================
# 8. SILVER TABLE
# ============================================================

@dp.materialized_view(name=SILVER_TABLE)
def stores_silver():

    df = spark.read.table(BRONZE_TABLE)

    df = standardize_stores(df)
    df = deduplicate_stores(df)
    df = apply_dq_rules(df)
    df = get_valid_records(df)

    # Remove DQ columns from Silver output
    df = df.drop(
        "dq_store_id",
        "dq_store_name",
        "dq_city",
        "dq_region",
        "dq_opening_date",
        "dq_error"
    )

    # Remove Bronze ingestion metadata
    metadata_columns = [
        "_source_file",
        "_ingested_at"
    ]

    df = df.drop(*metadata_columns)

    return df
