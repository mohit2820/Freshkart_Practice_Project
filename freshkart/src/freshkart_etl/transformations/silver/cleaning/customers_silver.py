from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    lower,
    initcap,
    to_date,
    try_to_timestamp,
    lit,
    coalesce,
    when,
    concat_ws,
    regexp_replace,
    length,
    expr,
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

BRONZE_TABLE = f"{CATALOG}.bronze.customers"
SILVER_TABLE = f"{CATALOG}.silver.customers_silver"
QUARANTINE_TABLE = f"{CATALOG}.data_quality.customers_quarantine"


# ============================================================
# 3. READ BRONZE + STANDARDISATION
# ============================================================

def get_customers():

    df = spark.read.table(BRONZE_TABLE)

    df = (
        df
        # Customer ID: trim and uppercase
        .withColumn(
            "customer_id",
            upper(trim(col("customer_id").cast("string")))
        )

        # Full name: remove surrounding spaces
        .withColumn(
            "full_name",
            trim(col("full_name"))
        )

        # Email: trim and lowercase
        .withColumn(
            "email",
            lower(trim(col("email")))
        )

        # Phone: keep digits only in temporary column
        .withColumn(
            "_phone_digits",
            regexp_replace(
                trim(col("phone").cast("string")),
                r"[^0-9]",
                ""
            )
        )

        # Keep only the last 10 digits
        .withColumn(
            "phone",
            when(
                length(col("_phone_digits")) > 10,
                expr("right(_phone_digits, 10)")
            ).otherwise(col("_phone_digits"))
        )

        # City standardisation
        .withColumn(
            "city",
            initcap(trim(col("city")))
        )

        # Tier: trim and uppercase
        .withColumn(
            "tier",
            upper(trim(col("tier")))
        )

        # Signup date: support yyyy-MM-dd and dd/MM/yyyy
        .withColumn(
            "signup_date",
            coalesce(
                try_to_timestamp(
                    trim(col("signup_date").cast("string")),
                    lit("yyyy-MM-dd")
                ).cast("date"),
                try_to_timestamp(
                    trim(col("signup_date").cast("string")),
                    lit("dd/MM/yyyy")
                ).cast("date")
            )
        )
    )

    return df


# ============================================================
# 4. RANK CUSTOMER RECORDS
# ============================================================

def rank_customer_records(df):

    customer_window = (
        Window
        .partitionBy("customer_id")
        .orderBy(
            col("_ingested_at").desc_nulls_last()
        )
    )

    return df.withColumn(
        "_row_number",
        row_number().over(customer_window)
    )


# ============================================================
# 5. DATA QUALITY RULES
# ============================================================

def apply_dq_rules(df):

    df = (
        df
        # DQ-001: Customer ID must be C followed by digits
        .withColumn(
            "dq_customer_id",
            when(
                col("customer_id").isNull()
                | (trim(col("customer_id")) == "")
                | (~col("customer_id").rlike(r"^C[0-9]+$")),
                "INVALID_CUSTOMER_ID"
            )
        )

        # DQ-002: Full name is mandatory
        .withColumn(
            "dq_full_name",
            when(
                col("full_name").isNull()
                | (trim(col("full_name")) == ""),
                "INVALID_FULL_NAME"
            )
        )

        # DQ-003: Email must have a valid format
        .withColumn(
            "dq_email",
            when(
                col("email").isNull()
                | (
                    ~col("email").rlike(
                        r"^[A-Za-z][A-Za-z0-9._%+-]*@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
                    )
                ),
                "INVALID_EMAIL"
            )
        )

        # DQ-004: Phone must contain exactly 10 digits
        .withColumn(
            "dq_phone",
            when(
                col("phone").isNull()
                | (~col("phone").rlike(r"^[0-9]{10}$")),
                "INVALID_PHONE"
            )
        )

        # DQ-005: City is mandatory
        .withColumn(
            "dq_city",
            when(
                col("city").isNull()
                | (trim(col("city")) == ""),
                "INVALID_CITY"
            )
        )

        # DQ-006: Tier is mandatory
        .withColumn(
            "dq_tier",
            when(
                col("tier").isNull()
                | (trim(col("tier")) == ""),
                "INVALID_TIER"
            )
        )

        # DQ-007: Signup date must parse successfully
        .withColumn(
            "dq_signup_date",
            when(
                col("signup_date").isNull(),
                "INVALID_SIGNUP_DATE"
            )
        )

        # Combine all applicable DQ errors
        .withColumn(
            "dq_error",
            concat_ws(
                ", ",
                col("dq_customer_id"),
                col("dq_full_name"),
                col("dq_email"),
                col("dq_phone"),
                col("dq_city"),
                col("dq_tier"),
                col("dq_signup_date"),
                when(
                    col("_row_number") > 1,
                    lit("OLDER_DUPLICATE_RECORD")
                )
            )
        )
    )

    return df


# ============================================================
# 6. VALID RECORDS
# ============================================================

def get_valid_records(df):

    return df.filter(
        col("dq_customer_id").isNull()
        & col("dq_full_name").isNull()
        & col("dq_email").isNull()
        & col("dq_phone").isNull()
        & col("dq_city").isNull()
        & col("dq_tier").isNull()
        & col("dq_signup_date").isNull()
    )


# ============================================================
# 7. QUARANTINE TABLE
# ============================================================

@dp.materialized_view(
    name=QUARANTINE_TABLE,
    comment="Invalid customer records and older duplicate records"
)
def customers_quarantine():

    df = get_customers()

    # Rank records before applying DQ rules
    df = rank_customer_records(df)

    df = apply_dq_rules(df)

    # Keep invalid records and older duplicate records
    df = df.filter(
        col("dq_customer_id").isNotNull()
        | col("dq_full_name").isNotNull()
        | col("dq_email").isNotNull()
        | col("dq_phone").isNotNull()
        | col("dq_city").isNotNull()
        | col("dq_tier").isNotNull()
        | col("dq_signup_date").isNotNull()
        | (col("_row_number") > 1)
    )

    # Remove internal processing columns
    df = df.drop(
        "_phone_digits",
        "_row_number"
    )

    return df


# ============================================================
# 8. SILVER TABLE
# ============================================================

@dp.materialized_view(
    name=SILVER_TABLE,
    comment="Standardized, validated and deduplicated customer records"
)
def customers_silver():

    df = get_customers()

    # Apply DQ rules
    df = rank_customer_records(df)
    df = apply_dq_rules(df)

    # Keep valid records only
    df = get_valid_records(df)

    # Keep latest valid record per customer
    df = (
        df
        .filter(col("_row_number") == 1)
        .drop(
            "_row_number",
            "dq_customer_id",
            "dq_full_name",
            "dq_email",
            "dq_phone",
            "dq_city",
            "dq_tier",
            "dq_signup_date",
            "dq_error",
            "_phone_digits"
        )
    )

    # Remove Bronze ingestion metadata if present
    metadata_columns = [
        "_source_file",
        "_ingested_at"
    ]

    df = df.drop(*metadata_columns)

    return df