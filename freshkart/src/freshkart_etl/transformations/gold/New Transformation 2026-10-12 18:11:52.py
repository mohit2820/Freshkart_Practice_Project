from pyspark import pipelines as dp
from pyspark.sql import functions as F
import datetime

@dp.table(name="fk_1393.silver.ops_reconciliation_results")
def ops_reconciliation_results():
    # SOURCE ORDER COUNT: distinct orders in silver.orders_silver
    order_count_source = spark.read.table("fk_1393.silver.orders_silver").select("order_id").distinct().count()

    # SOURCE ORDER COUNT: quarantine orders
    order_count_quarantine = spark.read.table("fk_1393.data_quality.orders_quarantine").select("order_id").distinct().count()
    order_count_source_total = order_count_source + order_count_quarantine

    # TARGET ORDER COUNT: total rows in orders_silver + quarantine
    order_count_target = spark.read.table("fk_1393.silver.orders_silver").count() + spark.read.table("fk_1393.data_quality.orders_quarantine").count()

    # LINE COUNT
    line_count_source = spark.read.table("fk_1393.silver.order_items_silver").count()
    line_count_target = spark.read.table("fk_1393.gold.fact_order_items").count()

    # NET REVENUE (DELIVERED orders only)
    delivered_orders_df = spark.read.table("fk_1393.silver.orders_silver").filter(F.col("order_status") == "DELIVERED").select("order_id")
    order_items = spark.read.table("fk_1393.silver.order_items_silver")
    delivered_items = order_items.join(delivered_orders_df, "order_id")
    net_revenue_source = delivered_items.agg(F.sum("line_amount")).collect()[0][0] or 0
    net_revenue_target = spark.read.table("fk_1393.gold.agg_daily_store_sales").agg(F.sum("net_item_revenue")).collect()[0][0] or 0

    # DELIVERED ORDERS
    delivered_count_source = delivered_orders_df.count()
    delivered_count_target = spark.read.table("fk_1393.gold.agg_daily_store_sales").agg(F.sum("delivered_orders")).collect()[0][0] or 0

    # CURRENT CUSTOMERS
    current_customers_source = spark.read.table("fk_1393.silver.customers_scd2").filter(F.col("is_current") == True).select("customer_id").distinct().count()
    current_customers_target = spark.read.table("fk_1393.gold.dim_customer").select("customer_id").distinct().count()

    now = datetime.datetime.now()
    results = [
        {"run_ts": now, "check_name": "ORDER_COUNT", "source_value": float(order_count_source_total), "target_value": float(order_count_target), "status": "PASS" if order_count_source_total == order_count_target else "FAIL"},
        {"run_ts": now, "check_name": "LINE_COUNT", "source_value": float(line_count_source), "target_value": float(line_count_target), "status": "PASS" if line_count_source == line_count_target else "FAIL"},
        {"run_ts": now, "check_name": "NET_REVENUE", "source_value": float(net_revenue_source), "target_value": float(net_revenue_target), "status": "PASS" if abs(net_revenue_source-net_revenue_target) < 1e-2 else "FAIL"},
        {"run_ts": now, "check_name": "DELIVERED_ORDERS", "source_value": float(delivered_count_source), "target_value": float(delivered_count_target), "status": "PASS" if delivered_count_source == delivered_count_target else "FAIL"},
        {"run_ts": now, "check_name": "CURRENT_CUSTOMERS", "source_value": float(current_customers_source), "target_value": float(current_customers_target), "status": "PASS" if current_customers_source == current_customers_target else "FAIL"},
    ]
    
    import pandas as pd
    return spark.createDataFrame(pd.DataFrame(results))
