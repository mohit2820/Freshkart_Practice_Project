from pyspark import pipelines as dp

@dp.table(name="fk_1393.gold.v_kpi_summary")
def v_kpi_summary():
    orders = spark.read.table("fk_1393.silver.orders_silver")
    order_items = spark.read.table("fk_1393.silver.order_items_silver")

    delivered_orders = orders.filter(orders.order_status == "DELIVERED")
    cancelled_orders = orders.filter(orders.order_status == "CANCELLED")
    on_time_orders = delivered_orders.filter(delivered_orders.actual_mins <= delivered_orders.promised_mins)

    delivered_orders_count = delivered_orders.count()
    cancelled_orders_count = cancelled_orders.count()
    on_time_orders_count = on_time_orders.count()
    net_item_revenue = order_items.filter(order_items.order_status == "DELIVERED").agg({"line_amount": "sum"}).collect()[0][0]

    avg_order_value = net_item_revenue / delivered_orders_count if delivered_orders_count > 0 else None
    on_time_rate_pct = 100 * on_time_orders_count / delivered_orders_count if delivered_orders_count > 0 else None
    cancellation_rate_pct = 100 * cancelled_orders_count / (delivered_orders_count + cancelled_orders_count) if (delivered_orders_count + cancelled_orders_count) > 0 else None

    # Active customers: distinct customers with at least one delivered order
    active_customer_ids = delivered_orders.select("customer_id").distinct()
    active_customers_count = active_customer_ids.count()

    # Repeat customer rate: customers with 2+ delivered orders
    repeat_customers_count = delivered_orders.groupBy("customer_id").count().filter("count >= 2").count()
    repeat_customer_rate_pct = 100 * repeat_customers_count / active_customers_count if active_customers_count > 0 else None

    import pyspark.sql.functions as F
    # Return the single-row DataFrame
    return spark.createDataFrame([
        {
            "delivered_orders": delivered_orders_count,
            "cancelled_orders": cancelled_orders_count,
            "net_item_revenue": float(net_item_revenue) if net_item_revenue is not None else None,
            "avg_order_value": float(avg_order_value) if avg_order_value is not None else None,
            "on_time_rate_pct": float(on_time_rate_pct) if on_time_rate_pct is not None else None,
            "cancellation_rate_pct": float(cancellation_rate_pct) if cancellation_rate_pct is not None else None,
            "active_customers": active_customers_count,
            "repeat_customer_rate_pct": float(repeat_customer_rate_pct) if repeat_customer_rate_pct is not None else None
        }
    ])
