from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Gold table: fk_1393.gold.agg_daily_store_sales
@dp.table(name="fk_1393.gold.agg_daily_store_sales")
def agg_daily_store_sales():
    orders = spark.read.table("fk_1393.silver.orders_silver")
    items = spark.read.table("fk_1393.silver.order_items_silver")
    stores = spark.read.table("fk_1393.silver.stores_silver")
    
    # Create a DATE column from order_ts (timestamp)
    orders = orders.withColumn("order_date", F.to_date("order_ts"))
    items = items.withColumn("order_date", F.to_date("order_ts"))
    
    # Store-date grid: every store for every date
    date_span = orders.select("order_date").distinct()
    store_span = stores.select("store_id", "city")
    grid = store_span.crossJoin(date_span)

    # Aggregations per store-date
    delivered_orders_expr = F.count(F.when(orders.order_status == "DELIVERED", orders.order_id))
    cancelled_orders_expr = F.count(F.when(orders.order_status == "CANCELLED", orders.order_id))
    gross_amount_expr = F.coalesce(F.sum(F.when(orders.order_status == "DELIVERED", orders.gross_amount)), F.lit(0))
    discount_amount_expr = F.coalesce(F.sum(F.when(orders.order_status == "DELIVERED", orders.discount_amount)), F.lit(0))
    net_item_revenue_expr = F.coalesce(F.sum(F.when(orders.order_status == "DELIVERED", orders.net_amount)), F.lit(0))
    delivery_fee_revenue_expr = F.coalesce(F.sum(F.when(orders.order_status == "DELIVERED", orders.delivery_fee)), F.lit(0))

    agg_orders = orders.groupBy("store_id", "order_date")\
        .agg(
            delivered_orders_expr.alias("delivered_orders"),
            cancelled_orders_expr.alias("cancelled_orders"),
            gross_amount_expr.alias("gross_amount"),
            discount_amount_expr.alias("discount_amount"),
            net_item_revenue_expr.alias("net_item_revenue"),
            delivery_fee_revenue_expr.alias("delivery_fee_revenue"),
            F.sum(F.when(orders.order_status == "DELIVERED", orders.items_count)).alias("units_sold"),
            F.sum(F.when(
                (orders.order_status == "DELIVERED") & 
                (orders.actual_mins <= orders.promised_mins), orders.order_id
            )).alias("on_time_orders")
        )

    # Units sold can also be from line items, but items_count from orders used here
    # Merge to store-date grid
    result = grid.join(agg_orders, ["store_id", "order_date"], "left")
    result = result.fillna(0, subset=[
        "delivered_orders", "cancelled_orders",
        "units_sold", "gross_amount", "discount_amount", "net_item_revenue", "delivery_fee_revenue", "on_time_orders"
    ])
    # Compute on_time_rate_pct
    result = result.withColumn(
        "on_time_rate_pct",
        F.when(
            result.delivered_orders > 0,
            F.round(100.0 * result.on_time_orders / result.delivered_orders, 2)
        ).otherwise(None)
    )
    return result
