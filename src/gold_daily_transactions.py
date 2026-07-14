from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Gold layer - Daily transaction metrics and aggregations"
)
def gold_daily_transactions():
    """
    Aggregate transaction data by date to track:
    - Total transactions per day
    - Total revenue per day
    - Average transaction amount
    - Total quantity sold
    - Breakdown by category, store location, and payment method
    """
    df = spark.read.table("silver_transactions_clean")
    
    return (
        df.withColumn("transaction_day", F.to_date("transaction_date"))
        .groupBy("transaction_day")
        .agg(
            F.count("transaction_id").alias("total_transactions"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_transaction_amount"),
            F.sum("quantity").alias("total_quantity_sold"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.collect_set("category").alias("categories_sold"),
            F.collect_set("store_location").alias("store_locations"),
            F.collect_set("payment_method").alias("payment_methods")
        )
        .orderBy(F.desc("transaction_day"))
    )
