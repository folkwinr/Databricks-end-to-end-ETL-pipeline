from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    comment="Silver layer - Cleaned and validated transactions"
)
@dp.expect_or_drop("valid_quantity", "quantity > 0")
@dp.expect_or_drop("valid_unit_price", "unit_price >= 0")
@dp.expect_or_drop("valid_total_amount", "total_amount >= 0")
def silver_transactions_clean():
    """
    Clean and validate transaction data:
    - Remove duplicates based on transaction_id
    - Filter out records with null critical fields
    - Validate business rules (positive quantities and prices)
    - Drop _rescued_data column
    """
    return (
        spark.readStream.table("bronze_transactions")
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("transaction_date").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["transaction_id"])
        .drop("_rescued_data")
    )
