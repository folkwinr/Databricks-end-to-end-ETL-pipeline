from pyspark import pipelines as dp

@dp.table(
    comment="Bronze layer - Raw transactions from source pipeline"
)
def bronze_transactions():
    """
    Read streaming data from the source pipeline transactions table.
    This is a pass-through layer preserving all raw data.
    """
    return spark.readStream.table("dataengineering.endtoend.transactions")
