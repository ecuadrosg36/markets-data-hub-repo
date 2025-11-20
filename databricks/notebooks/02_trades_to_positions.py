# Databricks PySpark — 02_trades_to_positions.py
from pyspark.sql import functions as F

raw_trades = dbutils.widgets.get("raw_trades")  # /mnt/raw/trades
silver_path = dbutils.widgets.get("silver_path")  # /mnt/silver

trades = (
    spark.read.option("header", "true")
    .csv(raw_trades)
    .withColumn("qty", F.col("qty").cast("double"))
    .withColumn("price", F.col("price").cast("double"))
    .withColumn("trade_date", F.to_date("trade_date"))
)

positions = trades.groupBy("instrument").agg(
    F.sum("qty").alias("position_qty"), F.avg("price").alias("avg_price")
)

positions.write.mode("overwrite").format("delta").save(f"{silver_path}/positions")
