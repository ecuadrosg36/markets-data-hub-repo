# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Normalize Trades
# MAGIC Clean and standardize trade data

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

sys.path.append("/Workspace/Repos/markets-data-hub-repo")

from config.config_loader import get_config
from utils.logger import setup_logger

# COMMAND ----------

config = get_config()
logger = setup_logger(__name__)

# COMMAND ----------

# Read from Bronze
logger.info("Reading from Bronze layer")

bronze_trades = spark.read.format("delta").load(f"{config.bronze_path}/trades")

logger.info(f"Bronze trades loaded: {bronze_trades.count()} records")

# COMMAND ----------

# Normalize and clean
silver_trades = (
    bronze_trades
    # Standardize trade_type
    .withColumn("trade_type", upper(trim(col("trade_type"))))
    # Standardize currency
    .withColumn("currency", upper(trim(col("currency"))))
    # Standardize status
    .withColumn(
        "status",
        when(col("status").isNull(), "OPEN").otherwise(upper(trim(col("status")))),
    )
    # Add business day calculation
    .withColumn("settlement_days", datediff(col("settlement_date"), col("trade_date")))
    # Calculate trade notional
    .withColumn("notional_value", col("quantity") * col("price"))
    # Add validation flags
    .withColumn(
        "is_valid",
        when(
            (col("quantity") > 0)
            & (col("price") > 0)
            & (col("trade_type").isin("BUY", "SELL"))
            & (col("settlement_date") >= col("trade_date")),
            lit(True),
        ).otherwise(lit(False)),
    )
    # Add processing timestamp
    .withColumn("silver_processed_at", current_timestamp())
)

# COMMAND ----------

# Remove duplicates based on trade_id
logger.info("Removing duplicates")

silver_trades_clean = silver_trades.dropDuplicates(["trade_id"])

logger.info(f"Records after deduplication:  {silver_trades_clean.count()}")

# COMMAND ----------

# Filter invalid records
invalid_count = silver_trades_clean.filter(col("is_valid") == False).count()
logger.info(f"Invalid records found: {invalid_count}")

silver_trades_final = silver_trades_clean.filter(col("is_valid") == True)

# COMMAND ----------

# Write to Silver layer
logger.info(f"Writing to Silver layer: {config.silver_path}/trades")

(
    silver_trades_final.write.format("delta")
    .mode("overwrite")
    .partitionBy("year", "month")
    .option("overwriteSchema", "true")
    .save(f"{config.silver_path}/trades")
)

logger.info("Silver trades normalization complete")

# COMMAND ----------

# Summary statistics
display(
    silver_trades_final.groupBy("trade_type", "currency", "status")
    .agg(
        count("*").alias("trade_count"),
        sum("notional_value").alias("total_notional"),
        avg("price").alias("avg_price"),
    )
    .orderBy("trade_type", "currency")
)
