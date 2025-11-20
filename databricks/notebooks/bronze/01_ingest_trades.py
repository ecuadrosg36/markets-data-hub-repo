# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Ingest Trades
# MAGIC Ingest raw trade data into Bronze layer

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

sys.path.append("/Workspace/Repos/markets-data-hub-repo")

from config.config_loader import get_config
from utils.logger import setup_logger

# COMMAND ----------

# Initialize
config = get_config()
logger = setup_logger(__name__)

# COMMAND ----------

# Define schema for trades
trades_schema = StructType(
    [
        StructField("trade_id", StringType(), False),
        StructField("trade_date", DateType(), False),
        StructField("settlement_date", DateType(), True),
        StructField("instrument_id", StringType(), False),
        StructField("counterparty_id", StringType(), False),
        StructField("trade_type", StringType(), False),
        StructField("quantity", DecimalType(18, 4), False),
        StructField("price", DecimalType(18, 4), False),
        StructField("currency", StringType(), False),
        StructField("trader_id", StringType(), True),
        StructField("book_id", StringType(), True),
        StructField("status", StringType(), True),
    ]
)

# COMMAND ----------

# Read raw trade data
logger.info("Reading raw trade data")

raw_trades = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(trades_schema)
    .load(f"{config.get('data.source_path')}/trades/*.csv")
)

logger.info(f"Raw trades loaded: {raw_trades.count()} records")

# COMMAND ----------

# Add metadata columns
bronze_trades = (
    raw_trades.withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name())
    .withColumn("year", year(col("trade_date")))
    .withColumn("month", month(col("trade_date")))
)

# COMMAND ----------

# Data quality checks
logger.info("Performing data quality checks")

# Check for duplicates
duplicate_count = (
    bronze_trades.groupBy("trade_id").count().filter(col("count") > 1).count()
)
if duplicate_count > 0:
    logger.warning(f"Found {duplicate_count} duplicate trade_ids")

# Check for nulls in critical fields
null_checks = {
    "trade_id": bronze_trades.filter(col("trade_id").isNull()).count(),
    "trade_date": bronze_trades.filter(col("trade_date").isNull()).count(),
    "instrument_id": bronze_trades.filter(col("instrument_id").isNull()).count(),
}

for field, null_count in null_checks.items():
    if null_count > 0:
        logger.warning(f"Found {null_count} null values in {field}")

# COMMAND ----------

# Write to Bronze layer
logger.info(f"Writing to Bronze layer: {config.bronze_path}/trades")

(
    bronze_trades.write.format("delta")
    .mode("append")
    .partitionBy("year", "month")
    .save(f"{config.bronze_path}/trades")
)

logger.info("Bronze ingestion complete")

# COMMAND ----------

# Display sample
display(bronze_trades.limit(10))
