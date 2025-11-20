# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Ingest Market Prices
# MAGIC Ingest EOD market prices into Bronze layer

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

# Define schema
prices_schema = StructType(
    [
        StructField("price_id", StringType(), False),
        StructField("price_date", DateType(), False),
        StructField("instrument_id", StringType(), False),
        StructField("open_price", DecimalType(18, 4), True),
        StructField("high_price", DecimalType(18, 4), True),
        StructField("low_price", DecimalType(18, 4), True),
        StructField("close_price", DecimalType(18, 4), False),
        StructField("volume", LongType(), True),
        StructField("bid_price", DecimalType(18, 4), True),
        StructField("ask_price", DecimalType(18, 4), True),
        StructField("source", StringType(), True),
    ]
)

# COMMAND ----------

logger.info("Reading raw market prices")

raw_prices = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(prices_schema)
    .load(f"{config.get('data.source_path')}/prices/*.csv")
)

logger.info(f"Raw prices loaded: {raw_prices.count()} records")

# COMMAND ----------

# Add metadata
bronze_prices = (
    raw_prices.withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", input_file_name())
    .withColumn("year", year(col("price_date")))
    .withColumn("month", month(col("price_date")))
)

# COMMAND ----------

# Data quality - price validation
logger.info("Validating price data")

# Check for negative prices
negative_prices = bronze_prices.filter(col("close_price") < 0).count()
if negative_prices > 0:
    logger.error(f"Found {negative_prices} records with negative close_price")

# Check for price anomalies (high > low violation)
price_anomalies = bronze_prices.filter(
    (col("high_price").isNotNull())
    & (col("low_price").isNotNull())
    & (col("high_price") < col("low_price"))
).count()

if price_anomalies > 0:
    logger.warning(f"Found {price_anomalies} records where high_price < low_price")

# COMMAND ----------

# Write to Bronze
logger.info(f"Writing to Bronze layer: {config.bronze_path}/market_prices")

(
    bronze_prices.write.format("delta")
    .mode("append")
    .partitionBy("year", "month")
    .save(f"{config.bronze_path}/market_prices")
)

logger.info("Market prices ingestion complete")

# COMMAND ----------

display(bronze_prices.limit(10))
