# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Calculate VaR (Value at Risk)
# MAGIC Calculate historical VaR at 95% and 99% confidence levels

# COMMAND ----------

from pyspark.sql import SparkSession, Window
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

# Parameters
LOOKBACK_DAYS = config.get("risk.lookback_days", 252)  # 1 year
VAR_CONFIDENCE_95 = 0.95
VAR_CONFIDENCE_99 = 0.99

logger.info(f"VaR calculation with {LOOKBACK_DAYS} days lookback")

# COMMAND ----------

# Read market prices
prices = spark.read.format("delta").load(f"{config.silver_path}/market_prices")

# Read positions
positions = spark.read.format("delta").load(f"{config.silver_path}/positions")

# COMMAND ----------

# Calculate daily returns
window_spec = Window.partitionBy("instrument_id").orderBy("price_date")

returns = (
    prices.select(col("instrument_id"), col("price_date"), col("close_price"))
    .withColumn("prev_close", lag("close_price", 1).over(window_spec))
    .withColumn(
        "daily_return", (col("close_price") - col("prev_close")) / col("prev_close")
    )
    .filter(col("daily_return").isNotNull())
)

# COMMAND ----------


# Calculate historical VaR using percentile method
@udf(returnType=DoubleType())
def calculate_var(returns_list, confidence_level):
    """Calculate VaR at given confidence level."""
    if not returns_list or len(returns_list) < 30:
        return None

    returns_sorted = sorted(returns_list)
    index = int((1 - confidence_level) * len(returns_sorted))
    return float(returns_sorted[index])


# COMMAND ----------

# Get latest positions date
latest_date = positions.agg(max("as_of_date")).collect()[0][0]

logger.info(f"Calculating VaR for positions as of {latest_date}")

# COMMAND ----------

# Get recent returns (lookback period)
cutoff_date = returns.select(max("price_date")).collect()[0][0]

returns_window = returns.filter(
    col("price_date") >= date_sub(lit(cutoff_date), LOOKBACK_DAYS)
)

# COMMAND ----------

# Aggregate returns by instrument
returns_agg = returns_window.groupBy("instrument_id").agg(
    collect_list("daily_return").alias("returns_list")
)

# COMMAND ----------

# Calculate VaR for each instrument
var_results = (
    returns_agg.withColumn(
        "var_95_pct", calculate_var(col("returns_list"), lit(VAR_CONFIDENCE_95))
    )
    .withColumn(
        "var_99_pct", calculate_var(col("returns_list"), lit(VAR_CONFIDENCE_99))
    )
    .select("instrument_id", "var_95_pct", "var_99_pct")
)

# COMMAND ----------

# Join with positions to get position-level VaR
position_var = (
    positions.filter(col("as_of_date") == latest_date)
    .join(var_results, "instrument_id", "left")
    .withColumn("var_95", col("market_value") * abs(col("var_95_pct")))
    .withColumn("var_99", col("market_value") * abs(col("var_99_pct")))
)

# COMMAND ----------

# Calculate portfolio-level VaR (simplified - assumes independence)
portfolio_var = (
    position_var.agg(
        sum("var_95").alias("total_var_95"),
        sum("var_99").alias("total_var_99"),
        sum("market_value").alias("total_market_value"),
    )
    .withColumn("as_of_date", lit(latest_date))
    .withColumn("calculated_at", current_timestamp())
)

logger.info("Portfolio VaR calculated")
display(portfolio_var)

# COMMAND ----------

# Prepare risk metrics for Gold layer
risk_metrics = position_var.select(
    concat(col("position_id"), lit("_var")).alias("metric_id"),
    col("as_of_date"),
    col("instrument_id"),
    col("book_id"),
    lit(None).cast(StringType()).alias("counterparty_id"),
    col("var_95"),
    col("var_99"),
    lit(None).cast(DecimalType(18, 2)).alias("cvar_95"),
    lit(None).cast(DecimalType(18, 2)).alias("cvar_99"),
    lit(None).cast(DecimalType(18, 6)).alias("delta"),
    lit(None).cast(DecimalType(18, 6)).alias("gamma"),
    lit(None).cast(DecimalType(18, 6)).alias("vega"),
    lit(None).cast(DecimalType(18, 6)).alias("theta"),
    lit(None).cast(DecimalType(18, 6)).alias("rho"),
    lit(None).cast(DecimalType(18, 2)).alias("daily_pnl"),
    lit(None).cast(DecimalType(18, 2)).alias("mtd_pnl"),
    lit(None).cast(DecimalType(18, 2)).alias("ytd_pnl"),
    col("market_value").alias("notional_exposure"),
    col("market_value").alias("market_exposure"),
    lit(None).cast(DecimalType(18, 2)).alias("credit_exposure"),
    current_timestamp().alias("created_at"),
)

# COMMAND ----------

# Write to Gold layer
logger.info(f"Writing VaR metrics to Gold layer: {config.gold_path}/risk_metrics")

(
    risk_metrics.write.format("delta")
    .mode("append")
    .partitionBy("as_of_date")
    .save(f"{config.gold_path}/risk_metrics")
)

logger.info("VaR calculation complete")

# COMMAND ----------

# Display summary by book
display(
    risk_metrics.groupBy("book_id")
    .agg(
        count("*").alias("position_count"),
        sum("var_95").alias("total_var_95"),
        sum("var_99").alias("total_var_99"),
        sum("market_exposure").alias("total_exposure"),
    )
    .orderBy(desc("total_var_99"))
)
