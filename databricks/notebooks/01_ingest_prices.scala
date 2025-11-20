
// Databricks Scala — 01_ingest_prices.scala
// Ingesta simple de precios desde CSV → Delta Bronze
import org.apache.spark.sql.functions._

val rawPath = dbutils.widgets.get("rawPath")  // e.g., /mnt/raw/prices
val bronzePath = dbutils.widgets.get("bronzePath")

val df = spark.read
  .option("header","true")
  .option("inferSchema","true")
  .csv(rawPath)

val dfClean = df.withColumn("date", to_date(col("date")))
dfClean.write.mode("overwrite").format("delta").save(bronzePath + "/prices")
