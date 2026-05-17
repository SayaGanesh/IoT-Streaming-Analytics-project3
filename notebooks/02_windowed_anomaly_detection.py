# Databricks notebook source
# MAGIC %md
# MAGIC # 02 – Windowed Aggregations & Anomaly Detection
# MAGIC
# MAGIC Reads Bronze streaming Delta table, computes 5-minute sliding window aggregations,
# MAGIC applies Z-score anomaly detection per device, and writes:
# MAGIC   - All windows → Silver Delta (historical analytics)
# MAGIC   - Anomaly windows → Cosmos DB (operational alerts)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, avg, max, min, stddev, count,
    when, lit, round as spark_round, current_timestamp,
    expr
)
from pyspark.sql.types import DoubleType

spark = SparkSession.builder \
    .appName("IoT_Windowed_Anomaly") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# COMMAND ----------

STORAGE_ACCOUNT  = "yourstorageaccount"
BRONZE_PATH      = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/iot_sensors/"
SILVER_PATH      = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/iot_windows/"
CHECKPOINT_SILVER = f"abfss://checkpoints@{STORAGE_ACCOUNT}.dfs.core.windows.net/iot_silver/"
CHECKPOINT_COSMOS = f"abfss://checkpoints@{STORAGE_ACCOUNT}.dfs.core.windows.net/iot_cosmos/"

COSMOS_ENDPOINT  = "https://your-cosmos-account.documents.azure.com:443/"
COSMOS_KEY       = dbutils.secrets.get(scope="kv-iot-scope", key="cosmos-key")
COSMOS_DB        = "iot_alerts"
COSMOS_CONTAINER = "anomalies"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze Stream

# COMMAND ----------

bronze_stream = (
    spark.readStream
    .format("delta")
    .option("ignoreChanges", "true")
    .load(BRONZE_PATH)
    .withWatermark("event_ts", "10 minutes")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5-Minute Sliding Window Aggregation (1-min slide)

# COMMAND ----------

windowed = (
    bronze_stream
    .groupBy(
        window(col("event_ts"), "5 minutes", "1 minute"),
        col("device_id"),
        col("device_type"),
        col("factory_zone")
    )
    .agg(
        spark_round(avg("temperature_c"),  3).alias("avg_temp_c"),
        spark_round(max("temperature_c"),  3).alias("max_temp_c"),
        spark_round(min("temperature_c"),  3).alias("min_temp_c"),
        spark_round(avg("vibration_ms2"),  3).alias("avg_vibration"),
        spark_round(max("vibration_ms2"),  3).alias("max_vibration"),
        spark_round(avg("pressure_bar"),   3).alias("avg_pressure"),
        spark_round(avg("rpm").cast(DoubleType()), 1).alias("avg_rpm"),
        count("message_id").alias("event_count"),
    )
    .withColumn("window_start",       col("window.start"))
    .withColumn("window_end",         col("window.end"))
    .drop("window")
    .withColumn("_processed_at",      current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Z-Score Anomaly Flagging
# MAGIC
# MAGIC Note: In production, rolling mean/stddev comes from a stateful lookup table
# MAGIC updated hourly. Here we use per-window thresholds as a simplified proxy.

# COMMAND ----------

TEMP_THRESHOLD_Z3    = 90.0   # ~3 std above mean for CNC (~65 + 3*8)
VIBRATION_THRESHOLD  = 4.5    # known bearing fault indicator

anomaly_flagged = (
    windowed
    .withColumn("anomaly_temp",
        when(col("avg_temp_c") > TEMP_THRESHOLD_Z3, lit(1)).otherwise(lit(0)))
    .withColumn("anomaly_vibration",
        when(col("avg_vibration") > VIBRATION_THRESHOLD, lit(1)).otherwise(lit(0)))
    .withColumn("is_anomaly",
        when((col("anomaly_temp") == 1) | (col("anomaly_vibration") == 1), lit(True))
        .otherwise(lit(False)))
    .withColumn("anomaly_severity",
        when(col("anomaly_temp") + col("anomaly_vibration") >= 2, lit("CRITICAL"))
        .when(col("anomaly_temp") + col("anomaly_vibration") == 1, lit("WARNING"))
        .otherwise(lit("NORMAL")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write All Windows → Silver Delta

# COMMAND ----------

silver_query = (
    anomaly_flagged.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_SILVER)
    .partitionBy("factory_zone")
    .start(SILVER_PATH)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Anomalies → Cosmos DB (Hot Path)

# COMMAND ----------

def write_anomalies_to_cosmos(batch_df, batch_id):
    anomalies = batch_df.filter(col("is_anomaly") == True)
    if anomalies.count() > 0:
        (
            anomalies
            .withColumn("id", expr("uuid()"))  # Cosmos requires id field
            .write
            .format("cosmos.oltp")
            .option("spark.synapse.linkedService", "CosmosDBLinkedService")
            .option("spark.cosmos.accountEndpoint", COSMOS_ENDPOINT)
            .option("spark.cosmos.accountKey", COSMOS_KEY)
            .option("spark.cosmos.database", COSMOS_DB)
            .option("spark.cosmos.container", COSMOS_CONTAINER)
            .mode("append")
            .save()
        )
        print(f"Batch {batch_id}: Wrote {anomalies.count()} anomalies to Cosmos DB")

cosmos_query = (
    anomaly_flagged.writeStream
    .foreachBatch(write_anomalies_to_cosmos)
    .option("checkpointLocation", CHECKPOINT_COSMOS)
    .outputMode("append")
    .start()
)

print("Both streaming queries active.")
silver_query.awaitTermination()
