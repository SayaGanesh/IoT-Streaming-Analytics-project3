# Databricks notebook source
# MAGIC %md
# MAGIC # 01 – IoT Streaming: Event Hub Ingestion → Bronze Delta
# MAGIC
# MAGIC Reads real-time sensor events from Azure Event Hubs via Kafka endpoint.
# MAGIC Deserializes JSON payloads, applies watermarking, writes to Bronze Delta
# MAGIC using append mode with checkpointing for exactly-once semantics.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)

spark = SparkSession.builder \
    .appName("IoT_Streaming_Bronze") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

EH_NAMESPACE    = "your-eventhub-namespace"
EH_NAME         = "iot-sensor-hub"
EH_CONN_STRING  = dbutils.secrets.get(scope="kv-iot-scope", key="eventhub-conn-string")

STORAGE_ACCOUNT = "yourstorageaccount"
BRONZE_PATH     = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/iot_sensors/"
CHECKPOINT_PATH = f"abfss://checkpoints@{STORAGE_ACCOUNT}.dfs.core.windows.net/iot_bronze/"

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers":             f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    "kafka.security.protocol":             "SASL_SSL",
    "kafka.sasl.mechanism":                "PLAIN",
    "kafka.sasl.jaas.config":              f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EH_CONN_STRING}";',
    "subscribe":                           EH_NAME,
    "startingOffsets":                     "latest",
    "failOnDataLoss":                      "false",
    "kafka.request.timeout.ms":            "60000",
    "kafka.session.timeout.ms":            "30000",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definition

# COMMAND ----------

sensor_schema = StructType([
    StructField("message_id",    StringType(),  True),
    StructField("device_id",     StringType(),  True),
    StructField("device_type",   StringType(),  True),
    StructField("factory_zone",  StringType(),  True),
    StructField("timestamp",     StringType(),  True),
    StructField("temperature_c", DoubleType(),  True),
    StructField("vibration_ms2", DoubleType(),  True),
    StructField("pressure_bar",  DoubleType(),  True),
    StructField("rpm",           IntegerType(), True),
    StructField("runtime_hours", DoubleType(),  True),
    StructField("firmware_ver",  StringType(),  True),
    StructField("schema_version", StringType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Stream from Event Hubs (Kafka)

# COMMAND ----------

raw_stream = (
    spark.readStream
    .format("kafka")
    .options(**KAFKA_OPTIONS)
    .load()
)

parsed_stream = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json_payload", "timestamp AS kafka_timestamp")
    .withColumn("payload",          from_json(col("json_payload"), sensor_schema))
    .select("payload.*", "kafka_timestamp")
    .withColumn("event_ts",         to_timestamp(col("timestamp")))
    .withColumn("_ingested_at",     current_timestamp())
    # 10-minute watermark for late arriving IoT data
    .withWatermark("event_ts", "10 minutes")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Bronze Stream (Append, checkpointed)

# COMMAND ----------

bronze_query = (
    parsed_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .partitionBy("factory_zone")
    .start(BRONZE_PATH)
)

print(f"Streaming query started. ID: {bronze_query.id}")
bronze_query.awaitTermination()
