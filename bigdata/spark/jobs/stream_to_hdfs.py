from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, from_json, to_timestamp
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "gateway.traffic.events"
OUTPUT_PATH = "hdfs://namenode:8020/traffic/raw"
CHECKPOINT_PATH = "hdfs://namenode:8020/spark/checkpoints/traffic_raw"

spark = (
    SparkSession.builder.appName("traffic-raw-to-hdfs")
    .config("spark.sql.session.timeZone", "UTC")  # dt/hr must be UTC
    .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Matches the fixed event contract
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("ip", StringType()),
    StructField("method", StringType()),
    StructField("url", StringType()),
    StructField("status_code", IntegerType()),
    StructField("decision", StringType()),
    StructField("user_agent", StringType()),
    StructField("latency_ms", LongType()),
])

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 50000)
    .option("failOnDataLoss", "false")
    .load()
)

events = (
    raw.select(from_json(col("value").cast("string"), schema).alias("e"))
    .select("e.*")
    .filter(col("ip").isNotNull())
    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))
    .filter(col("timestamp").isNotNull())
    .withColumn("dt", date_format(col("timestamp"), "yyyy-MM-dd"))
    .withColumn("hr", date_format(col("timestamp"), "HH"))
)

query = (
    events.writeStream.format("parquet")
    .outputMode("append")
    .option("path", OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("dt", "hr")
    .trigger(processingTime="30 seconds")
    .start()
)

query.awaitTermination()