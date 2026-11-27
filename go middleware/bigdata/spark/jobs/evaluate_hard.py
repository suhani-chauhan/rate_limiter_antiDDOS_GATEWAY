import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RESULTS = "hdfs://namenode:8020/traffic/analysis/results"
THRESHOLD = 0.5

spark = (SparkSession.builder.appName("evaluate-hard")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

with open("/opt/spark-jobs/hard_humans/hard_humans.json") as f:
    hard = json.load(f)["hard_human_ips"]
labels = spark.createDataFrame(list(hard.items()), ["ip", "scenario"])

per_ip = (spark.read.parquet(RESULTS)
          .join(F.broadcast(labels), "ip", "inner")
          .groupBy("ip", "scenario")
          .agg(F.max("score").alias("best"),
               F.max_by("reason", "score").alias("reason"),
               F.max("request_count").alias("max_req")))

print(f"Threshold = {THRESHOLD}. Every flagged IP below is a FALSE POSITIVE.")
(per_ip.groupBy("scenario")
 .agg(F.count("*").alias("ips"),
      F.sum((F.col("best") >= THRESHOLD).cast("int")).alias("wrongly_flagged"),
      F.round(F.min("best"), 3).alias("min_score"),
      F.round(F.max("best"), 3).alias("max_score"),
      F.max_by("reason", "best").alias("reason_of_highest"))
 .orderBy("scenario").show(truncate=False))