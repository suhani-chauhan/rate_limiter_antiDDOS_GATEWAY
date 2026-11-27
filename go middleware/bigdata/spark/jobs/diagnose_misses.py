import glob
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RESULTS = "hdfs://namenode:8020/traffic/analysis/results"

spark = (SparkSession.builder.appName("diagnose-misses")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

truth = {}
for p in glob.glob("/opt/spark-jobs/ground_truth/*.json"):
    with open(p) as f:
        truth.update(json.load(f)["attacker_ips"])
labels = spark.createDataFrame(list(truth.items()), ["ip", "attack_type"])

per_ip = (spark.read.parquet(RESULTS)
          .join(labels, "ip", "left")
          .withColumn("attack_type", F.coalesce("attack_type", F.lit("human")))
          .groupBy("ip", "attack_type")
          .agg(F.max("request_count").alias("max_req"),
               F.max("score").alias("max_score"),
               F.max(F.col("is_flagged").cast("int")).alias("flagged")))

attackers = per_ip.filter(F.col("attack_type") != "human")

print("MISSED attackers (never flagged):")
(attackers.filter("flagged = 0")
 .groupBy("attack_type")
 .agg(F.count("*").alias("ips"),
      F.round(F.avg("max_req"), 1).alias("avg_best_window_requests"),
      F.max("max_req").alias("max_best_window_requests"),
      F.round(F.max("max_score"), 2).alias("max_score"))
 .show(truncate=False))

print("Recall split by whether the IP ever had >= 5 requests in one window:")
(attackers.withColumn("had_enough_data", F.col("max_req") >= 5)
 .groupBy("had_enough_data")
 .agg(F.count("*").alias("ips"), F.sum("flagged").alias("flagged_ips"))
 .show())