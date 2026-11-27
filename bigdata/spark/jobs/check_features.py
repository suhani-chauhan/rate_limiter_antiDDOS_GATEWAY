import glob
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

FEATURES = "hdfs://namenode:8020/traffic/analysis/features"

spark = (SparkSession.builder.appName("check-features")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# Merge every ground-truth file: {ip: attack_type}
truth = {}
for path in glob.glob("/opt/spark-jobs/ground_truth/*.json"):
    with open(path) as f:
        truth.update(json.load(f)["attacker_ips"])
labels = spark.createDataFrame(list(truth.items()), ["ip", "attack_type"])
print("Attacker IPs in ground truth:", len(truth))

df = (spark.read.parquet(FEATURES)
      .join(labels, "ip", "left")
      .withColumn("attack_type", F.coalesce("attack_type", F.lit("human")))
      .filter(F.col("request_count") >= 5))  # ignore nearly empty edge windows

(df.groupBy("attack_type")
   .agg(F.countDistinct("ip").alias("ips"),
        F.count("*").alias("windows"),
        F.round(F.avg("rps"), 2).alias("rps"),
        F.round(F.avg("distinct_urls"), 1).alias("urls"),
        F.round(F.avg("top_url_share"), 2).alias("top_url_share"),
        F.round(F.avg("distinct_uas"), 1).alias("uas"),
        F.round(F.avg("limited_share"), 2).alias("limited"),
        F.round(F.avg("gap_cv"), 2).alias("gap_cv"))
   .orderBy("attack_type")
   .show(truncate=False))