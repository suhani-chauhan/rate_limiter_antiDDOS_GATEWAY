import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RESULTS = "hdfs://namenode:8020/traffic/analysis/results"
OUT = "hdfs://namenode:8020/traffic/analysis/flagged"

ap = argparse.ArgumentParser()
ap.add_argument("--lookback-minutes", type=int, default=0,
                help="only use windows ending in the last N minutes (0 = all)")
args = ap.parse_args()

spark = (SparkSession.builder.appName("export-flagged")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(RESULTS).filter("is_flagged")
if args.lookback_minutes > 0:
    cutoff = F.current_timestamp() - F.expr(f"INTERVAL {args.lookback_minutes} MINUTES")
    df = df.filter(F.col("window_end") >= cutoff)

best = (df.groupBy("ip")
          .agg(F.max("score").alias("score"),
               F.max_by("reason", "score").alias("reason"),
               F.max_by("detection_type", "score").alias("detection_type")))

# one small JSON-lines file, easy to read from the host
best.coalesce(1).write.mode("overwrite").json(OUT)
print("FLAGGED IPS EXPORTED:", spark.read.json(OUT).count())