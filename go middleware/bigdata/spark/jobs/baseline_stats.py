from pyspark.sql import SparkSession
from pyspark.sql import functions as F

FEATURES = "hdfs://namenode:8020/traffic/analysis/features"

spark = (SparkSession.builder.appName("baseline-stats")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(FEATURES).filter(F.col("request_count") >= 5)
print("Windows with >= 5 requests:", df.count())

cols = ["rps", "distinct_urls", "top_url_share", "limited_share", "gap_cv"]
rows = []
for c in cols:
    q = df.approxQuantile(c, [0.5, 0.95, 0.99], 0.001)
    # MAD = median of |x - median|
    mad = df.select(F.abs(F.col(c) - F.lit(q[0])).alias("d")).approxQuantile("d", [0.5], 0.001)[0]
    rows.append((c, q[0], q[1], q[2], mad))

spark.createDataFrame(rows, ["feature", "median", "p95", "p99", "mad"]) \
    .select("feature",
            F.round("median", 3).alias("median"),
            F.round("p95", 3).alias("p95"),
            F.round("p99", 3).alias("p99"),
            F.round("mad", 3).alias("mad")) \
    .show(truncate=False)