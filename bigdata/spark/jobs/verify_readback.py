from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("verify-readback")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("hdfs://namenode:8020/traffic/raw")
print("TOTAL ROWS:", df.count())
df.printSchema()

print("Rows per partition:")
df.groupBy("dt", "hr").count().orderBy("dt", "hr").show()

print("Decisions:")
df.groupBy("decision").count().show()

print("Top 5 IPs by request count:")
df.groupBy("ip").count().orderBy("count", ascending=False).show(5, truncate=False)

print("Sample rows:")
df.show(3, truncate=False)