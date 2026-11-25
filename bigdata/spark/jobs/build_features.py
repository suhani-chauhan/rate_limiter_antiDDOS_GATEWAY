from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

RAW_PATH = "hdfs://namenode:8020/traffic/raw"
OUT_PATH = "hdfs://namenode:8020/traffic/analysis/features"

WINDOW = "1 minute"
SLIDE = "10 seconds"
WINDOW_SECONDS = 60
MAX_GAP_SECONDS = 60  # gaps longer than this are ignored (set to null)


def build_features(events):
    # 1) Per-event columns -------------------------------------------------
    # lag() looks at the previous event of the SAME ip (ordered by time).
    w = Window.partitionBy("ip").orderBy("timestamp")
    ev = (
        events
        .withColumn("prev_ts", F.lag("timestamp").over(w))
        .withColumn("gap_raw",
                    F.col("timestamp").cast("double") - F.col("prev_ts").cast("double"))
        # when() without otherwise() gives null if the condition is false (or gap is null)
        .withColumn("gap_s", F.when(F.col("gap_raw") <= MAX_GAP_SECONDS, F.col("gap_raw")))
        .withColumn("is_error", (F.col("status_code") >= 400).cast("int"))
        .withColumn("is_limited",
                    F.col("decision").isin("rate_limited", "blocked").cast("int"))
    )

    # 2) Main aggregates per (ip, sliding window) --------------------------
    # window() assigns each event to every 1-minute window it falls in.
    main = (
        ev.groupBy(F.window("timestamp", WINDOW, SLIDE).alias("w"), "ip")
        .agg(
            F.count("*").alias("request_count"),
            F.countDistinct("url").alias("distinct_urls"),
            F.countDistinct("user_agent").alias("distinct_uas"),
            F.min("user_agent").alias("user_agent"),
            F.avg("is_error").alias("error_rate"),
            F.avg("is_limited").alias("limited_share"),
            F.avg("gap_s").alias("gap_mean"),
            F.stddev_samp("gap_s").alias("gap_std"),
            F.avg("latency_ms").alias("avg_latency_ms"),
        )
        .withColumn("window_start", F.col("w.start"))
        .withColumn("window_end", F.col("w.end"))
        .drop("w")
    )

    # 3) Most-hit URL per (ip, window): count per url first, then take the max
    per_url = (
        ev.groupBy(F.window("timestamp", WINDOW, SLIDE).alias("w"), "ip", "url")
        .agg(F.count("*").alias("c"))
        .withColumn("window_start", F.col("w.start"))
        .drop("w")
    )
    top = per_url.groupBy("window_start", "ip").agg(
        F.max_by("url", "c").alias("top_url"),
        F.max("c").alias("top_url_count"),
    )

    # 4) Combine and derive ratios ----------------------------------------
    return (
        main.join(top, ["window_start", "ip"])
        .withColumn("rps", F.col("request_count") / WINDOW_SECONDS)
        .withColumn("top_url_share", F.col("top_url_count") / F.col("request_count"))
        .withColumn("gap_cv", F.col("gap_std") / F.col("gap_mean"))
        .select(
            "ip", "window_start", "window_end",
            "request_count", "rps", "distinct_urls", "top_url", "top_url_share",
            "distinct_uas", "user_agent", "error_rate", "limited_share",
            "gap_mean", "gap_std", "gap_cv", "avg_latency_ms",
        )
    )


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("build-features")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")  # tiny data: avoid 200 tiny tasks
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    events = spark.read.parquet(RAW_PATH)
    print("RAW EVENTS:", events.count())

    build_features(events).write.mode("overwrite").parquet(OUT_PATH)

    feats = spark.read.parquet(OUT_PATH)
    print("FEATURE ROWS (ip x window):", feats.count())
    print("DISTINCT IPS:", feats.select("ip").distinct().count())
    feats.printSchema()
    print("Top 5 windows by requests per second:")
    (feats.select("ip", "window_start", "request_count", F.round("rps", 2).alias("rps"),
                  "top_url", F.round("top_url_share", 2).alias("top_url_share"),
                  F.round("limited_share", 2).alias("limited_share"))
          .orderBy(F.desc("rps")).show(5, truncate=False))