import argparse
import glob
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

FEATURES_PATH = "hdfs://namenode:8020/traffic/analysis/features"
RESULTS_PATH = "hdfs://namenode:8020/traffic/analysis/results"

# ------------------------- tunable settings -------------------------
MIN_REQ = 5                           # fewer requests than this -> score 0
MIN_REQ_PATTERN = 8                   # needed for hammer / regularity rules
RATE_START, RATE_FULL = 0.5, 2.0      # rps where rate rule starts / maxes out
LIMITED_START, LIMITED_FULL = 0.1, 0.5
HAMMER_SHARE = 0.9                    # share of requests on one URL
BOT_GAP_CV = 0.35                     # gap_cv at or below this = robotic
Z_FULL = 7.0                          # robust z that maps to anomaly score 1.0
Z_OUTLIER = 3.5                       # z above this is mentioned in the reason
GROUP_MIN_IPS = 10                    # IPs sharing UA + URL to count as a group
GROUP_MIN_REQ = 3                     # group members need only this many requests
GROUP_MAX_GAP_CV = 0.5                # group members must have robotic timing
GROUP_SHARE = 0.8                     # member must send >= 80% to the group URL
GROUP_FLOOR = 0.7                     # minimum score for group members
W_RULE, W_ANOM, W_UA, W_GROUP = 0.50, 0.30, 0.10, 0.10
W_RATE, W_LIMITED, W_HAMMER, W_REGULAR = 0.35, 0.25, 0.25, 0.15
SUSPICIOUS_UA = r"(?i)(python-requests|curl|wget|scrapy|bot|crawl|spider|harvest|httpclient|go-http-client)"

# feature -> direction (+1: high is suspicious, -1: low is suspicious)
Z_FEATURES = {"rps": 1, "distinct_urls": 1, "limited_share": 1,
              "top_url_share": 1, "gap_cv": -1}
MAD_FLOOR = {"rps": 0.03, "distinct_urls": 1.0, "limited_share": 0.05,
             "top_url_share": 0.05, "gap_cv": 0.05}
# --------------------------------------------------------------------


def clip01(c):
    return F.least(F.greatest(c, F.lit(0.0)), F.lit(1.0))


def compute_baselines(df):
    """Median and MAD per feature, from windows with enough requests."""
    base = df.filter(F.col("request_count") >= MIN_REQ)
    out = {}
    for c in Z_FEATURES:
        med = base.approxQuantile(c, [0.5], 0.001)[0]
        mad = (base.select(F.abs(F.col(c) - F.lit(med)).alias("d"))
               .approxQuantile("d", [0.5], 0.001)[0])
        out[c] = (med, max(mad, MAD_FLOOR[c]))
    return out


def score_windows(feats, base, threshold):
    df = feats
    n = F.col("request_count")

    # 1) robust z-scores: 0.6745 * (x - median) / MAD
    for c, direction in Z_FEATURES.items():
        med, mad = base[c]
        z = direction * 0.6745 * (F.col(c) - F.lit(med)) / F.lit(mad)
        df = df.withColumn(f"z_{c}", F.coalesce(z, F.lit(0.0)))
    structs = [F.struct(F.col(f"z_{c}").alias("z"), F.lit(c).alias("name"))
               for c in Z_FEATURES]
    df = (df.withColumn("z_top", F.array_max(F.array(*structs)))
            .withColumn("anomaly_score", clip01(F.col("z_top.z") / Z_FULL)))

    # 2) rules (each gives 0..1)
    df = (df
          .withColumn("s_rate", clip01((F.col("rps") - RATE_START) / (RATE_FULL - RATE_START)))
          .withColumn("s_limited",
                      F.when(n >= MIN_REQ,
                             clip01((F.col("limited_share") - LIMITED_START)
                                    / (LIMITED_FULL - LIMITED_START))).otherwise(0.0))
          .withColumn("s_hammer",
                      F.when((n >= MIN_REQ_PATTERN) & (F.col("top_url_share") >= HAMMER_SHARE),
                             1.0).otherwise(0.0))
          .withColumn("s_regular",
                      F.when((n >= MIN_REQ_PATTERN) & (F.col("gap_cv") <= BOT_GAP_CV),
                             1.0).otherwise(0.0))
          .withColumn("s_ua", F.when(F.col("user_agent").rlike(SUSPICIOUS_UA), 1.0).otherwise(0.0))
          .withColumn("rule_score",
                      W_RATE * F.col("s_rate") + W_LIMITED * F.col("s_limited")
                      + W_HAMMER * F.col("s_hammer") + W_REGULAR * F.col("s_regular")))

    # 3) distributed group: many IPs, same user agent, same top URL, same window
    is_cand = ((n >= GROUP_MIN_REQ) & (F.col("top_url_share") >= GROUP_SHARE)
               & (F.col("gap_cv") <= GROUP_MAX_GAP_CV))
    groups = (df.filter(is_cand)
              .groupBy("window_start", "user_agent", "top_url")
              .agg(F.countDistinct("ip").alias("group_size"))
              .filter(F.col("group_size") >= GROUP_MIN_IPS))
    df = (df.join(groups, ["window_start", "user_agent", "top_url"], "left")
            .withColumn("group_size",
                        F.when(is_cand, F.coalesce(F.col("group_size"), F.lit(0))).otherwise(0))
            .withColumn("group_score",
                        F.when(F.col("group_size") >= GROUP_MIN_IPS,
                               clip01(F.col("group_size") / (2.0 * GROUP_MIN_IPS))).otherwise(0.0)))

    # 4) combine into one score
    raw = (W_RULE * F.col("rule_score") + W_ANOM * F.col("anomaly_score")
           + W_UA * F.col("s_ua") + W_GROUP * F.col("group_score"))
    raw = F.when(F.col("group_size") >= GROUP_MIN_IPS,
                 F.greatest(raw, F.lit(GROUP_FLOOR))).otherwise(raw)
    judged = (n >= MIN_REQ) | (F.col("group_size") >= GROUP_MIN_IPS)
    df = df.withColumn("score", F.round(F.when(judged, clip01(raw)).otherwise(0.0), 4))

    # 5) human-readable reason (concat_ws skips the nulls from when() without otherwise)
    pieces = [
        F.when(F.col("rps") >= RATE_START,
               F.format_string("high request rate (%.2f/s)", F.col("rps"))),
        F.when(F.col("limited_share") >= 0.3,
               F.format_string("%.0f%% of requests rate-limited", F.col("limited_share") * 100)),
        F.when(F.col("s_hammer") == 1.0,
               F.format_string("%.0f%% of requests hit %s",
                               F.col("top_url_share") * 100, F.col("top_url"))),
        F.when(F.col("s_regular") == 1.0,
               F.format_string("near-constant timing (gap CV %.2f)", F.col("gap_cv"))),
        F.when(F.col("s_ua") == 1.0, F.lit("suspicious user agent")),
        F.when(F.col("z_top.z") >= Z_OUTLIER,
               F.format_string("statistical outlier: %s (z=%.1f)",
                               F.col("z_top.name"), F.col("z_top.z"))),
        F.when(F.col("group_size") >= GROUP_MIN_IPS,
               F.format_string("part of a %d-IP group sharing one user agent and URL",
                               F.col("group_size"))),
    ]
    joined = F.concat_ws("; ", *pieces)
    reason = (F.when(~judged, F.lit("too few requests to judge"))
              .when(joined == "", F.lit("normal behaviour"))
              .otherwise(joined))

    return (df.withColumn("reason", reason)
              .withColumn("is_flagged", F.col("score") >= threshold)
              .withColumn("detection_type",
                          F.when(F.col("group_size") >= GROUP_MIN_IPS, "distributed_group")
                          .otherwise("per_ip"))
              .select("ip", "window_start", "window_end", "score", "reason",
                      "is_flagged", "detection_type",
                      "rule_score", "anomaly_score", "group_score",
                      "request_count", "rps", "top_url", "top_url_share",
                      "gap_cv", "limited_share", "user_agent"))


def load_labels(spark):
    truth = {}
    for p in glob.glob("/opt/spark-jobs/ground_truth/*.json"):
        with open(p) as f:
            truth.update(json.load(f)["attacker_ips"])
    return spark.createDataFrame(list(truth.items()), ["ip", "attack_type"])


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--threshold", type=float, default=0.5)
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("detect")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    feats = spark.read.parquet(FEATURES_PATH)
    base = compute_baselines(feats)
    print("BASELINES (median, MAD):", {k: (round(v[0], 3), round(v[1], 3)) for k, v in base.items()})
    print("THRESHOLD:", args.threshold)

    score_windows(feats, base, args.threshold).write.mode("overwrite").parquet(RESULTS_PATH)

    # ---------- quick preview against ground truth (full evaluation is Step 4) ----------
    res = spark.read.parquet(RESULTS_PATH)
    print("RESULT ROWS:", res.count(), "| FLAGGED WINDOWS:", res.filter("is_flagged").count())

    w = (res.join(load_labels(spark), "ip", "left")
            .withColumn("attack_type", F.coalesce("attack_type", F.lit("human"))))
    per_ip = (w.groupBy("ip", "attack_type")
               .agg(F.max("score").alias("max_score"),
                    F.max(F.col("is_flagged").cast("int")).alias("flagged")))

    print("Per attack type (IP level: flagged if ANY window is flagged):")
    (per_ip.groupBy("attack_type")
           .agg(F.count("*").alias("ips"), F.sum("flagged").alias("flagged_ips"),
                F.round(F.min("max_score"), 2).alias("lowest_best_score"),
                F.round(F.max("max_score"), 2).alias("highest_score"))
           .orderBy("attack_type").show(truncate=False))

    is_att = F.col("attack_type") != "human"
    tp = per_ip.filter(is_att & (F.col("flagged") == 1)).count()
    fn = per_ip.filter(is_att & (F.col("flagged") == 0)).count()
    fp = per_ip.filter(~is_att & (F.col("flagged") == 1)).count()
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    print(f"TP={tp} FN={fn} FP(humans flagged)={fp} precision={prec:.3f} recall={rec:.3f}")

    print("Example reason for the best-scoring flagged window per attack type:")
    (w.filter("is_flagged").groupBy("attack_type")
      .agg(F.max_by("reason", "score").alias("example_reason"))
      .show(truncate=False))

    print("Top 5 highest-scoring HUMAN windows (false-positive risk):")
    (w.filter(F.col("attack_type") == "human")
      .select("ip", "score", "reason").orderBy(F.desc("score")).show(5, truncate=False))