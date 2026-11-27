import glob
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RESULTS = "hdfs://namenode:8020/traffic/analysis/results"
THRESHOLDS = [0.15, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]

spark = (SparkSession.builder.appName("evaluate")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

truth = {}
for p in glob.glob("/opt/spark-jobs/ground_truth/*.json"):
    with open(p) as f:
        truth.update(json.load(f)["attacker_ips"])
labels = spark.createDataFrame(list(truth.items()), ["ip", "attack_type"])

res = (spark.read.parquet(RESULTS)
       .join(F.broadcast(labels), "ip", "left")
       .withColumn("is_attacker", F.col("attack_type").isNotNull())
       .withColumn("attack_type", F.coalesce("attack_type", F.lit("human"))))

per_ip = [r.asDict() for r in
          res.groupBy("ip", "is_attacker", "attack_type")
             .agg(F.max("score").alias("max_score")).collect()]
att = [r for r in per_ip if r["is_attacker"]]
hum = [r for r in per_ip if not r["is_attacker"]]
print(f"Attackers in ground truth: {len(truth)} | seen in results: {len(att)}")
print(f"Human IPs: {len(hum)}")

human_w = res.filter(~F.col("is_attacker") & (F.col("request_count") >= 5)).cache()
n_hw = human_w.count()

print(f"\n{'thr':>5} {'TP':>4} {'FN':>4} {'FP':>4} {'prec':>6} {'recall':>7} {'F1':>6} "
      f"{'human IPs blocked':>18} {'human windows >= thr':>21}")
for t in THRESHOLDS:
    tp = sum(1 for r in att if r["max_score"] >= t)
    fn = len(att) - tp
    fp = sum(1 for r in hum if r["max_score"] >= t)
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
    fpw = human_w.filter(F.col("score") >= t).count()
    print(f"{t:>5.2f} {tp:>4} {fn:>4} {fp:>4} {prec:>6.3f} {rec:>7.3f} {f1:>6.3f} "
          f"{fp / max(len(hum), 1):>17.1%} {fpw / max(n_hw, 1):>20.2%}")

print("\nScore margin:")
print("  weakest attacker best-score :", round(min(r["max_score"] for r in att), 3))
print("  strongest human best-score  :", round(max(r["max_score"] for r in hum), 3))

print("\nBest score per attack type (min / max):")
for typ in sorted({r["attack_type"] for r in att}):
    s = [r["max_score"] for r in att if r["attack_type"] == typ]
    print(f"  {typ:<26} n={len(s):<3} min={min(s):.3f} max={max(s):.3f}")