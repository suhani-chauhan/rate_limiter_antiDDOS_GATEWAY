import subprocess
import sys
import time
from datetime import datetime

INTERVAL = 60          # seconds between cycle starts
LOOKBACK_MIN = 5       # only recent windows may trigger blocks

SPARK = ["docker", "exec", "spark-master", "/opt/spark/bin/spark-submit",
         "--master", "local[2]", "--driver-memory", "1g",
         "--conf", "spark.jars.ivy=/tmp/ivy2"]

STEPS = [
    ("features", SPARK + ["/opt/spark-jobs/build_features.py"]),
    ("detect", SPARK + ["/opt/spark-jobs/detect.py"]),
    ("export", SPARK + ["/opt/spark-jobs/export_flagged.py",
                        "--lookback-minutes", str(LOOKBACK_MIN)]),
    ("redis", [sys.executable, "redis_writer/redis_writer.py"]),
]


def ts():
    return datetime.now().strftime("%H:%M:%S")


def run_cycle():
    for name, cmd in STEPS:
        t0 = time.time()
        res = subprocess.run(cmd, capture_output=True, text=True)
        took = time.time() - t0
        if res.returncode != 0:
            tail = (res.stderr or res.stdout).strip().splitlines()[-3:]
            print(f"[{ts()}] {name} FAILED after {took:.0f}s: {' | '.join(tail)}")
            return False
        extra = ""
        for line in res.stdout.splitlines():
            if line.startswith(("FLAGGED IPS", "Wrote", "Read ")):
                extra += " | " + line
        print(f"[{ts()}] {name} ok ({took:.0f}s){extra}")
    return True


if __name__ == "__main__":
    print(f"Detection loop started: every {INTERVAL}s, lookback {LOOKBACK_MIN} min. Ctrl+C to stop.")
    try:
        while True:
            start = time.time()
            ok = run_cycle()
            print(f"[{ts()}] cycle {'complete' if ok else 'aborted'}")
            time.sleep(max(1, INTERVAL - (time.time() - start)))
    except KeyboardInterrupt:
        print("Stopped.")