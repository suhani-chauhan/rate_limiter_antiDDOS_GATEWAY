import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone

import redis
from redis.backoff import ExponentialBackoff
from redis.exceptions import RedisError
from redis.retry import Retry

FLAGGED_GLOB = "/traffic/analysis/flagged/part-*"
BLOCK_TTL = 3600
SET_KEY = "suspicious:ips"
BATCH = 200


def read_flagged():
    """Read the JSON lines the Spark job wrote, via the namenode container."""
    cmd = ["docker", "exec", "namenode", "hdfs", "dfs", "-cat", FLAGGED_GLOB]
    out = subprocess.run(cmd, capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(f"could not read flagged export: {out.stderr.strip()[:300]}")
    rows = []
    for line in out.stdout.splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def connect(host, port):
    return redis.Redis(
        host=host, port=port,
        socket_connect_timeout=2, socket_timeout=2,
        retry=Retry(ExponentialBackoff(cap=4, base=0.25), retries=5),
        retry_on_error=[redis.exceptions.ConnectionError, redis.exceptions.TimeoutError],
        decode_responses=True,
    )


def push(r, rows):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for i in range(0, len(rows), BATCH):
        pipe = r.pipeline(transaction=False)
        for row in rows[i:i + BATCH]:
            ip = row["ip"]
            score = round(float(row["score"]), 4)
            value = json.dumps({"score": score, "reason": row["reason"], "detected_at": now})
            pipe.set(f"blocked:ip:{ip}", value, ex=BLOCK_TTL)   # contract: TTL 3600s
            pipe.zadd(SET_KEY, {ip: score})                      # contract: dashboard set
        pipe.execute()


def prune(r):
    """Drop sorted-set members whose blocked:ip key has expired."""
    members = r.zrange(SET_KEY, 0, -1)
    stale = []
    for i in range(0, len(members), BATCH):
        chunk = members[i:i + BATCH]
        pipe = r.pipeline(transaction=False)
        for ip in chunk:
            pipe.exists(f"blocked:ip:{ip}")
        for ip, alive in zip(chunk, pipe.execute()):
            if not alive:
                stale.append(ip)
    if stale:
        r.zrem(SET_KEY, *stale)
    return len(stale)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=6379)
    args = ap.parse_args()

    try:
        rows = read_flagged()
    except Exception as e:
        print("ERROR reading flagged IPs:", e)
        return 2
    print(f"Read {len(rows)} flagged IPs from HDFS")

    try:
        r = connect(args.host, args.port)
        r.ping()
        push(r, rows)
        removed = prune(r)
    except RedisError as e:
        print(f"ERROR: Redis unavailable after retries ({e}). Nothing more written.")
        return 1

    print(f"Wrote {len(rows)} blocked:ip:* keys (TTL {BLOCK_TTL}s) and ZADDed to {SET_KEY}; "
          f"pruned {removed} stale entries")
    print("suspicious:ips size now:", r.zcard(SET_KEY))
    return 0


if __name__ == "__main__":
    sys.exit(main())