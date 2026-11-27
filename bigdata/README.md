# Traffic Big Data Pipeline

Kafka (KRaft) → Spark Structured Streaming → HDFS (Parquet), plus Redis for Part B.

## Prerequisites
- Docker Desktop (WSL2 backend on Windows) with **≥ 6 GB RAM** allocated
- Free host ports: 29092, 9870, 8080, 8081, 7077, 6379

## Start
```bash
docker compose up -d
docker compose ps          # wait until kafka, namenode, datanode, spark-*, redis show "healthy"
docker compose logs hdfs-init   # should end with HDFS-INIT-OK
```
First start pulls ~4 GB of images and takes a few minutes.

## Endpoints
| Service | From your host | From other containers |
|---|---|---|
| Kafka | `localhost:29092` | `kafka:9092` |
| HDFS NameNode UI | http://localhost:9870 | `hdfs://namenode:8020` |
| Spark Master UI | http://localhost:8080 | `spark://spark-master:7077` |
| Redis | `localhost:6379` | `redis:6379` |

## Stop
```bash
docker compose down        # keep data
docker compose down -v     # wipe all data (Kafka, HDFS, Redis)
```

## Troubleshooting
- Namenode unhealthy: `docker compose logs namenode`; if it says "not formatted", run `docker compose down -v` and start again.
- Windows Git Bash mangles `/paths` in docker exec: prefix with `MSYS_NO_PATHCONV=1`.
## Part A: Run the pipeline

1. `docker compose up -d` and wait for all services to be healthy.
2. Create the topic:
   `docker cp kafka/create-topic.sh kafka:/tmp/create-topic.sh`
   `docker exec kafka bash /tmp/create-topic.sh`
3. Start the streaming job (leave it running):
   `docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 512m --executor-memory 512m --total-executor-cores 2 --conf spark.jars.ivy=/tmp/ivy --conf spark.driver.host=spark-master --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /opt/spark-jobs/stream_to_hdfs.py`
4. Generate traffic:
   `cd simulator && python -m venv .venv && .venv\Scripts\activate && pip install -r requirements.txt`
   `python simulator.py --rate 50 --duration 60`
5. Verify (row count, schema, top IPs):
   `docker exec spark-master /opt/spark/bin/spark-submit --master local[1] --driver-memory 512m --conf spark.jars.ivy=/tmp/ivy2 /opt/spark-jobs/verify_readback.py`

### Contract summary
- Kafka topic `gateway.traffic.events` (6 partitions, key = ip), from the host at `localhost:29092`, inside Docker at `kafka:9092`
- HDFS: `/traffic/raw/dt=YYYY-MM-DD/hr=HH/` (Parquet); checkpoint at `/spark/checkpoints/traffic_raw`
- Ground truth for scoring Part B: `simulator/output/ground_truth*.json`
- Part B tip: read with `spark.sql.sources.partitionColumnTypeInference.enabled=false` so `hr` stays a string

## Part B: Detection

Prerequisites: stack healthy, streaming job running (http://localhost:8080 shows `traffic-raw-to-hdfs`).

1. Features: `docker exec spark-master /opt/spark/bin/spark-submit --master local[2] --driver-memory 1g --conf spark.jars.ivy=/tmp/ivy2 /opt/spark-jobs/build_features.py`
2. Detect (writes `/traffic/analysis/results`): same command with `detect.py` (optional `--threshold 0.5`)
3. Export flagged IPs: same command with `export_flagged.py --lookback-minutes 5`
4. Push to Redis: `python redis_writer\redis_writer.py`
5. Run all four on a loop: `python scheduler\run_detection.py` (about every 90 s)
6. Evaluate: `evaluate.py` (precision/recall/F1 by threshold), `evaluate_hard.py` (hard-human false positives)

Redis contract: `blocked:ip:<ip>` = `{"score","reason","detected_at"}` with TTL 3600 s; sorted set `suspicious:ips`.

Default threshold 0.5. Known limitations: shared office/NAT IPs score close to the threshold (needs an allow-list); the group rule relies on timing regularity and is tuned to the simulator; results are from simulated traffic.