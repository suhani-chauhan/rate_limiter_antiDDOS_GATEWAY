#!/usr/bin/env python3
"""Traffic simulator: normal humans + attack patterns -> Kafka.

Events are NOT labelled. Attacker IPs are written to a separate ground-truth file.
"""
import argparse
import heapq
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC = "gateway.traffic.events"

BROWSER_UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 Chrome/124.0 Mobile Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
]
BOT_UA = "python-requests/2.31.0"
SCRAPER_UA = "Mozilla/5.0 (compatible; DataHarvester/1.0)"

HUMAN_PAGES = ["/", "/home", "/products", "/products/{n}", "/cart", "/search?q=shoes",
               "/static/app.js", "/static/style.css", "/api/offers"]
RATE_LIMIT_PER_SEC = 10  # simulated gateway limit per IP


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + \
        f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z"


class Actor:
    """One IP that sends requests at some pace with some behaviour."""

    def __init__(self, ip, ua_fn, url_fn, interval_fn, method_fn=None):
        self.ip = ip
        self.ua_fn = ua_fn
        self.url_fn = url_fn
        self.interval_fn = interval_fn
        self.method_fn = method_fn or (lambda: "GET")
        self.recent = []  # timestamps for the simulated rate limiter

    def make_event(self, t):
        # simulated gateway: sliding 1-second window
        self.recent = [x for x in self.recent if t - x < 1.0]
        self.recent.append(t)
        limited = len(self.recent) > RATE_LIMIT_PER_SEC
        url = self.url_fn()
        decision = "rate_limited" if limited else "allowed"
        status = 429 if limited else (404 if random.random() < 0.01 else 200)
        return {
            "timestamp": now_iso(),
            "ip": self.ip,
            "method": self.method_fn(),
            "url": url,
            "status_code": status,
            "decision": decision,
            "user_agent": self.ua_fn(),
            "latency_ms": random.randint(2, 15) if limited else random.randint(20, 250),
        }


def rand_ip(used):
    while True:
        ip = f"{random.randint(11, 223)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"
        if ip not in used:
            used.add(ip)
            return ip


def build_humans(n, used):
    actors = []
    for _ in range(n):
        ua = random.choice(BROWSER_UAS)
        actors.append(Actor(
            rand_ip(used),
            ua_fn=lambda ua=ua: ua,
            url_fn=lambda: random.choice(HUMAN_PAGES).format(n=random.randint(1, 500)),
            # humans: think time 1-15s between actions, bursty
            interval_fn=lambda: random.lognormvariate(1.3, 0.8),
            method_fn=lambda: "POST" if random.random() < 0.05 else "GET",
        ))
    return actors


def build_flood(used, truth, rate):
    ip = rand_ip(used)
    truth[ip] = "flood"
    mean = 1.0 / (rate * 0.5)  # very fast
    return [Actor(ip, lambda: BOT_UA,
                  lambda: random.choice(["/", "/home", "/products", "/api/offers"]),
                  lambda: random.expovariate(1.0 / mean))]


def build_low_and_slow(used, truth, n_ips):
    actors = []
    for _ in range(n_ips):
        ip = rand_ip(used)
        truth[ip] = "distributed_low_and_slow"
        actors.append(Actor(ip, lambda: BOT_UA,
                            lambda: "/api/offers",
                            # one request every ~4-6s per IP: never trips a per-IP limit
                            lambda: random.uniform(4.0, 6.0)))
    return actors


def build_scrapers(used, truth, n_ips):
    actors = []
    for _ in range(n_ips):
        ip = rand_ip(used)
        truth[ip] = "scraper"
        counter = {"n": random.randint(1, 50)}

        def url(c=counter):
            c["n"] += 1
            return f"/products/{c['n']}"
        actors.append(Actor(ip, lambda: SCRAPER_UA, url,
                            lambda: random.uniform(0.15, 0.3)))
    return actors


def build_checkout_hammer(used, truth, n_ips):
    actors = []
    for _ in range(n_ips):
        ip = rand_ip(used)
        truth[ip] = "checkout_hammer"
        actors.append(Actor(ip, lambda: BOT_UA,
                            lambda: "/checkout",
                            lambda: random.uniform(0.08, 0.2),
                            method_fn=lambda: "POST"))
    return actors


def main():
    ap = argparse.ArgumentParser(description="Traffic simulator -> Kafka")
    ap.add_argument("--bootstrap", default="localhost:29092")
    ap.add_argument("--rate", type=float, default=50, help="approx. normal-human events/sec")
    ap.add_argument("--duration", type=int, default=60, help="seconds to run")
    ap.add_argument("--attacks", default="flood,lowslow,scraper,checkout",
                    help="comma list: flood,lowslow,scraper,checkout or 'none'")
    ap.add_argument("--attack-start", type=int, default=10, help="seconds before attacks begin")
    ap.add_argument("--ground-truth", default="output/ground_truth.json")
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    used, truth = set(), {}
    # avg human gap is ~5s, so humans needed = rate * 5
    humans = build_humans(max(10, int(args.rate * 5)), used)
    attackers = []
    chosen = {a.strip() for a in args.attacks.split(",")}
    if "flood" in chosen:
        attackers += build_flood(used, truth, args.rate)
    if "lowslow" in chosen:
        attackers += build_low_and_slow(used, truth, max(30, int(args.rate)))
    if "scraper" in chosen:
        attackers += build_scrapers(used, truth, 3)
    if "checkout" in chosen:
        attackers += build_checkout_hammer(used, truth, 3)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
        linger_ms=20, batch_size=64 * 1024, compression_type="gzip", acks=1,
    )

    start = time.time()
    heap, tie = [], 0
    for a in humans:  # humans start spread out over the first seconds
        heapq.heappush(heap, (start + random.uniform(0, 8), tie, a)); tie += 1
    for a in attackers:  # attackers wait for attack_start
        heapq.heappush(heap, (start + args.attack_start + random.uniform(0, 2), tie, a)); tie += 1

    end = start + args.duration
    sent = 0
    print(f"Running {args.duration}s: {len(humans)} humans, {len(attackers)} attacker actors "
          f"({len(truth)} attacker IPs). Attacks start at +{args.attack_start}s.")

    try:
        while heap:
            t, _, actor = heapq.heappop(heap)
            if t >= end:
                break
            wait = t - time.time()
            if wait > 0:
                time.sleep(wait)
            producer.send(TOPIC, key=actor.ip, value=actor.make_event(t))
            sent += 1
            heapq.heappush(heap, (t + max(0.001, actor.interval_fn()), tie, actor)); tie += 1
            if sent % 500 == 0:
                print(f"  sent {sent} events ({t - start:.0f}s)")
    except KeyboardInterrupt:
        print("Interrupted.")
    finally:
        producer.flush()
        producer.close()

    with open(args.ground_truth, "w") as f:
        json.dump({"attacker_ips": truth, "total_events_sent": sent,
                   "generated_at": now_iso()}, f, indent=2)
    print(f"Done. Sent {sent} events. Ground truth: {args.ground_truth} ({len(truth)} attacker IPs)")


if __name__ == "__main__":
    main()