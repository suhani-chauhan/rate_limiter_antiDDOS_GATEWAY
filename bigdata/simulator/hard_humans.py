#!/usr/bin/env python3
"""Hard-to-classify HUMAN traffic -> Kafka. None of these IPs are attackers."""
import argparse
import heapq
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC = "gateway.traffic.events"
BROWSERS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 Chrome/124.0 Mobile Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
]
CHROME = BROWSERS[0]
PAGES = ["/", "/home", "/products", "/products/{n}", "/cart", "/search?q=shoes",
         "/static/app.js", "/static/style.css", "/api/offers"]


def now_iso():
    n = datetime.now(timezone.utc)
    return n.strftime("%Y-%m-%dT%H:%M:%S.") + f"{n.microsecond // 1000:03d}Z"


class Actor:
    def __init__(self, ip, ua_fn, url_fn, gap_fn, start, stop):
        self.ip, self.ua_fn, self.url_fn, self.gap_fn = ip, ua_fn, url_fn, gap_fn
        self.start, self.stop = start, stop

    def event(self):
        return {"timestamp": now_iso(), "ip": self.ip, "method": "GET",
                "url": self.url_fn(), "status_code": 200, "decision": "allowed",
                "user_agent": self.ua_fn(), "latency_ms": random.randint(20, 250)}


def page():
    return random.choice(PAGES).format(n=random.randint(1, 500))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:29092")
    ap.add_argument("--duration", type=int, default=90)
    ap.add_argument("--out", default="output/hard_humans.json")
    args = ap.parse_args()

    used, truth, actors = set(), {}, []

    def ip_for(scenario):
        while True:
            ip = f"198.18.{random.randint(0, 255)}.{random.randint(1, 254)}"
            if ip not in used:
                used.add(ip)
                truth[ip] = scenario
                return ip

    # 1) heavy shoppers: refresh one product page, fairly regular rhythm
    for _ in range(5):
        ua = random.choice(BROWSERS)
        actors.append(Actor(ip_for("heavy_shopper"), lambda ua=ua: ua,
                            lambda: "/products/42" if random.random() < 0.92 else page(),
                            lambda: random.uniform(1.5, 5.0),
                            random.uniform(0, 5), args.duration))

    # 2) office NAT: ONE ip, ~30 people, several browsers, ~6 req/s combined
    actors.append(Actor(ip_for("office_nat"), lambda: random.choice(BROWSERS), page,
                        lambda: random.expovariate(6.0), 0, args.duration))

    # 3) flash-sale crowd: 60 different people, same Chrome, same product page
    for _ in range(60):
        actors.append(Actor(ip_for("flash_sale_crowd"), lambda: CHROME,
                            lambda: "/products/777" if random.random() < 0.9 else "/checkout",
                            lambda: random.expovariate(1 / 3.0),
                            10 + random.uniform(0, 5), 50))

    producer = KafkaProducer(bootstrap_servers=args.bootstrap,
                             key_serializer=lambda k: k.encode(),
                             value_serializer=lambda v: json.dumps(v).encode(),
                             linger_ms=20, compression_type="gzip", acks=1)
    t0 = time.time()
    heap = [(t0 + a.start, i, a) for i, a in enumerate(actors)]
    heapq.heapify(heap)
    tie, sent = len(actors), 0
    print(f"Running {args.duration}s: {len(truth)} hard-human IPs")
    while heap:
        t, _, a = heapq.heappop(heap)
        if t - t0 >= a.stop:
            continue
        wait = t - time.time()
        if wait > 0:
            time.sleep(wait)
        producer.send(TOPIC, key=a.ip, value=a.event())
        sent += 1
        heapq.heappush(heap, (t + max(0.01, a.gap_fn()), tie, a))
        tie += 1
    producer.flush()
    producer.close()
    with open(args.out, "w") as f:
        json.dump({"hard_human_ips": truth, "total_events_sent": sent}, f, indent=2)
    print(f"Done. Sent {sent} events. Wrote {args.out}")


if __name__ == "__main__":
    main()