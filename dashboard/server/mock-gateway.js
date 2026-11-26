const express = require("express");
const client = require("prom-client");
const Redis = require("ioredis");

const redis = new Redis(process.env.REDIS_URL || "redis://localhost:6379");

const requests = new client.Counter({
  name: "gateway_requests_total",
  help: "Requests by gateway decision",
  labelNames: ["decision"],
});
const latency = new client.Histogram({
  name: "gateway_request_duration_seconds",
  help: "Gateway request latency",
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1],
});
["allowed", "rate_limited", "blocked"].forEach((d) => requests.inc({ decision: d }, 0));

const rand = (a, b) => a + Math.random() * (b - a);

// Traffic simulation: runs every 250ms
setInterval(() => {
  const underAttack = Math.floor(Date.now() / 1000) % 90 < 25;
  requests.inc({ decision: "allowed" }, Math.round(rand(20, 26)));
  for (let i = 0; i < 5; i++) latency.observe(rand(0.02, underAttack ? 0.2 : 0.07));
  if (underAttack) {
    const burst = 150;
    requests.inc({ decision: "rate_limited" }, Math.round(burst * 0.7));
    requests.inc({ decision: "blocked" }, Math.round(burst * 0.2));
    requests.inc({ decision: "allowed" }, Math.round(burst * 0.1));
  }
}, 250);

// Seed Redis the way Part B writes it
const ATTACKERS = [
  ["203.0.113.10", "request rate 48/s, 80% rate limited"],
  ["198.51.100.4", "near-constant timing (cv 0.02), repeated /checkout"],
  ["198.51.100.17", "near-constant timing (cv 0.02), repeated /checkout"],
  ["192.0.2.20", "scraping: 43 distinct URLs/min, UA Scrapy"],
  ["192.0.2.21", "scraping: 41 distinct URLs/min, UA Scrapy"],
  ["192.0.2.50", "endpoint hammering: 100% on /checkout, 10 req/s"],
  ["192.0.2.51", "endpoint hammering: 100% on /checkout, 9 req/s"],
];
async function seed() {
  for (const [ip, reason] of ATTACKERS) {
    if (Math.random() < 0.3) continue;
    const score = +rand(0.6, 0.99).toFixed(2);
    await redis.zadd("suspicious:ips", score, ip);
    await redis.set(
      `blocked:ip:${ip}`,
      JSON.stringify({ score, reason, detected_at: new Date().toISOString() }),
      "EX", 3600
    );
  }
}
if (process.env.SEED_REDIS === "1") {
  seed();
  setInterval(seed, 10000);
}

const app = express();
app.get("/metrics", async (_req, res) => {
  res.set("Content-Type", client.register.contentType);
  res.end(await client.register.metrics());
});
app.listen(8090, () => console.log("mock gateway metrics on :8090/metrics"));