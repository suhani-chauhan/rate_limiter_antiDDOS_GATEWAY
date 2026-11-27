const express = require("express");
const cors = require("cors");
const Redis = require("ioredis");

const PROM = process.env.PROM_URL || "http://localhost:9090";
const redis = new Redis(process.env.REDIS_URL || "redis://localhost:6379");

async function prom(path, params) {
  const r = await fetch(`${PROM}/api/v1/${path}?${new URLSearchParams(params)}`);
  const j = await r.json();
  if (j.status !== "success") throw new Error(j.error || "prometheus error");
  return j.data.result;
}

const app = express();
app.use(cors());

// Totals over the last 5 minutes + current req/s + block ratio
app.get("/api/summary", async (_req, res) => {
  try {
    const rows = await prom("query", {
      query: "sum by (decision)(increase(gateway_requests_total[5m]))",
    });
    const t = { allowed: 0, rate_limited: 0, blocked: 0 };
    rows.forEach((r) => (t[r.metric.decision] = Math.round(+r.value[1])));
    const rps = await prom("query", { query: "sum(rate(gateway_requests_total[30s]))" });
    const total = t.allowed + t.rate_limited + t.blocked;
    res.json({
      allowed: t.allowed,
      rate_limited: t.rate_limited,
      blocked: t.blocked,
      total,
      block_ratio: total ? (t.rate_limited + t.blocked) / total : 0,
      rps: rps.length ? +(+rps[0].value[1]).toFixed(1) : 0,
    });
  } catch (e) { res.status(502).json({ error: e.message }); }
});

// Time series for the React chart: [{t, allowed, rate_limited, blocked}]
app.get("/api/timeseries", async (req, res) => {
  try {
    const minutes = Math.min(+req.query.minutes || 15, 120);
    const end = Math.floor(Date.now() / 1000);
    const rows = await prom("query_range", {
      query: "sum by (decision)(rate(gateway_requests_total[30s]))",
      start: end - minutes * 60, end, step: 5,
    });
    const byT = new Map();
    rows.forEach((s) => s.values.forEach(([t, v]) => {
      const p = byT.get(t) || { t: t * 1000, allowed: 0, rate_limited: 0, blocked: 0 };
      p[s.metric.decision] = +(+v).toFixed(1);
      byT.set(t, p);
    }));
    res.json([...byT.values()].sort((a, b) => a.t - b.t));
  } catch (e) { res.status(502).json({ error: e.message }); }
});

// Suspicious IPs from Part B's Redis output
app.get("/api/suspicious", async (_req, res) => {
  try {
    const total = await redis.zcard("suspicious:ips");
    const raw = await redis.zrevrange("suspicious:ips", 0, 49, "WITHSCORES");
    const ips = [], scores = [];
    for (let i = 0; i < raw.length; i += 2) { ips.push(raw[i]); scores.push(+raw[i + 1]); }

    const pipe = redis.pipeline();
    ips.forEach((ip) => { pipe.get(`blocked:ip:${ip}`); pipe.ttl(`blocked:ip:${ip}`); });
    const results = ips.length ? await pipe.exec() : [];

    const items = [];
    ips.forEach((ip, i) => {
      const ttl = results[i * 2 + 1][1];
      if (ttl === -2) return; // key expired: hide stale IP
      let meta = {};
      try { meta = JSON.parse(results[i * 2][1] || "{}"); } catch {}
      items.push({
        ip, score: scores[i],
        reason: meta.reason || "", detected_at: meta.detected_at || null, ttl,
      });
    });
    res.json({ total, items });
  } catch (e) { res.status(502).json({ error: e.message }); }
});

app.get("/api/alerts", async (_req, res) => {
  try {
    const r = await fetch(`${PROM}/api/v1/alerts`);
    const j = await r.json();
    res.json(
      j.data.alerts.map((a) => ({
        name: a.labels.alertname,
        severity: a.labels.severity,
        state: a.state,
        summary: a.annotations.summary,
        since: a.activeAt,
      }))
    );
  } catch (e) { res.status(502).json({ error: e.message }); }
});

app.listen(4000, () => console.log("dashboard API on :4000"));