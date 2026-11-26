import { useEffect, useState } from "react";
import {
  AreaChart, Area, XAxis, YAxis, Tooltip, Legend,
  CartesianGrid, ResponsiveContainer,
} from "recharts";

const API = import.meta.env.VITE_API_URL || "http://localhost:4000";
const GRAFANA = import.meta.env.VITE_GRAFANA_URL || "http://localhost:3000";
const REFRESH_MS = 5000;

const fmt = (n) => Number(n || 0).toLocaleString();
const clock = (t) =>
  new Date(t).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });

function useApi(path) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  useEffect(() => {
    let alive = true;
    const load = async () => {
      try {
        const r = await fetch(`${API}${path}`);
        const j = await r.json();
        if (!r.ok || j.error) throw new Error(j.error || `HTTP ${r.status}`);
        if (alive) { setData(j); setError(null); }
      } catch (e) {
        if (alive) setError(e.message);
      }
    };
    load();
    const id = setInterval(load, REFRESH_MS);
    return () => { alive = false; clearInterval(id); };
  }, [path]);
  return { data, error };
}

function StatCard({ label, value, tone }) {
  return (
    <div className={`card ${tone || ""}`}>
      <div className="card-label">{label}</div>
      <div className="card-value">{value}</div>
    </div>
  );
}

// Real detector scores run about 0.5 to 0.78, so thresholds sit in that range
function ScoreBar({ score }) {
  const level = score >= 0.7 ? "high" : score >= 0.6 ? "mid" : "low";
  return (
    <div className="scorebar">
      <div className={`scorefill ${level}`} style={{ width: `${Math.round(score * 100)}%` }} />
      <span>{score.toFixed(2)}</span>
    </div>
  );
}

function ttlText(s) {
  if (s == null || s < 0) return "-";
  const m = Math.floor(s / 60);
  return m >= 60 ? `${Math.floor(m / 60)}h ${m % 60}m` : `${m}m ${s % 60}s`;
}

export default function App() {
  const summary = useApi("/api/summary");
  const series = useApi("/api/timeseries?minutes=15");
  const suspicious = useApi("/api/suspicious");
  const alerts = useApi("/api/alerts");

  const s = summary.data;
  const list = suspicious.data?.items || [];
  const total = suspicious.data?.total ?? 0;
  const anyError = summary.error || series.error || suspicious.error;

  return (
    <div className="page">
      <header>
        <div>
          <h1>Anti-DDoS Gateway Monitor</h1>
          <p className="sub">Live traffic, blocked requests and suspicious IPs (refreshes every 5s)</p>
        </div>
        <span className={`status ${anyError ? "bad" : "ok"}`}>
          {anyError ? "API problem" : "Live"}
        </span>
      </header>

      {anyError && <div className="banner">Could not load some data: {anyError}</div>}

      {Array.isArray(alerts.data) && alerts.data.length > 0 && (
        <section className="alerts">
          {alerts.data.map((a) => (
            <div key={a.name} className={`alert ${a.severity}`}>
              <strong>{a.name}</strong>
              <span>{a.summary}</span>
              <em>{a.state}</em>
            </div>
          ))}
        </section>
      )}

      <section className="cards">
        <StatCard label="Allowed (5m)" value={fmt(s?.allowed)} tone="good" />
        <StatCard label="Rate limited (5m)" value={fmt(s?.rate_limited)} tone="warn" />
        <StatCard label="Blocked (5m)" value={fmt(s?.blocked)} tone="bad" />
        <StatCard label="Block ratio" value={s ? `${(s.block_ratio * 100).toFixed(1)}%` : "-"} />
        <StatCard label="Requests / sec" value={s ? s.rps : "-"} />
      </section>

      <section className="panel">
        <h2>Traffic (requests/sec, last 15 min)</h2>
        <div className="chart">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={series.data || []}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2a3140" />
              <XAxis dataKey="t" tickFormatter={clock} stroke="#8b94a7" minTickGap={40} />
              <YAxis stroke="#8b94a7" />
              <Tooltip labelFormatter={clock} contentStyle={{ background: "#151a23", border: "1px solid #2a3140" }} />
              <Legend />
              <Area type="monotone" dataKey="allowed" stackId="1" stroke="#34d399" fill="#34d39955" />
              <Area type="monotone" dataKey="rate_limited" stackId="1" stroke="#fbbf24" fill="#fbbf2455" />
              <Area type="monotone" dataKey="blocked" stackId="1" stroke="#f87171" fill="#f8717155" />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </section>

      <section className="panel">
        <h2>Suspicious IPs <span className="count">{total}</span></h2>
        <p className="sub" style={{ marginBottom: 10 }}>
          Showing top {list.length} of {total} flagged IPs, highest score first
        </p>
        <div className="tablewrap">
          <table>
            <thead>
              <tr><th>IP</th><th>Score</th><th>Reason</th><th>Detected</th><th>Block expires in</th></tr>
            </thead>
            <tbody>
              {list.map((r) => (
                <tr key={r.ip}>
                  <td className="mono">{r.ip}</td>
                  <td><ScoreBar score={r.score} /></td>
                  <td>{r.reason}</td>
                  <td>{r.detected_at ? clock(r.detected_at) : "-"}</td>
                  <td>{ttlText(r.ttl)}</td>
                </tr>
              ))}
              {suspicious.data && list.length === 0 && (
                <tr><td colSpan="5" className="empty">No suspicious IPs right now</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      <section className="grafana">
        <div className="panel">
          <h2>Grafana: requests by decision</h2>
          <iframe title="requests" src={`${GRAFANA}/d-solo/gateway-traffic/gateway-traffic?orgId=1&panelId=1&refresh=5s&theme=dark`} />
        </div>
        <div className="panel">
          <h2>Grafana: p95 latency</h2>
          <iframe title="latency" src={`${GRAFANA}/d-solo/gateway-traffic/gateway-traffic?orgId=1&panelId=4&refresh=5s&theme=dark`} />
        </div>
      </section>
    </div>
  );
}