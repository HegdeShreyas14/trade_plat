import { useEffect, useMemo, useState } from "react";
import {
  Activity,
  Gauge,
  Medal,
  Radio,
  ShieldAlert,
  ShieldCheck,
  Trophy,
  Upload,
  FileCode,
  CheckCircle,
  XCircle,
  Sun,
  Moon
} from "lucide-react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

function formatNumber(value, digits = 0) {
  if (!Number.isFinite(value)) {
    return "0";
  }
  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  }).format(value);
}

function normalizeRows(rows) {
  return rows.map((row, index) => ({
    rank: row.rank ?? index + 1,
    id: row.id ?? row.contestant_id ?? "unknown",
    score: Number(row.score ?? 0),
    tps: Number(row.tps ?? 0),
    p50_lat_us: Number(row.p50_lat_us ?? row.engine_p50_us ?? 0),
    p90_lat_us: Number(row.p90_lat_us ?? row.engine_p90_us ?? 0),
    p99_lat_us: Number(row.p99_lat_us ?? row.engine_p99_us ?? 0),
    correctness: row.correctness ?? "UNKNOWN",
    total_events: Number(row.total_events ?? row.consumed_total ?? 0),
    last_match_id: Number(row.last_match_id ?? 0),
  }));
}

function parseRows(event) {
  try {
    return normalizeRows(JSON.parse(event.data));
  } catch {
    return [];
  }
}

function StatusPill({ connected }) {
  return (
    <div className={`status-pill ${connected ? "connected" : "offline"}`}>
      <Radio size={16} aria-hidden="true" />
      <span>{connected ? "Live stream" : "Waiting for API"}</span>
    </div>
  );
}

function Stat({ icon: Icon, label, value, tone = "neutral" }) {
  return (
    <section className={`stat ${tone}`}>
      <div className="stat-icon">
        <Icon size={18} aria-hidden="true" />
      </div>
      <div>
        <span>{label}</span>
        <strong>{value}</strong>
      </div>
    </section>
  );
}

function Correctness({ value }) {
  const passed = value === "PASSED";
  const Icon = passed ? ShieldCheck : ShieldAlert;
  return (
    <span className={`correctness ${passed ? "passed" : "failed"}`}>
      <Icon size={15} aria-hidden="true" />
      {value}
    </span>
  );
}

export default function App() {
  const [rows, setRows] = useState([]);
  const [connected, setConnected] = useState(false);
  const [updatedAt, setUpdatedAt] = useState(null);

  const [file, setFile] = useState(null);
  const [contestantId, setContestantId] = useState("");
  const [uploading, setUploading] = useState(false);
  const [uploadStatus, setUploadStatus] = useState({ type: null, message: "" });

  const [isDark, setIsDark] = useState(() => {
    if (typeof window !== "undefined") {
      const savedTheme = localStorage.getItem("theme");
      if (savedTheme) return savedTheme === "dark";
      return window.matchMedia("(prefers-color-scheme: dark)").matches;
    }
    return true;
  });

  useEffect(() => {
    const root = document.documentElement;
    if (isDark) {
      root.setAttribute("data-theme", "dark");
      localStorage.setItem("theme", "dark");
    } else {
      root.setAttribute("data-theme", "light");
      localStorage.setItem("theme", "light");
    }
  }, [isDark]);

  useEffect(() => {
    let closed = false;

    async function loadSnapshot() {
      try {
        const response = await fetch(`${API_URL}/api/v1/leaderboard`);
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        const data = await response.json();
        if (!closed) {
          setRows(normalizeRows(data));
          setUpdatedAt(new Date());
        }
      } catch {
        if (!closed) {
          setConnected(false);
        }
      }
    }

    loadSnapshot();
    const source = new EventSource(`${API_URL}/api/v1/leaderboard/stream`);

    const onRows = (event) => {
      setRows(parseRows(event));
      setConnected(true);
      setUpdatedAt(new Date());
    };

    source.addEventListener("snapshot", onRows);
    source.addEventListener("update", onRows);
    source.onerror = () => setConnected(false);
    source.onopen = () => setConnected(true);

    return () => {
      closed = true;
      source.close();
    };
  }, []);

  const totals = useMemo(() => {
    return rows.reduce(
      (acc, row) => {
        acc.tps += row.tps;
        acc.events += row.total_events;
        acc.p99 = Math.max(acc.p99, row.p99_lat_us);
        return acc;
      },
      { tps: 0, events: 0, p99: 0 },
    );
  }, [rows]);

  const chartRows = rows.slice(0, 10).map((row) => ({
    name: row.id,
    score: Number(row.score.toFixed(2)),
    p99: Number(row.p99_lat_us.toFixed(2)),
    tps: Number(row.tps.toFixed(2)),
  }));

  const handleFileChange = (e) => {
    const selectedFile = e.target.files[0];
    if (selectedFile && (selectedFile.type === "application/zip" || selectedFile.name.endsWith('.zip'))) {
      setFile(selectedFile);
      setUploadStatus({ type: null, message: "" });
    } else {
      setFile(null);
      setUploadStatus({ type: "error", message: "Invalid format. Please supply a valid .zip code archive." });
    }
  };

  const handleUploadSubmit = async () => {
    if (!file) return;
    const trimmedId = contestantId.trim();
    if (!trimmedId) {
      setUploadStatus({ type: "error", message: "Enter a contestant name before submitting." });
      return;
    }
    setUploading(true);
    setUploadStatus({ type: "info", message: "Pushing code package to orchestration gateway..." });

    const formData = new FormData();
    formData.append("file", file);
    formData.append("contestant_id", trimmedId);

    try {
      // FIXED SCHEMA TARGET: Direct connection URL targets now bind explicitly to port 8000
      const response = await fetch(`${API_URL}/api/v1/submissions/upload`, {
        method: 'POST',
        body: formData,
      });

      if (response.ok) {
        setUploadStatus({ type: "success", message: "Sandbox provisioning sequence triggered successfully!" });
        setFile(null);
      } else {
        const errData = await response.json();
        setUploadStatus({ type: "error", message: errData.detail || "Upload rejected by evaluator." });
      }
    } catch (err) {
      setUploadStatus({ type: "error", message: "Network timeout connecting to orchestration mesh." });
    } finally {
      setUploading(false);
    }
  };

  const top = rows[0];

  return (
    <main className="shell">
      <header className="topbar">
        <div>
          <div className="eyebrow">IICPC market simulator</div>
          <h1>Live HFT Leaderboard</h1>
        </div>
        <div className="topbar-actions" style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
          <button
            onClick={() => setIsDark(!isDark)}
            aria-label="Toggle structural theme layout view"
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              background: 'rgba(255, 255, 255, 0.06)',
              border: '1px solid var(--border-color, rgba(255,255,255,0.1))',
              borderRadius: '50%',
              width: '36px',
              height: '36px',
              cursor: 'pointer',
              color: 'var(--text-color, #ffffff)',
              transition: 'all 0.2s'
            }}
          >
            {isDark ? <Sun size={18} style={{ color: '#fbbf24' }} /> : <Moon size={18} style={{ color: '#475569' }} />}
          </button>
          
          {updatedAt && <time>{updatedAt.toLocaleTimeString()}</time>}
          <StatusPill connected={connected} />
        </div>
      </header>

      <section className="stats-grid" aria-label="Leaderboard summary">
        <Stat
          icon={Trophy}
          label="Current leader"
          value={top ? top.id : "No scores"}
          tone="gold"
        />
        <Stat icon={Gauge} label="Aggregate TPS" value={formatNumber(totals.tps, 1)} />
        <Stat icon={Activity} label="Worst p99" value={`${formatNumber(totals.p99, 2)} us`} />
        <Stat icon={Medal} label="Total fills" value={formatNumber(totals.events)} />
      </section>

      <section className="panel upload-panel" style={{ marginBottom: '2rem' }}>
        <div className="panel-title">
          <h2 style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <FileCode size={18} style={{ color: '#2f7865' }} /> Agent Code Submission
          </h2>
          <span>ZIP archive upload</span>
        </div>
        
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '1.5rem', alignItems: 'center', marginTop: '1rem' }}>
          <label style={{
            flex: '1 1 400px',
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
            padding: '1.5rem',
            border: '2px dashed var(--border-color, #334155)',
            borderRadius: '8px',
            cursor: 'pointer',
            background: 'rgba(255,255,255,0.02)',
            transition: 'all 0.2s'
          }}>
            <Upload size={24} style={{ color: '#64748b', marginBottom: '8px' }} />
            <p style={{ fontSize: '14px', margin: 0, fontWeight: 500 }}>
              {file ? <span style={{ color: '#2f7865' }}>{file.name}</span> : "Click to browse or drop your algorithm .zip here"}
            </p>
            <p style={{ fontSize: '11px', color: '#64748b', marginTop: '4px', marginBottom: 0 }}>
              Accepts C++, Python, Go, or Rust source files wrapped in a single zip package
            </p>
            <input type="file" className="hidden" accept=".zip" onChange={handleFileChange} disabled={uploading} style={{ display: 'none' }} />
          </label>

          <div style={{ flex: '1 1 200px', display: 'flex', flexDirection: 'column', gap: '10px' }}>
            <input
              type="text"
              placeholder="Contestant name"
              value={contestantId}
              onChange={(e) => setContestantId(e.target.value)}
              disabled={uploading}
              style={{
                width: '100%',
                padding: '10px 12px',
                borderRadius: '6px',
                border: '1px solid var(--border-color, #334155)',
                background: 'rgba(255,255,255,0.04)',
                color: 'var(--text-color, #ffffff)',
                fontSize: '14px',
                outline: 'none',
                boxSizing: 'border-box',
              }}
            />
            <button
              onClick={handleUploadSubmit}
              disabled={!file || uploading}
              style={{
                width: '100%',
                padding: '12px',
                borderRadius: '6px',
                border: 'none',
                fontWeight: 600,
                fontSize: '14px',
                cursor: (!file || uploading) ? 'not-allowed' : 'pointer',
                background: (!file || uploading) ? 'rgba(255,255,255,0.05)' : '#2f7865',
                color: (!file || uploading) ? '#64748b' : '#ffffff',
                transition: 'all 0.2s'
              }}
            >
              {uploading ? "Deploying Code..." : "Launch Into Sandbox"}
            </button>
            
            {uploadStatus.message && (
              <div style={{
                display: 'flex',
                alignItems: 'start',
                gap: '6px',
                fontSize: '12px',
                padding: '8px 12px',
                borderRadius: '4px',
                border: '1px solid',
                background: uploadStatus.type === 'success' ? 'rgba(16,185,129,0.1)' : uploadStatus.type === 'error' ? 'rgba(244,63,94,0.1)' : 'rgba(59,130,246,0.1)',
                color: uploadStatus.type === 'success' ? '#10b981' : uploadStatus.type === 'error' ? '#f43f5e' : '#3b82f6',
                borderColor: uploadStatus.type === 'success' ? 'rgba(16,185,129,0.2)' : uploadStatus.type === 'error' ? 'rgba(244,63,94,0.2)' : 'rgba(59,130,246,0.2)'
              }}>
                {uploadStatus.type === 'success' ? <CheckCircle size={14} style={{ marginTop: '2px', flexShrink: 0 }} /> : <XCircle size={14} style={{ marginTop: '2px', flexShrink: 0 }} />}
                <span>{uploadStatus.message}</span>
              </div>
            )}
          </div>
        </div>
      </section>

      <section className="dashboard-grid">
        <div className="panel chart-panel">
          <div className="panel-title">
            <h2>Score by Contestant</h2>
            <span>top 10</span>
          </div>
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={chartRows} margin={{ top: 8, right: 8, bottom: 8, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} />
              <XAxis dataKey="name" tick={{ fontSize: 11 }} interval={0} />
              <YAxis tick={{ fontSize: 11 }} width={56} />
              <Tooltip cursor={{ fill: "rgba(47, 120, 101, 0.08)" }} />
              <Bar dataKey="score" fill="#2f7865" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        <div className="panel chart-panel">
          <div className="panel-title">
            <h2>Latency Profile</h2>
            <span>p99 us</span>
          </div>
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={chartRows} margin={{ top: 8, right: 16, bottom: 8, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} />
              <XAxis dataKey="name" tick={{ fontSize: 11 }} interval={0} />
              <YAxis tick={{ fontSize: 11 }} width={56} />
              <Tooltip />
              <Line
                type="monotone"
                dataKey="p99"
                stroke="#b85b31"
                strokeWidth={2}
                dot={{ r: 3 }}
                activeDot={{ r: 5 }}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </section>

      <section className="panel table-panel">
        <div className="panel-title">
          <h2>Contestant Standings</h2>
          <span>{rows.length} active</span>
        </div>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Rank</th>
                <th>Contestant</th>
                <th>Correctness</th>
                <th>Score</th>
                <th>TPS</th>
                <th>p50</th>
                <th>p90</th>
                <th>p99</th>
                <th>Events</th>
                <th>Last Match</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.id}>
                  <td>#{row.rank}</td>
                  <td className="contestant">{row.id}</td>
                  <td>
                    <Correctness value={row.correctness} />
                  </td>
                  <td>{formatNumber(row.score, 4)}</td>
                  <td>{formatNumber(row.tps, 1)}</td>
                  <td>{formatNumber(row.p50_lat_us, 2)} us</td>
                  <td>{formatNumber(row.p90_lat_us, 2)} us</td>
                  <td>{formatNumber(row.p99_lat_us, 2)} us</td>
                  <td>{formatNumber(row.total_events)}</td>
                  <td>{formatNumber(row.last_match_id)}</td>
                </tr>
              ))}
              {rows.length === 0 && (
                <tr>
                  <td className="empty" colSpan="10">
                    No leaderboard data yet.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </main>
  );
}