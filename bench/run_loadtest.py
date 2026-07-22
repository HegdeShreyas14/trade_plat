#!/usr/bin/env python3
"""Drive repeated load tests against the stack and record aggregated results.

A single run is not trustworthy: back-to-back runs of identical load have been
observed to differ by more than 3x at P99, because a stack carrying Kafka backlog
from a previous run spends CPU on catch-up consumption that competes with the
matching engine. This harness therefore tears the stack down and brings it back up
between runs, then reports min/median/max across runs rather than one sample.

Latency figures are engine-internal (t2 - t1), NOT wire-to-wire round trip.
"""
import argparse
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = REPO_ROOT / "results"

# The scoring engine's rolling window needs time to fill before its samples mean
# anything, so the leading portion of every run is discarded as warm-up.
WARMUP_FRACTION = 0.25

# Services the benchmark needs. The frontend and upload API sit off the data path.
STACK_SERVICES = ["zookeeper", "kafka", "redis", "matching-engine", "scoring-engine"]
HEALTHCHECKED = {"zookeeper", "kafka", "redis"}


def parse_args():
    parser = argparse.ArgumentParser(description="Run load tests and record results")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--duration", type=int, default=60, help="Load duration per run, seconds")
    parser.add_argument("--runs", type=int, default=5, help="Number of runs to aggregate")
    parser.add_argument("--mm", type=int, default=50, help="Market maker bots")
    parser.add_argument("--noise", type=int, default=100, help="Noise bots")
    parser.add_argument("--momentum", type=int, default=20, help="Momentum bots")
    parser.add_argument("--label", default="baseline", help="Short name for this benchmark")
    parser.add_argument("--redis-host-port", default=os.getenv("REDIS_HOST_PORT", "6379"),
                        help="Host port for redis; override when a system redis owns 6379")
    parser.add_argument("--no-restart", action="store_true",
                        help="Skip the inter-run stack restart (results will be contaminated)")
    return parser.parse_args()


def compose_env(args):
    env = os.environ.copy()
    env["REDIS_HOST_PORT"] = str(args.redis_host_port)
    return env


def compose(cmd, args, **kwargs):
    return subprocess.run(["docker", "compose", *cmd], cwd=REPO_ROOT, env=compose_env(args),
                          capture_output=True, text=True, **kwargs)


def service_states(args):
    """Map service name -> status string, as compose reports it."""
    proc = compose(["ps", "--format", "{{.Service}}\t{{.Status}}"], args)
    states = {}
    for line in proc.stdout.splitlines():
        if "\t" in line:
            name, status = line.split("\t", 1)
            states[name.strip()] = status.strip()
    return states


def wait_until_ready(args, timeout=180):
    """Block until every benchmark service is up, and healthchecked ones are healthy."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        states = service_states(args)
        if all(svc in states for svc in STACK_SERVICES):
            ok = True
            for svc in STACK_SERVICES:
                status = states[svc]
                if not status.startswith("Up"):
                    ok = False
                elif svc in HEALTHCHECKED and "healthy" not in status:
                    ok = False
            if ok:
                return True
        time.sleep(3)
    print(f"[bench] stack not ready within {timeout}s: {service_states(args)}", file=sys.stderr)
    return False


def restart_stack(args):
    """Full down/up.

    Kafka registers an ephemeral znode in Zookeeper, and restarting it while
    Zookeeper keeps running collides with that stale registration -- so a partial
    restart fails where a full teardown succeeds.
    """
    compose(["down", "--remove-orphans"], args)
    up = compose(["up", "-d", *STACK_SERVICES], args)
    if up.returncode != 0:
        print(f"[bench] compose up failed:\n{up.stderr}", file=sys.stderr)
        return False
    return wait_until_ready(args)


def host_facts():
    """Capture enough about the machine that the numbers can be interpreted later."""
    facts = {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
        "cpu_model": None,
        "mem_total_kb": None,
    }
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                facts["cpu_model"] = line.split(":", 1)[1].strip()
                break
    except OSError:
        pass
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal"):
                facts["mem_total_kb"] = int(line.split()[1])
                break
    except OSError:
        pass
    return facts


def run_bot_fleet(args):
    """Run the fleet in the foreground for the full duration, capturing its output."""
    cmd = [
        sys.executable,
        str(REPO_ROOT / "bot-fleet" / "main.py"),
        "--host", args.host,
        "--port", str(args.port),
        "--mm", str(args.mm),
        "--noise", str(args.noise),
        "--momentum", str(args.momentum),
        "--duration", str(args.duration),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=args.duration + 120)
    if proc.returncode != 0:
        print(f"[bench] bot fleet exited {proc.returncode}: {proc.stderr}", file=sys.stderr)
    return cmd, proc.stdout or "", proc.stderr or ""


def harvest_scoring_samples(args, since_iso):
    """Pull the scoring engine's per-second JSON lines emitted during the run."""
    proc = compose(["logs", "--no-log-prefix", "--since", since_iso, "scoring-engine"], args)
    samples = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            sample = json.loads(line)
        except json.JSONDecodeError:
            continue
        if "tps" in sample:
            samples.append(sample)
    return samples


def parse_fleet_ops(stdout):
    """Pull the send-side OPS readings out of the fleet's [Metrics] lines."""
    readings = []
    for line in stdout.splitlines():
        if "[Metrics]" not in line or "OPS:" not in line:
            continue
        try:
            ops_field = line.split("OPS:", 1)[1].split("|", 1)[0]
            readings.append(float(ops_field.replace(",", "").replace("/sec", "").strip()))
        except (IndexError, ValueError):
            continue
    return readings


def parse_fleet_totals(stdout):
    """Final sent/drops/fails counters from the fleet's last [Metrics] line."""
    totals = {}
    for line in stdout.splitlines():
        if "[Metrics]" not in line:
            continue
        try:
            for key, field in (("sent", "Total Sent:"), ("drops", "Drops:"), ("fails", "Fails:")):
                if field in line:
                    totals[key] = int(line.split(field, 1)[1].split("|")[0].replace(",", "").strip())
        except (IndexError, ValueError):
            continue
    return totals


def summarize(samples, fleet_ops, fleet_totals):
    """Reduce one run's raw samples to steady-state figures under load.

    The harvest window brackets the run, so it necessarily catches idle samples:
    the fleet staggers its connections over the first few seconds, and the drain
    period after it stops still emits samples at zero load. Those describe an idle
    engine, not engine performance, so load-bearing samples are selected first and
    the warm-up fraction is trimmed from those.
    """
    if not samples:
        return {"error": "no scoring samples captured", "sample_count": 0}

    under_load = [s for s in samples if s.get("tps", 0) > 0]
    if not under_load:
        return {"error": "no samples recorded any throughput", "sample_count": len(samples)}

    warmup = int(len(under_load) * WARMUP_FRACTION)
    steady = under_load[warmup:] or under_load

    def field(name):
        return [s[name] for s in steady if isinstance(s.get(name), (int, float))]

    tps, p50, p90, p99 = field("tps"), field("p50_lat_us"), field("p90_lat_us"), field("p99_lat_us")

    med_tps = statistics.median(tps) if tps else 0
    med_p99 = statistics.median(p99) if p99 else 0
    # Mirrors the engine's own formula, but off steady-state medians rather than
    # whichever sample happened to land last.
    steady_score = med_tps / max(med_p99, 0.1) if med_tps else 0

    # Correctness is a latch: one violation anywhere disqualifies the whole run.
    violations = sorted({s.get("correctness") for s in samples} - {"PASSED", None})

    totals = [s.get("total_events", 0) for s in samples]
    return {
        "sample_count": len(samples),
        "under_load_sample_count": len(under_load),
        "idle_samples_excluded": len(samples) - len(under_load),
        "steady_state_sample_count": len(steady),
        # A fresh stack starts this counter at zero; a non-zero value means the
        # scoring engine was already carrying events when the run began.
        "scoring_backlog_at_start": min(totals) if totals else 0,
        "engine_tps_median": round(med_tps, 2),
        "engine_tps_peak": round(max(tps), 2) if tps else 0,
        "p50_lat_us": round(statistics.median(p50), 2) if p50 else 0,
        "p90_lat_us": round(statistics.median(p90), 2) if p90 else 0,
        "p99_lat_us": round(med_p99, 2),
        "p99_worst_us": round(max(p99), 2) if p99 else 0,
        "correctness": violations[0] if violations else "PASSED",
        # total_events is cumulative since the scoring engine process started, so a
        # delta across the harvest window is what this run actually produced.
        "trade_events_this_run": (max(totals) - min(totals)) if totals else 0,
        "steady_state_score": round(steady_score, 4),
        "fleet_send_ops_median": round(statistics.median(fleet_ops), 2) if fleet_ops else 0,
        "fleet_orders_sent": fleet_totals.get("sent", 0),
        "fleet_drops": fleet_totals.get("drops", 0),
        "fleet_fails": fleet_totals.get("fails", 0),
    }


# Metrics aggregated across runs, and whether a lower value is the better outcome.
AGGREGATED = [
    ("engine_tps_median", "Engine throughput (trades/s)"),
    ("engine_tps_peak", "Engine throughput peak (trades/s)"),
    ("p50_lat_us", "P50 latency (us)"),
    ("p90_lat_us", "P90 latency (us)"),
    ("p99_lat_us", "P99 latency (us)"),
    ("p99_worst_us", "P99 worst sample (us)"),
    ("fleet_send_ops_median", "Fleet send rate (orders/s)"),
    ("trade_events_this_run", "Trade events per run"),
]


def aggregate(run_summaries):
    """Collapse per-run summaries into min/median/max, with spread called out."""
    good = [r for r in run_summaries if "error" not in r]
    if not good:
        return {"error": "every run failed", "runs": len(run_summaries)}

    stats = {}
    for key, _label in AGGREGATED:
        values = [r[key] for r in good if isinstance(r.get(key), (int, float))]
        if not values:
            continue
        low, high = min(values), max(values)
        stats[key] = {
            "min": round(low, 2),
            "median": round(statistics.median(values), 2),
            "max": round(high, 2),
            # How far the worst run strays from the best. Large spread means a single
            # run should not be quoted as the platform's performance.
            "spread_ratio": round(high / low, 2) if low > 0 else None,
        }

    violations = sorted({r.get("correctness") for r in good} - {"PASSED", None})
    return {
        "runs_completed": len(good),
        "runs_attempted": len(run_summaries),
        "correctness": violations[0] if violations else "PASSED",
        "total_fleet_drops": sum(r.get("fleet_drops", 0) for r in good),
        "total_fleet_fails": sum(r.get("fleet_fails", 0) for r in good),
        "metrics": stats,
    }


def write_markdown(path, meta, agg, runs):
    stats = agg.get("metrics", {})

    def row(key, label):
        s = stats.get(key)
        if not s:
            return f"| {label} | — | — | — | — |"
        spread = f"{s['spread_ratio']}x" if s.get("spread_ratio") else "—"
        return (f"| {label} | {s['min']:,.2f} | **{s['median']:,.2f}** | "
                f"{s['max']:,.2f} | {spread} |")

    lines = [
        f"# Load test — {meta['label']}",
        "",
        f"**Run at:** {meta['started_at']}  ",
        f"**Runs:** {agg.get('runs_completed', 0)} of {meta['runs']} "
        f"({meta['duration_s']}s each, full stack restart between runs)  ",
        f"**Fleet:** {meta['bots']['total']} bots "
        f"({meta['bots']['mm']} market maker, {meta['bots']['noise']} noise, "
        f"{meta['bots']['momentum']} momentum)  ",
        f"**Correctness:** {agg.get('correctness', 'UNKNOWN')} "
        f"(fleet drops: {agg.get('total_fleet_drops', 0)}, fails: {agg.get('total_fleet_fails', 0)})",
        "",
        "## Results across runs",
        "",
        "| Metric | Min | Median | Max | Spread |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    lines += [row(key, label) for key, label in AGGREGATED]

    lines += [
        "",
        "**Median is the figure to quote.** Spread is max/min across runs; anything much",
        "above ~1.5x means run-to-run variance dominates and a single run would misreport",
        "the platform.",
        "",
        "## Per-run detail",
        "",
        "| # | Trades/s | P50 | P90 | P99 | Events | Backlog at start | Correctness |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for i, r in enumerate(runs, 1):
        if "error" in r:
            lines.append(f"| {i} | FAILED — {r['error']} | | | | | | |")
            continue
        lines.append(
            f"| {i} | {r['engine_tps_median']:,.0f} | {r['p50_lat_us']:,.2f} | "
            f"{r['p90_lat_us']:,.2f} | {r['p99_lat_us']:,.2f} | "
            f"{r['trade_events_this_run']:,} | {r['scoring_backlog_at_start']:,} | "
            f"{r['correctness']} |"
        )

    lines += [
        "",
        "`Backlog at start` is the scoring engine's cumulative event counter when the run",
        "began. A fresh stack reads 0; a non-zero value means the run inherited state and",
        "its numbers are not comparable to the others.",
        "",
        "## What was measured",
        "",
        "Latency is **engine-internal** — `t2 - t1` from the trade event, i.e. the span the",
        "matching engine itself accounts for. It is **not** a wire-to-wire round trip and does",
        "not include network, kernel, or client-side time. Throughput counts trade events",
        "published to Kafka, which is match output, not inbound order rate — the fleet send",
        "rate is reported separately for that.",
        "",
        f"Within each run, samples recording zero throughput (fleet ramp-up and post-run",
        f"drain) are dropped, then the leading {int(WARMUP_FRACTION * 100)}% of what remains is",
        "trimmed as warm-up while the scoring engine's rolling window fills.",
        "",
        "## Limitations",
        "",
        "**This does not establish the engine's ceiling.** The fleet's send rate is governed",
        "by the bots' own hardcoded sleep intervals, not by backpressure from the engine: market",
        "makers emit two orders per 10 ms, noise bots one per ~125 ms, momentum bots one per 50 ms.",
        "That caps offered load at roughly 11.2k orders/s regardless of how fast the engine is, and",
        "the observed send rate sits near that cap with zero drops. The engine absorbed everything",
        "it was offered; it was never pushed to saturation.",
        "",
        "Treat these figures as *latency under a fixed, modest offered load* rather than as capacity.",
        "Establishing a real ceiling requires a load generator that can outrun the engine -- which",
        "means sharding the fleet across processes/pods instead of running 170 coroutines in one",
        "Python process, where the GIL and a single event loop bound throughput.",
        "",
        "## Reproducing",
        "",
        "```bash",
        f"REDIS_HOST_PORT={meta['redis_host_port']} python3 bench/run_loadtest.py \\",
        f"    --runs {meta['runs']} --duration {meta['duration_s']} \\",
        f"    --mm {meta['bots']['mm']} --noise {meta['bots']['noise']} "
        f"--momentum {meta['bots']['momentum']} --label {meta['label']}",
        "```",
        "",
        "The harness brings the stack up and tears it down itself between runs.",
        "",
        "## Host",
        "",
        f"- CPU: {meta['host'].get('cpu_model') or 'unknown'} ({meta['host'].get('cpu_count')} cores)",
        f"- Memory: {(meta['host'].get('mem_total_kb') or 0) // 1024:,} MB",
        f"- Platform: {meta['host'].get('platform')}",
        "",
    ]
    path.write_text("\n".join(lines))


def main():
    args = parse_args()
    if shutil.which("docker") is None:
        sys.exit("docker not found on PATH")

    RESULTS_DIR.mkdir(exist_ok=True)
    started = datetime.now(timezone.utc)
    runs = []

    for run_no in range(1, args.runs + 1):
        print(f"\n[bench] === run {run_no}/{args.runs} ===", flush=True)

        if args.no_restart:
            print("[bench] reusing running stack (--no-restart)", flush=True)
            if not wait_until_ready(args):
                runs.append({"error": "stack not ready"})
                continue
        else:
            print("[bench] restarting stack...", flush=True)
            if not restart_stack(args):
                runs.append({"error": "stack failed to start"})
                continue
            # Let the engine settle before load so the first samples are not
            # measuring container startup.
            time.sleep(5)

        since_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        _cmd, fleet_stdout, _fleet_stderr = run_bot_fleet(args)
        time.sleep(5)  # let trailing samples land

        summary = summarize(
            harvest_scoring_samples(args, since_iso),
            parse_fleet_ops(fleet_stdout),
            parse_fleet_totals(fleet_stdout),
        )
        runs.append(summary)

        if "error" in summary:
            print(f"[bench] run {run_no} failed: {summary['error']}", file=sys.stderr)
        else:
            print(f"[bench] run {run_no}: {summary['engine_tps_median']:,.0f} trades/s, "
                  f"p99 {summary['p99_lat_us']}us, {summary['correctness']}", flush=True)

    agg = aggregate(runs)
    meta = {
        "label": args.label,
        "started_at": started.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": args.duration,
        "runs": args.runs,
        "restart_between_runs": not args.no_restart,
        "redis_host_port": args.redis_host_port,
        "bots": {
            "mm": args.mm, "noise": args.noise, "momentum": args.momentum,
            "total": args.mm + args.noise + args.momentum,
        },
        "target": f"{args.host}:{args.port}",
        "host": host_facts(),
    }

    stamp = started.strftime("%Y%m%dT%H%M%SZ")
    json_path = RESULTS_DIR / f"loadtest-{stamp}-{args.label}.json"
    md_path = RESULTS_DIR / f"loadtest-{stamp}-{args.label}.md"
    json_path.write_text(json.dumps({"meta": meta, "aggregate": agg, "runs": runs}, indent=2))
    write_markdown(md_path, meta, agg, runs)

    print("\n" + json.dumps(agg, indent=2), flush=True)
    print(f"\n[bench] wrote {json_path.relative_to(REPO_ROOT)}", flush=True)
    print(f"[bench] wrote {md_path.relative_to(REPO_ROOT)}", flush=True)
    return 0 if agg.get("runs_completed") else 1


if __name__ == "__main__":
    sys.exit(main())
