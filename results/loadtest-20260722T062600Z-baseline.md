# Load test — baseline

**Run at:** 2026-07-22T06:26:00.840489+00:00  
**Runs:** 5 of 5 (60s each, full stack restart between runs)  
**Fleet:** 170 bots (50 market maker, 100 noise, 20 momentum)  
**Correctness:** PASSED (fleet drops: 0, fails: 0)

## Results across runs

| Metric | Min | Median | Max | Spread |
| --- | ---: | ---: | ---: | ---: |
| Engine throughput (trades/s) | 7,410.50 | **7,760.00** | 7,961.00 | 1.07x |
| Engine throughput peak (trades/s) | 7,829.00 | **8,259.00** | 8,816.00 | 1.13x |
| P50 latency (us) | 3.56 | **3.56** | 3.56 | 1.0x |
| P90 latency (us) | 7.12 | **8.31** | 9.50 | 1.33x |
| P99 latency (us) | 14.25 | **16.62** | 20.18 | 1.42x |
| P99 worst sample (us) | 9,670.64 | **22,006.78** | 25,392.95 | 2.63x |
| Fleet send rate (orders/s) | 10,606.00 | **10,658.00** | 10,708.00 | 1.01x |
| Trade events per run | 411,963.00 | **432,516.00** | 438,596.00 | 1.06x |

**Median is the figure to quote.** Spread is max/min across runs; anything much
above ~1.5x means run-to-run variance dominates and a single run would misreport
the platform.

## Per-run detail

| # | Trades/s | P50 | P90 | P99 | Events | Backlog at start | Correctness |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 7,961 | 3.56 | 8.31 | 16.62 | 434,202 | 0 | PASSED |
| 2 | 7,760 | 3.56 | 9.50 | 20.18 | 432,516 | 0 | PASSED |
| 3 | 7,410 | 3.56 | 8.31 | 20.18 | 411,963 | 0 | PASSED |
| 4 | 7,547 | 3.56 | 7.12 | 15.44 | 425,814 | 0 | PASSED |
| 5 | 7,858 | 3.56 | 7.12 | 14.25 | 438,596 | 0 | PASSED |

`Backlog at start` is the scoring engine's cumulative event counter when the run
began. A fresh stack reads 0; a non-zero value means the run inherited state and
its numbers are not comparable to the others.

## What was measured

Latency is **engine-internal** — `t2 - t1` from the trade event, i.e. the span the
matching engine itself accounts for. It is **not** a wire-to-wire round trip and does
not include network, kernel, or client-side time. Throughput counts trade events
published to Kafka, which is match output, not inbound order rate — the fleet send
rate is reported separately for that.

Within each run, samples recording zero throughput (fleet ramp-up and post-run
drain) are dropped, then the leading 25% of what remains is
trimmed as warm-up while the scoring engine's rolling window fills.

## Limitations

**This does not establish the engine's ceiling.** The fleet's send rate is governed
by the bots' own hardcoded sleep intervals, not by backpressure from the engine: market
makers emit two orders per 10 ms, noise bots one per ~125 ms, momentum bots one per 50 ms.
That caps offered load at roughly 11.2k orders/s regardless of how fast the engine is, and
the observed send rate sits near that cap with zero drops. The engine absorbed everything
it was offered; it was never pushed to saturation.

Treat these figures as *latency under a fixed, modest offered load* rather than as capacity.
Establishing a real ceiling requires a load generator that can outrun the engine -- which
means sharding the fleet across processes/pods instead of running 170 coroutines in one
Python process, where the GIL and a single event loop bound throughput.

## Reproducing

```bash
REDIS_HOST_PORT=6380 python3 bench/run_loadtest.py \
    --runs 5 --duration 60 \
    --mm 50 --noise 100 --momentum 20 --label baseline
```

The harness brings the stack up and tears it down itself between runs.

## Host

- CPU: AMD Ryzen 9 8945HX with Radeon Graphics (32 cores)
- Memory: 15,270 MB
- Platform: Linux-6.8.0-136-generic-x86_64-with-glibc2.39
