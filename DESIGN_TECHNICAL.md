# HFT Market Simulator — Technical Design Document

**Author**: Shreyas Hegde  
**Date**: June 14, 2026  
**Version**: 1.0

---

## 1. System Purpose & Scope

The HFT Market Simulator is a distributed platform for evaluating algorithmic trading agents in a sandboxed, low-latency environment. It automates containerization (Python, Rust, Go, C++), enforces security isolation via Kubernetes, and provides real-time scoring based on throughput and latency metrics.

**Core Design Principle**: Decouple the high-performance matching engine (C++) from the orchestration and telemetry layers (Python), using Kafka as a durable event log to prevent data loss.

---

## 2. Architecture Overview

### 2.1 High-Level Components

```
┌─────────────────────────────────────────────────────┐
│ Frontend (React + Vite, port 5173)                  │
│ ├─ Live leaderboard table                           │
│ ├─ Bar chart (top 10 scores)                        │
│ └─ Line chart (P99 latency trends)                  │
└────────────────┬────────────────────────────────────┘
                 │ SSE (push updates)
┌────────────────▼────────────────────────────────────┐
│ API Gateway (FastAPI, port 8000)                    │
│ ├─ POST /api/v1/submissions/upload                  │
│ ├─ GET /api/v1/leaderboard                          │
│ └─ GET /api/v1/leaderboard/stream (SSE)             │
└────────────────┬────────────────────────────────────┘
                 │ async subprocess
┌────────────────▼────────────────────────────────────┐
│ Orchestrator (K8s, Docker)                          │
│ ├─ Auto-detect language                             │
│ ├─ Generate Dockerfile                              │
│ ├─ Build container image                            │
│ └─ Apply pod to K8s                                 │
└────────────────┬────────────────────────────────────┘
                 │ WebSocket (ws://:8080)
      ┌──────────┴──────────┐
      │                     │
┌─────▼──────────┐   ┌──────▼─────────────────────┐
│ Contestant Pod │   │ Matching Engine (C++)      │
│ (K8s Pod)      │   │ ├─ TCPServer (Core 2)      │
│ ├─ sandbox_    │   │ ├─ MatchingEngine (Core 1) │
│   sdk.py       │   │ ├─ KafkaOffloader (Core 3) │
│ ├─ solution.py │   │ └─ Latency histograms      │
│ └─ requirements│   │                            │
│   .txt         │   │ Port: 8080                 │
└────────────────┘   └──────┬─────────────────────┘
                            │ Binary TradeEvents
                     ┌──────▼──────────┐
                     │ Kafka Cluster   │
                     │ ├─ Zookeeper    │
                     │ ├─ Broker       │
                     │ └─ Topic:       │
                     │   trade-events  │
                     └──────┬──────────┘
                            │ Message consumption
                     ┌──────▼──────────────────┐
                     │ Scoring Engine (Python) │
                     │ ├─ Validate trades      │
                     │ ├─ Compute percentiles  │
                     │ └─ Update Redis         │
                     └──────┬──────────────────┘
                            │ ZADD, HSET, PUBLISH
                     ┌──────▼──────────┐
                     │ Redis Cluster   │
                     │ ├─ Sorted sets  │
                     │ │  (leaderboard)│
                     │ ├─ Hashes       │
                     │ │  (metadata)   │
                     │ └─ Pub/Sub      │
                     │    (updates)    │
                     └─────────────────┘
```

### 2.2 Data Flow Timeline

**T+0s**: User submits contestant.zip via browser upload  
**T+1s**: API extracts ZIP, spawns orchestrator async  
**T+3s**: Orchestrator auto-detects language, builds image  
**T+5s**: Scoring engine spawned (Kafka consumer ready)  
**T+6s**: Contestant pod starts, opens WebSocket to engine  
**T+8s**: Bot begins tick loop, submits orders  
**T+9ms**: Engine receives order, records t1, enqueues  
**T+11ms**: Engine matches, records t2, publishes TradeEvent  
**T+11.5ms**: Scoring engine receives event, validates  
**T+12.6ms**: Metrics computed, Redis updated, SSE event sent  
**T+12.8ms**: Browser receives update, re-renders leaderboard  
**T+60s**: Pod exits, final scores persist in Redis

---

## 3. Key Design Decisions

### 3.1 C++ for Matching Engine

**Why**: Latency-critical hot path demands compiled code, lock-free data structures, and CPU pinning.

**Implementation**:
- Single-threaded matching loop pinned to Core 1
- Lock-free MPSC queue (65536 elements) for order ingress
- std::map for order book (O(log n) insertion, supports price-level iteration)
- Histogram-based latency tracking (100k buckets × 1µs = 100ms max tracked)

**Trade-offs**: Low-level C++17, no garbage collection, predictable performance over development speed.

### 3.2 WebSocket over Raw TCP

**Why**: Standard protocol, firewall-friendly, Python library support, hand-rolled for latency control.

**Implementation**:
- Manual HTTP 101 upgrade handshake (no library overhead)
- Binary frame parsing (opcode 0x2), client masking (RFC 6455)
- 32-byte OrderMessage struct (little-endian): order_id (8B) + t0 (8B) + price (8B) + qty (4B) + side (1B) + padding (3B)

**Trade-offs**: Custom WebSocket parsing adds complexity but gives precise latency control.

### 3.3 Kafka for Trade Durability

**Why**: Decouple matching from scoring; provide durable event log for replay.

**Implementation**:
- Single partition (total ordering)
- 60-byte packed TradeEvent struct (no padding)
- Engine batches up to 1024 events per Kafka message
- Scoring engine reads with unique `group_id`, `auto.offset.reset=latest`

**Trade-offs**: Adds latency (Kafka polling at 50ms), but prevents trade loss.

### 3.4 Kubernetes for Sandbox

**Why**: Standardized, declarative, multi-layer security (NetworkPolicy, Pod Security Standard, seccomp).

**Implementation**:
- evaluation-sandbox namespace with restricted PSS
- NetworkPolicy: egress only to port 8080
- runAsNonRoot (UID 10002), dropped ALL capabilities, read-only root FS
- One pod per submission, deleted on completion

**Trade-offs**: K8s overhead and complexity, but industry-standard security model.

### 3.5 Redis for Leaderboard

**Why**: In-memory sorted sets ideal for ranking; Pub/Sub for SSE notifications.

**Implementation**:
- Sorted set: `ZADD leaderboard:scores <score> <contestant_id>`
- Hashes: `HSET contestant:meta:<id> {tps, p50, p99, score, ...}`
- Pub/Sub: scoring engine publishes to `leaderboard_updates`

**Trade-offs**: No persistence (ephemeral scores), but microsecond latency for leaderboard updates.

---

## 4. System Components

### 4.1 Matching Engine (C++)

**File**: `matching-engine/main.cpp`, `MatchingEngine.cpp`, `TCPServer.cpp`

**Threads**:
- **Core 0**: Main thread (idle loop for graceful shutdown)
- **Core 1**: `processLoop()` — dequeues orders, matches, records t2
- **Core 2**: `TCPServer::run()` — epoll, WebSocket parsing, records t1
- **Core 3**: `KafkaOffloader::run()` — drains SPSC queue, publishes to Kafka

**Data Structures**:
- `OrderBook`: std::map<price, vector<Order>> for buy/sell sides
- `MPSCQueue<Order>`: 65536-element lock-free ring buffer
- `SPSCQueue<TradeEvent>`: 65536-element lock-free ring buffer
- Histograms: 100k buckets per thread (latency tracking)

**Key Algorithm** (matching):
```cpp
while (bestBid >= bestAsk) {
    tradedQty = min(buyQty, sellQty);
    executionPrice = makerPrice;  // maker's limit
    t2 = system_clock::now();
    publish_to_kafka(TradeEvent{...});
    erase_matched_orders();
}
```

### 4.2 Orchestrator (Python)

**File**: `infra/orchestrator.py`

**Responsibilities**:
1. Load K8s config from `~/.kube/config`
2. Create namespace `evaluation-sandbox`
3. Apply NetworkPolicy (egress :8080 only)
4. Auto-detect language by scanning ZIP tree
5. Generate Dockerfile (Python/Rust/Go/C++)
6. Build image via Docker
7. Spawn scoring engine as subprocess
8. Apply pod to K8s, wait for completion
9. Cleanup (delete pod, terminate scoring engine)

**Language Detection**:
- Python: `requirements.txt`, `sandbox_sdk.py`, or `.py` files
- Rust: `Cargo.toml` or `.rs` files
- Go: `go.mod` or `.go` files
- C++: Default (requires Makefile in submission)

### 4.3 Scoring Engine (Python)

**File**: `telemetry/scoring_engine.py`

**Responsibilities**:
1. Create Kafka consumer with unique `group_id`
2. Poll with 50ms timeout
3. For each TradeEvent:
   - Unpack 60-byte binary struct
   - Validate (monotonic match_id, timestamp ordering, self-match check)
   - Record engine latency (t2 - t1)
4. Every 1 second:
   - Compute TPS, P50/P90/P99 over 10-second window
   - Calculate score = TPS / max(p99_us, 0.1)
   - Update Redis (ZADD, HSET, PUBLISH)

**Validation Checks**:
- match_id == last_match_id + 1 (gaps detected)
- t0 < t1 < t2 (timestamp ordering)
- buy_order_id != sell_order_id (no self-match)
- price > 0 and finite (valid price)
- qty > 0 (valid quantity)

**Disqualification**: First validation failure sets score = -1.0 (permanent).

### 4.4 API Gateway (FastAPI)

**File**: `telemetry/api.py`

**Endpoints**:
- `POST /api/v1/submissions/upload`: Extract ZIP, spawn orchestrator
- `GET /api/v1/leaderboard`: Return top 25 from Redis (ZREVRANGE)
- `GET /api/v1/leaderboard/stream`: SSE stream (Redis Pub/Sub subscriber)

**Concurrency**:
- Async event loop (uvicorn)
- Background tasks for orchestrator (non-blocking)
- Redis pub/sub for SSE notifications

### 4.5 Frontend (React + Vite)

**File**: `leaderboard/src/App.jsx`

**Features**:
- EventSource (SSE) for live updates
- Table: rank, contestant_id, score, TPS, P50/P90/P99, correctness, fills
- Bar chart: top 10 scores
- Line chart: top 10 P99 latencies
- Theme toggle (dark/light)

**State Management**:
- `rows`: leaderboard data
- `connected`: SSE connection status
- `isDark`: theme preference

---

## 5. Scoring Formula

$$\text{score} = \frac{\text{TPS}}{\max(\text{p99\_latency\_us}, 0.1)}$$

**Rationale**:
- **Numerator (TPS)**: Rewards high throughput, incentivizes market participation
- **Denominator (P99)**: Penalizes tail latency, reflects SLA requirements
- **Ratio**: Creates trade-off between speed and latency

**Examples**:
- 1000 TPS, P99=100µs → score=10 (balanced)
- 1000 TPS, P99=10µs → score=100 (exceptional)
- 100 TPS, P99=100µs → score=1 (poor)

**Disqualification**: Any correctness violation → score = -1.0 (always last)

---

## 6. Security Model

### 6.1 Isolation Layers

**Layer 1: Container Isolation**
- `runAsNonRoot: true` (UID 10002)
- `readOnlyRootFilesystem: true` (no persistence, no escape)
- `allowPrivilegeEscalation: false` (no setuid)

**Layer 2: Capability Dropping**
- All Linux capabilities dropped (CAP_NET_RAW, CAP_SYS_ADMIN, etc.)
- seccomp: RuntimeDefault (curated syscall whitelist)

**Layer 3: Network Isolation**
- NetworkPolicy: egress restricted to `10.0.2.2:8080` (matching engine host)
- Cannot reach other pods, internet, or internal services

**Layer 4: Filesystem Isolation**
- Read-only root FS (no write, no escape vector)
- No access to host filesystem

### 6.2 Threat Model

| Threat | Mitigation |
|--------|-----------|
| Arbitrary code execution | Read-only FS, no capabilities |
| Privilege escalation | runAsNonRoot, allowPrivilegeEscalation=false |
| Network exfiltration | NetworkPolicy (port 8080 only) |
| DNS resolution | No access to kube-dns (outside policy) |
| Resource exhaustion | Memory/CPU limits on pod |

---

## 7. Assumptions & Constraints

### 7.1 Assumptions

1. **Synchronous Clocks**: t0, t1, t2 recorded on different machines must be NTP-synchronized
2. **Kafka Availability**: Trade loss acceptable only if Kafka is permanently down
3. **Single Evaluation Window**: Contestants evaluated for 60 seconds, scores reset
4. **Honest Submissions**: No intentional protocol violations (validated by engine)
5. **Stateless Matching**: No order cancellation or modification (only submit/match)

### 7.2 Constraints

1. **Throughput Ceiling**: ~10k TPS per engine (single core bottleneck)
2. **Concurrent Bots**: ~170 connections per engine (TCP limit per process)
3. **Latency Floor**: ~5-10µs P50 (cache hits, empty queue)
4. **Order Book Size**: 100k price levels max (DRAM bounded)
5. **Retention**: Kafka 7-day retention (disk space constraint)

---

## 8. Operational Considerations

### 8.1 Deployment Checklist

- [ ] Commit all changes to git
- [ ] Fix CORS in `telemetry/api.py` (allow frontend origin)
- [ ] Test locally: `docker compose up`
- [ ] Verify leaderboard SSE updates in browser
- [ ] Load test with bot-fleet (170 concurrent connections)
- [ ] Verify Kafka event ordering (no gaps in match_id)
- [ ] Test disqualification on invalid orders
- [ ] Monitor Redis memory usage (ephemeral, safe)
- [ ] Verify orchestrator cleanup (pods fully deleted)
- [ ] Review Terraform for cloud deployment (if applicable)

### 8.2 Monitoring & Observability

**Matching Engine**:
- P50/P90/P99 latency histograms (printed to stdout every second)
- Throughput (orders/second, trades/second)
- Queue depth (MPSC, SPSC)

**Scoring Engine**:
- Kafka consumer lag (if behind, indicates processing slowdown)
- Validation failures (logged to stderr)
- Percentile computation time (should be < 10ms)

**API Gateway**:
- SSE client count (connected consumers)
- Upload request latency (should be < 1s)
- Orchestrator subprocess lifecycle

**Leaderboard**:
- SSE reconnection count (high = unstable network)
- Update latency (should be < 50ms from trade to browser)

### 8.3 Failure Modes

| Failure | Impact | Recovery |
|---------|--------|----------|
| Matching engine crash | Orders not processed | Docker restart (on-failure) |
| Kafka broker down | Trades lost until recovery | Manual recovery (Kafka persists on disk) |
| Scoring engine crash | Leaderboard updates stop | Orchestrator respawns it |
| Redis down | Leaderboard unavailable | Restart (ephemeral, safe) |
| Network partition | Pod/engine disconnected | Pod waits for reconnect |

---

## 9. Implementation Details

### 9.1 Protocol Wire Format

**OrderMessage (Bot → Engine)** — 32 bytes, little-endian:
```
[0-7]    order_id (uint64)
[8-15]   t0_ns (uint64, nanoseconds)
[16-23]  price (double, IEEE 754)
[24-27]  qty (uint32)
[28]     side (uint8: 1=BUY, 2=SELL)
[29-31]  padding (zeros)
```

**TradeEvent (Engine → Kafka)** — 60 bytes, little-endian, packed:
```
[0-7]    match_id (uint64, monotonic counter)
[8-15]   buy_order_id (uint64)
[16-23]  sell_order_id (uint64)
[24-31]  t0_ns (uint64, taker's bot time)
[32-39]  t1_ns (uint64, gateway ingest)
[40-47]  t2_ns (uint64, match time)
[48-55]  price (double)
[56-59]  qty (uint32)
```

### 9.2 CPU Pinning Strategy

```cpp
// Core 0: Reserved for OS, interrupts, jitter
// Core 1: MatchingEngine::processLoop() — hot path
// Core 2: TCPServer::run() — network I/O
// Core 3: KafkaOffloader::run() — Kafka publishing
```

Pinning isolates latency-sensitive threads from OS scheduler preemption.

### 9.3 Concurrency Model

**Matching Engine**:
- Lock-free MPSC queue (TCPServer → MatchingEngine)
- Lock-free SPSC queue (MatchingEngine → KafkaOffloader)
- Histograms flushed every 65536 orders (batched locking)

**Scoring Engine**:
- Single-threaded (Kafka consumer loop)
- Sliding window over 10 seconds
- Redis operations atomically published

**API Gateway**:
- Async event loop (uvicorn + asyncio)
- Concurrent SSE clients (non-blocking pub/sub)

---

## 10. Future Enhancements

1. **eBPF Kernel Tracing**: Measure kernel vs. userspace latency contributions
2. **Multi-Engine Sharding**: Hash orders by contestant ID across multiple engines
3. **Persistent Redis**: RDB snapshots for historical leaderboard
4. **TLS/mTLS**: For distributed deployments beyond localhost
5. **Trade Replay**: Re-score bots against historical market data
6. **Real Order Book**: Replace mock market ticks with actual live feed
7. **Chaos Engineering**: Fault injection framework (packet loss, latency spikes)
8. **API Rate Limiting**: Prevent submission spam
9. **Contestant API**: Allow bots to query live leaderboard positions

---

## 11. Conclusion

The HFT Market Simulator achieves low-latency evaluation through:
1. **Native C++ matching engine** with lock-free queues and CPU pinning
2. **Kafka durability** preventing trade loss
3. **Kubernetes sandboxing** with multi-layer security
4. **Async Python orchestration** for non-blocking management
5. **Real-time feedback** via Redis Pub/Sub and SSE

The system is production-ready for local development. A Terraform configuration for GCP is included under `terraform/`, but it has never been applied against a live project and is therefore unvalidated — cloud deployment is a designed path, not a demonstrated one. Key trade-offs prioritize latency and correctness over throughput ceilings and persistent storage.

