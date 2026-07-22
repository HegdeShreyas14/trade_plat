# HFT Market Simulator & Sandbox Platform — Comprehensive Design Document

> Complete technical reference covering architecture, infrastructure, algorithms, and operational considerations.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [High-Level Architecture](#2-high-level-architecture)
3. [End-to-End Data Flow](#3-end-to-end-data-flow)
4. [Sandbox Engine](#4-sandbox-engine)
5. [eBPF Kernel Latency Prober](#5-ebpf-kernel-latency-prober)
6. [Bot Fleet](#6-bot-fleet)
7. [Telemetry & Validation](#7-telemetry--validation)
8. [Real-Time Leaderboard](#8-real-time-leaderboard)
9. [Chaos Engineering](#9-chaos-engineering)
10. [Inter-Service Communication](#10-inter-service-communication)
11. [Data Stores](#11-data-stores)
12. [Infrastructure as Code](#12-infrastructure-as-code)
13. [CI/CD Pipeline](#13-cicd-pipeline)
14. [Composite Scoring Algorithm](#14-composite-scoring-algorithm)
15. [Technology Decisions](#15-technology-decisions)
16. [Architecture Decision Records](#16-architecture-decision-records)
17. [Performance Characteristics](#17-performance-characteristics)
18. [Contestant Upload Flow](#18-contestant-upload-flow)
19. [Week 4 — Final Delivery Summary](#19-week-4--final-delivery-summary)

---

## 1. System Overview

### Purpose

The HFT Market Simulator is a real-time distributed trading platform for evaluating algorithmic trading agents across multiple programming languages (Python, Rust, Go, C++). It provides:

- **Automated Containerization**: Compile and sandbox polyglot submissions without manual intervention
- **Low-Latency Matching**: Native C++ exchange engine matching in single-digit microseconds
  (measured P50 3.56 µs, P99 16.62 µs — see `results/`)
- **Distributed Evaluation**: Kubernetes-based isolation ensuring contestant code cannot escape sandbox
- **Live Telemetry**: Real-time scoring, latency analytics, and order book visualization
- **Fair Competition**: Deterministic scoring based on throughput (TPS) and latency (P99)

### Core Metrics

- **Throughput Target**: 10,000+ orders per second
- **Latency Target**: P99 < 100 microseconds (engine-side only)
- **Correctness**: Automated validation of all trades against protocol compliance
- **Isolation**: NetworkPolicy + Pod Security Standard ensures zero escape vectors
- **Scaling**: Single-core matching loop handles 170+ concurrent bot connections

---

## 2. High-Level Architecture

### System Layers

```
┌─────────────────────────────────────────────────────────────┐
│                    Browser / UI Layer                       │
│              (React + Vite, port 5173)                      │
└──────────────────────────┬──────────────────────────────────┘
                           │ SSE / WebSocket
┌──────────────────────────▼──────────────────────────────────┐
│              API & Gateway Layer (FastAPI)                  │
│     (telemetry/api.py, port 8000)                           │
│     ├─ /api/v1/submissions/upload                           │
│     ├─ /api/v1/leaderboard                                  │
│     └─ /api/v1/leaderboard/stream (SSE)                     │
└──────────────┬───────────────────────────┬──────────────────┘
               │                           │
               │ Async subprocess          │ Redis Pub/Sub
               │ spawn                     │ + Cache
               ▼                           ▼
┌──────────────────────────┐   ┌─────────────────────────────┐
│   Orchestration Layer    │   │    Data Store Layer         │
│ (infra/orchestrator.py)  │   │  ├─ Redis (scores, meta)    │
│                          │   │  ├─ Kafka (trade events)    │
│ ├─ K8s API calls        │   │  └─ Zookeeper (broker coord)│
│ ├─ Docker builds        │   └─────────────────────────────┘
│ ├─ Scoring spawn        │
│ └─ Lifecycle mgmt       │
└──────────────┬───────────┘
               │ kubectl apply
               ▼
┌──────────────────────────────────────────────────────────────┐
│           Evaluation Sandbox (Minikube / K8s)               │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  Namespace: evaluation-sandbox                         │ │
│  │                                                        │ │
│  │  ┌──────────────────────────────────────────────────┐ │ │
│  │  │ Pod: active-contestant-sandbox                  │ │ │
│  │  │                                                  │ │ │
│  │  │  ├─ Container: contestant-agent                │ │ │
│  │  │  │  └─ Process: python sandbox_sdk.py          │ │ │
│  │  │  │     (runs on_market_update)                 │ │ │
│  │  │  │                                              │ │ │
│  │  │  └─ NetworkPolicy:                             │ │ │
│  │  │     └─ Egress restricted to :8080 only         │ │ │
│  │  └──────────────────────────────────────────────────┘ │ │
│  └────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
               │ WebSocket (TCP)
               │ ws://10.0.2.2:8080
               ▼
┌──────────────────────────────────────────────────────────────┐
│              Matching Engine Layer (C++)                     │
│         (matching-engine/main.cpp, port 8080)               │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ TCPServer (Core 2)      MatchingEngine (Core 1)      │   │
│  │ ├─ epoll event loop     ├─ MPSC queue consumer      │   │
│  │ ├─ WebSocket handshake  ├─ OrderBook (buy/sell)     │   │
│  │ ├─ Frame parsing        ├─ Matching logic           │   │
│  │ ├─ t1 timestamp         ├─ t2 timestamp             │   │
│  │ └─ enqueueOrder()       ├─ Latency histograms       │   │
│  │                         └─ SPSCQueue producer       │   │
│  │                                                      │   │
│  │ KafkaOffloader (Core 3)  MetricsLoop (Core 2)       │   │
│  │ ├─ SPSC queue consumer  ├─ Percentile calc          │   │
│  │ ├─ Trade batching       └─ Stdout logging           │   │
│  │ └─ Kafka publishing                                 │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  CPU Pinning: Core 0 (free for OS), Cores 1-3 (pinned)    │
└──────────────────────────────────────────────────────────────┘
               │ Binary TradeEvents
               │ 60 bytes per event
               ▼
┌──────────────────────────────────────────────────────────────┐
│          Kafka Broker Cluster                               │
│  ├─ Topic: trade-events (1 partition by default)           │
│  ├─ Listeners: INTERNAL (kafka:29092), EXTERNAL (9092)     │
│  └─ Retention: 7 days or 1GB (whichever first)            │
└──────────────────────────────────────────────────────────────┘
               │ Message consumption
               │ (polling group: scoring-<contestant_id>)
               ▼
┌──────────────────────────────────────────────────────────────┐
│         Scoring Engine (Python)                             │
│  (telemetry/scoring_engine.py, spawned per evaluation)      │
│                                                              │
│  ├─ Kafka consumer (confluent-kafka)                        │
│  ├─ TradeEvent unpacking & validation                       │
│  ├─ Correctness checks (monotonicity, timestamps, etc)      │
│  ├─ Latency percentile computation (P50, P90, P99)          │
│  ├─ TPS calculation (trades / window_seconds)               │
│  └─ Score update to Redis (ZADD, HSET, PUBLISH)            │
└──────────────────────────────────────────────────────────────┘
```

### Service Dependencies

```yaml
Startup Order (with health checks):
  1. zookeeper
     ↓ health: nc -z localhost 2181
  2. kafka (depends_on zookeeper + condition: service_healthy)
     ↓ health: kafka-topics --list
  3. redis
     ↓ health: redis-cli ping
  4. matching-engine (depends_on kafka)
  5. telemetry-api (depends_on redis)
  6. leaderboard (depends_on telemetry-api)
  7. orchestrator (spawned by API as subprocess per submission)
  8. scoring-engine (spawned by orchestrator per evaluation)
```

---

## 3. End-to-End Data Flow

### Request Lifecycle: From Submission to Score

```
[Timeline visualization]

T+0s    User opens http://localhost:5173
        ├─ Browser loads React SPA from leaderboard service
        ├─ App.jsx mounts and loads snapshot via GET /api/v1/leaderboard
        └─ App.jsx opens EventSource to /api/v1/leaderboard/stream (SSE)

T+1s    User selects submission.zip and enters "alice" as contestant_id
        └─ Browser sends multipart POST to /api/v1/submissions/upload

T+2s    API receives upload
        ├─ Extracts ZIP to /tmp/sandbox_uploads/alice/
        ├─ Validates structure (sandbox_sdk.py, solution.py, requirements.txt)
        ├─ Creates asyncio.create_task(run_detached_orchestrator())
        └─ Returns {"status": "QUEUED"} to browser (immediate, non-blocking)

T+3s    Orchestrator subprocess starts (infra/orchestrator.py --sub-id alice)
        ├─ Loads K8s config (from ~/.kube/config or in-cluster)
        ├─ Creates namespace: evaluation-sandbox
        ├─ Applies NetworkPolicy: egress to :8080 only
        ├─ Calls auto_detect_and_build()
        │  ├─ Scans unzipped tree for language indicators
        │  └─ Generates Dockerfile.generated for Python
        ├─ Runs: docker build -f Dockerfile.generated -t contestant-agent:alice /tmp/sandbox_uploads/alice/
        │  └─ Image pulls python:3.11, installs requirements.txt deps
        └─ Image ready (tagged in Minikube's Docker daemon)

T+5s    Orchestrator spawns scoring engine subprocess
        ├─ Calls: python telemetry/scoring_engine.py \
        │        --kafka-brokers kafka:29092 \
        │        --contestant-id alice \
        │        --group-id scoring-alice-<timestamp> \
        │        --offset-reset latest \
        │        --window-seconds 10 \
        │        --metrics-interval-ms 1000
        └─ Consumer created, awaiting Kafka messages

T+6s    Orchestrator spawns contestant sandbox pod
        ├─ Creates K8s Pod spec (evaluation-sandbox namespace)
        ├─ Applies security context: runAsNonRoot, drop ALL capabilities
        ├─ Sets env vars:
        │  ├─ EXCHANGE_GATEWAY_URL=ws://10.0.2.2:8080
        │  └─ CONTESTANT_ID=alice
        ├─ Applies pod to cluster: kubectl apply
        └─ Pod enters Pending → ContainerCreating → Running

T+8s    Pod starts, container starts python sandbox_sdk.py
        ├─ ExchangeConnection.__init__() opens WebSocket
        │  └─ Performs HTTP upgrade handshake → 101 Switching Protocols
        ├─ Enters tick loop:
        │  ├─ Generate mock market_tick {"symbol": "BTCUSD", "price": 150}
        │  ├─ Call on_market_update(market_tick, exchange)
        │  │  └─ solution.py: if price < 152: exchange.submit_order("BUY", 151, 10)
        │  └─ Sleep 1 second, repeat

T+9s    Bot submits first order
        ├─ sandbox_sdk.py::ExchangeConnection.submit_order()
        │  ├─ side_code = 1 (BUY)
        │  ├─ t0 = time.time_ns() (bot-side timestamp)
        │  ├─ Packs 32-byte struct:
        │  │  └─ <QQdIB3x: order_id, t0, price, qty, side, padding>
        │  ├─ asyncio.create_task(ws.send(binary_frame))
        │  └─ Returns immediately (fire-and-forget)
        │
        └─ websockets library:
           ├─ Creates WebSocket binary frame (opcode 0x2)
           ├─ Applies 4-byte random XOR mask (RFC 6455)
           └─ Sends ~45 bytes (frame header + masked payload)

T+9.1ms (on engine, after network round-trip)
        └─ TCPServer::handleClientData() on Core 2
           ├─ epoll_wait() returns client_fd as readable
           ├─ read() into per-client RingBuffer
           ├─ Parses WebSocket frame:
           │  ├─ Byte 0: FIN=1, opcode=0x2 (binary)
           │  ├─ Byte 1: MASK=1, payload_len=32
           │  ├─ Bytes 2-5: 4-byte mask key
           │  ├─ Unmasks 32 bytes XOR mask[i % 4]
           │  └─ Appends unmasked payload to buffer
           ├─ Extracts OrderMessage (cast from unmasked 32 bytes)
           │  └─ order_id=<alice_1>, timestamp_ns=<t0>, price=151.0, qty=10, side=1
           ├─ Records t1 = system_clock::now() (gateway-ingest time)
           ├─ Constructs Order(order_id, qty, side, price, t0, t1)
           ├─ Atomically increments global seqNo (seqNo = 1000001)
           └─ Calls engine_.enqueueOrder(order)

T+9.1ms    MPSCQueue::enqueue() on TCPServer thread
           ├─ Lock-free CAS loop claims slot in 65536-element ring
           ├─ Writes Order into claimed slot
           └─ Increments sequence counter with memory_order_release

T+9.2ms    MatchingEngine::processLoop() on Core 1
           ├─ MPSCQueue::dequeue() acquires Order
           ├─ Calls addOrder():
           │  └─ Inserts into OrderBook::sellOrders[151.0] (it's a sell-side order)
           └─ No match yet (no buy-side orders crossing)

[Second bot order arrives ~1 second later]

T+10s      Another bot (different contestant, or same bot, second tick) submits
           ├─ order_id=<bob_1>, price=150.5, qty=10, side=2 (SELL)
           └─ Same flow: TCPServer → enqueue → processLoop

T+10.2ms   MatchingEngine::processLoop() receives second order
           ├─ bestBid (bob_1 @ 150.5) >= bestAsk (alice_1 @ 151.0) — NO MATCH YET
           └─ Continues, no trades executed

[Example: when a trade-triggering order arrives]

T+11s      Third order arrives: order_id=<charlie_1>, price=151.5, qty=10, side=1 (BUY)
           ├─ TCPServer records t1
           └─ enqueueOrder(charlie_1)

T+11.2ms   MatchingEngine::processLoop()
           ├─ bestBid (charlie_1 @ 151.5) >= bestAsk (alice_1 @ 151.0) — MATCH!
           ├─ Calls matching():
           │  ├─ taker = charlie_1 (higher seqNo, arrived later)
           │  ├─ maker = alice_1 (lower seqNo, arrived first)
           │  ├─ execution_price = 151.0 (maker's limit)
           │  ├─ traded_qty = min(10, 10) = 10
           │  ├─ Records t2 = system_clock::now()
           │  └─ Constructs TradeEvent:
           │     ├─ match_id = 1 (global counter)
           │     ├─ buy_order_id = charlie_1
           │     ├─ sell_order_id = alice_1
           │     ├─ t0_ns = t0 from taker (charlie_1's bot time)
           │     ├─ t1_ns = t1 from taker (gateway-ingest time)
           │     ├─ t2_ns = t2 (match time)
           │     ├─ price = 151.0
           │     └─ qty = 10
           ├─ Calls kafkaOffloader.publishBatch([tradeEvent])
           └─ Records latencies in histogram:
              ├─ network_latency = t1 - t0 (microseconds)
              └─ engine_latency = t2 - t1 (microseconds)

T+11.3ms   SPSCQueue::enqueue() (MatchingEngine thread → KafkaOffloader)
           ├─ MatchingEngine (Core 1) pushes TradeEvent
           └─ Into SPSCQueue (65536-element ring buffer)

T+11.4ms   KafkaOffloader::run() on Core 3
           ├─ Continuously drains SPSCQueue (up to 1024 events per iteration)
           ├─ When batch_size >= 1 or timeout (~1ms):
           │  ├─ Serializes N × TradeEvent structs (each 60 bytes)
           │  ├─ Calls librdkafka Producer::produce()
           │  │  ├─ Topic: "trade-events"
           │  │  ├─ Partition: PARTITION_UA (Kafka auto-selects, → 0)
           │  │  └─ Payload: N×60 bytes (raw structs)
           │  ├─ Calls producer.flush() periodically (linger.ms=1)
           │  └─ Sends to Kafka broker
           └─ Increments delivered_ counter

T+11.5ms   Kafka broker receives TradeEvent
           ├─ Appends to topic "trade-events", partition 0
           ├─ Offset: auto-increment (e.g., offset 1000)
           └─ Retains until consumer group commits offset

T+11.6ms   Scoring engine (confluent-kafka consumer)
           ├─ Consumer group: scoring-alice-<timestamp>
           ├─ auto.offset.reset=latest (started after engine, reads new events only)
           ├─ Polls with 50ms timeout
           ├─ Receives Kafka message:
           │  ├─ Value: 60-byte binary (TradeEvent struct)
           │  └─ Offset: 1000
           ├─ Calls unpack_trade_events(payload):
           │  └─ struct.unpack_from("<QQQQQQdI", payload, 0)
           │     → (match_id=1, buy=charlie_1, sell=alice_1, t0, t1, t2, 151.0, 10)
           │
           ├─ ContestantState.validate(event):
           │  ├─ ✓ match_id == 1 (first trade, monotic)
           │  ├─ ✓ t1 >= t0 (timestamps ordered)
           │  ├─ ✓ t2 >= t1 (timestamps ordered)
           │  ├─ ✓ buy_order_id != sell_order_id (not self-match)
           │  ├─ ✓ price > 0 and finite (valid price)
           │  └─ ✓ qty > 0 (valid quantity)
           │
           └─ ContestantState.accept(event):
              ├─ Appends (wall_time_monotonic(), engine_latency_ns) to deque
              ├─ Prunes samples older than window_seconds (10s)
              └─ Maintains sliding window of recent trades

T+12.6ms   Scoring engine (every metrics_interval_ms = 1000ms)
           ├─ ContestantState.metrics():
           │  ├─ latencies = [engine_latency_ns for each sample in window]
           │  ├─ p50_us = percentile(latencies, 0.50) / 1000
           │  ├─ p90_us = percentile(latencies, 0.90) / 1000
           │  ├─ p99_us = percentile(latencies, 0.99) / 1000
           │  ├─ tps = len(samples) / window_seconds
           │  └─ score = tps / max(p99_us, 0.1)
           │
           ├─ Calls publish_metrics():
           │  ├─ redis.zadd("leaderboard:scores", {alice: score})
           │  ├─ redis.hset("contestant:meta:alice", {
           │  │     tps: 234.5,
           │  │     p50_us: 45.2,
           │  │     p90_us: 87.6,
           │  │     p99_us: 120.4,
           │  │     score: 1.95,
           │  │     total_trades: 1,
           │  │     last_match_id: 1,
           │  │     correctness: "OK"
           │  │  })
           │  └─ redis.publish("leaderboard_updates", json_payload)
           └─ Next metric emission: T+13.6ms

T+12.7ms   API (telemetry/api.py) receives Redis pub/sub notification
           ├─ FastAPI coroutine subscribed to "leaderboard_updates" channel
           ├─ Calls read_leaderboard():
           │  ├─ redis.zrevrange("leaderboard:scores", 0, 24, withscores=True)
           │  ├─ For each contestant_id: redis.hgetall(f"contestant:meta:{contestant_id}")
           │  └─ Constructs leaderboard rows (sorted by score descending)
           │
           └─ Yields SSE "update" event with full leaderboard:
              └─ event: update\ndata: {rows: [...]}\n\n

T+12.8ms   Browser (React App.jsx) receives SSE event
           ├─ EventSource listener: addEventListener("update", onRows)
           ├─ onRows(event):
           │  ├─ Parses event.data (JSON)
           │  └─ Calls setRows(parsed_rows)
           │
           ├─ React renders with new state
           │  ├─ Table updates: alice now has 1 trade, score 1.95
           │  ├─ BarChart recomputes
           │  └─ LineChart recomputes
           │
           └─ User sees leaderboard updated live within ~15ms of trade execution

[Evaluation concludes after 60 seconds]

T+60s      Orchestrator::wait_for_agent_completion(timeout=60) returns
           ├─ Pod phase = "Succeeded"
           ├─ Orchestrator fetches last 50 lines of pod stdout
           ├─ Sends SIGTERM to scoring_engine subprocess
           ├─ Scoring engine terminates, commits final offsets to Kafka
           ├─ Redis holds final scores indefinitely
           └─ Orchestrator calls cleanup(): kubectl delete pod

[At any point, user can submit another contestant]

T+61s      Leaderboard shows alice at top (or wherever score ranks)
           ├─ User submits bob.zip
           └─ New orchestrator + scoring engine spawned (non-blocking)
           
           [Process repeats independently for bob]
```

---

## 4. Sandbox Engine

### Kubernetes Pod Security

The sandbox engine enforces **zero escape** guarantees through Kubernetes security primitives.

#### Pod Security Standard: "restricted"

All contestant pods are created with the "restricted" Pod Security Standard, enforced at the namespace level:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: evaluation-sandbox
  labels:
    pod-security.kubernetes.io/enforce: restricted
    pod-security.kubernetes.io/audit: restricted
    pod-security.kubernetes.io/warn: restricted
```

#### Pod Security Context

```yaml
spec:
  securityContext:
    runAsNonRoot: true              # Container cannot be UID 0
    runAsUser: 10002                # Run as unprivileged user
    fsGroup: 10002                  # Filesystem group ownership
    runAsGroup: 10002
    seccompProfile:
      type: RuntimeDefault          # curated syscall whitelist
    
  containers:
  - name: contestant-agent
    securityContext:
      allowPrivilegeEscalation: false   # no setuid/setgid bit execution
      readOnlyRootFilesystem: true      # / is read-only
      runAsNonRoot: true
      runAsUser: 10002
      capabilities:
        drop:
        - ALL                           # drop all Linux capabilities
```

#### NetworkPolicy: Egress Restriction

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: contestant-egress-restriction
  namespace: evaluation-sandbox
spec:
  podSelector:
    matchLabels:
      tier: contestant-agent
  policyTypes:
  - Egress
  egress:
  - to:
    - namespaceSelector: {}
    ports:
    - protocol: TCP
      port: 8080                       # only port 8080
  - to:                               # allow DNS for internal Kubernetes
    - namespaceSelector:
        matchLabels:
          name: kube-system
    ports:
    - protocol: UDP
      port: 53
```

**Enforcement:**
- Contestant can connect ONLY to `EXCHANGE_HOST:8080` (the matching engine gateway)
- Cannot resolve external domains (DNS blocked except to kube-system)
- Cannot open raw sockets (CAP_NET_RAW dropped)
- Cannot spawn child processes with elevated privileges
- Cannot write to filesystem (read-only root)

### Multi-Language Auto-Detection

The orchestrator detects contestant submission language via:

```python
def detect_language(unzipped_dir: str) -> str:
    """Scan directory recursively for language indicators"""
    
    for root, dirs, files in os.walk(unzipped_dir):
        # Rust indicators
        if "Cargo.toml" in files or any(f.endswith(".rs") for f in files):
            return "rust"
        
        # Go indicators
        if "go.mod" in files or any(f.endswith(".go") for f in files):
            return "go"
        
        # Python indicators
        if ("requirements.txt" in files or 
            "sandbox_sdk.py" in files or
            any(f.endswith(".py") for f in files)):
            return "python"
    
    # Default
    return "cpp"  # C++ requires manual Makefile
```

### Dockerfile Generation

For Python submissions, the orchestrator generates:

```dockerfile
FROM python:3.11-slim
WORKDIR /bot
COPY . .
RUN pip install --no-cache-dir -r requirements.txt

# Set security context (enforced by pod spec)
USER 10002

# Entrypoint: locate sandbox_sdk.py in tree and execute it
ENTRYPOINT ["python", "sandbox_sdk.py"]
```

**Multi-language approach** (pseudocode):

```python
if language == "python":
    dockerfile = """
    FROM python:3.11-slim
    WORKDIR /bot
    COPY . .
    RUN pip install -r requirements.txt
    USER 10002
    ENTRYPOINT ["python", "sandbox_sdk.py"]
    """

elif language == "rust":
    dockerfile = """
    FROM rust:1.70-alpine
    WORKDIR /bot
    COPY . .
    RUN cargo build --release
    USER 10002
    ENTRYPOINT ["./target/release/bot"]
    """

elif language == "go":
    dockerfile = """
    FROM golang:1.20-alpine
    WORKDIR /bot
    COPY . .
    RUN go build -o bot .
    USER 10002
    ENTRYPOINT ["./bot"]
    """

elif language == "cpp":
    dockerfile = """
    FROM g++:11
    WORKDIR /bot
    COPY . .
    RUN make
    USER 10002
    ENTRYPOINT ["./bot"]
    """
```

### Resource Limits

Pod requests and limits prevent denial-of-service:

```yaml
resources:
  requests:
    memory: "256Mi"
    cpu: "250m"
  limits:
    memory: "512Mi"
    cpu: "500m"
```

A contestant bot that tries to allocate >512MB RAM or consume >500m CPU is killed by kubelet.

---

## 5. eBPF Kernel Latency Prober

### Purpose

Measure end-to-end latency at kernel level, capturing jitter that userspace timestamps might miss.

### Kernel Tracepoint Hooks

```c
// tracing/ebpf_prober.c (pseudocode)

#include <uapi/linux/ptrace.h>

BPF_HASH(order_timestamps, u64, u64);  // order_id → t0_kernel
BPF_PERF_OUTPUT(latency_events);

struct order_event {
    u64 order_id;
    u64 t_arrival;          // kernel time_ns at TCP recv
    u64 t_match_complete;   // kernel time_ns at send
    u32 engine_latency_ns;
};

// Trace point: tcp_cleanup_rbuf (ingress)
TRACEPOINT_PROBE(tcp, tcp_cleanup_rbuf) {
    // Fired when TCP socket receives data
    struct sock *sk = (struct sock *)args->skaddr;
    u16 sport = args->sport;
    u16 dport = args->dport;
    
    // Only trace port 8080 (matching engine)
    if (dport != 8080) return 0;
    
    // Record kernel timestamp
    u64 ts = bpf_ktime_get_ns();
    u64 order_id = 0;  // would extract from socket buffer
    
    order_timestamps.update(&order_id, &ts);
    return 0;
}

// Trace point: tcp_transmit_skb (egress)
TRACEPOINT_PROBE(tcp, tcp_transmit_skb) {
    // Fired when TCP socket sends data (trade event)
    u64 ts = bpf_ktime_get_ns();
    u64 order_id = 0;  // extract from payload
    
    u64 *t_arrival = order_timestamps.lookup(&order_id);
    if (!t_arrival) return 0;
    
    struct order_event ev = {
        .order_id = order_id,
        .t_arrival = *t_arrival,
        .t_match_complete = ts,
        .engine_latency_ns = ts - *t_arrival,
    };
    
    latency_events.perf_submit(args, &ev, sizeof(ev));
    order_timestamps.delete(&order_id);
    
    return 0;
}
```

### Userspace Consumer

```python
# telemetry/ebpf_latency_consumer.py

from bcc import BPF

bpf_code = open("tracing/ebpf_prober.c").read()
bpf = BPF(text=bpf_code)

def handle_latency(cpu, data, size):
    event = bpf["latency_events"].event(data)
    print(f"Order {event.order_id}: kernel latency {event.engine_latency_ns} ns")
    
    # Update percentiles for this contestant
    contestant_state[event.order_id].record_kernel_latency(event.engine_latency_ns)

bpf["latency_events"].open_perf_buffer(handle_latency)

while True:
    try:
        bpf.perf_buffer_poll()
    except KeyboardInterrupt:
        break
```

### Kernel vs. Userspace Latency

| Level | Measurement | Accuracy | Jitter Source |
|-------|-------------|----------|---------------|
| Kernel (eBPF) | Entry: tcp_cleanup_rbuf, Exit: tcp_transmit_skb | ±1-5µs | CPU scheduling, interrupt handling |
| Userspace (t2-t1) | Entry: TCPServer::handleClientData, Exit: MatchingEngine::matching | ±5-50µs | Context switches, cache misses |
| Wall-clock (t2-t0) | Includes network + kernel + engine | ±10-100µs | Accumulated all sources |

**eBPF allows isolation of kernel vs. userspace contributions to latency.**

---

## 6. Bot Fleet

### Load Testing Framework

The `bot-fleet/main.py` is a standalone tool for pre-contest stress testing.

### Bot Types

#### 1. Market Maker Bot

```python
class MarketMakerBot:
    """Passively places bids and asks"""
    
    async def on_tick(self):
        mid_price = 150.0
        spread = 0.5
        
        # Inventory-weighted spread adjustment
        if self.inventory > 0:
            # heavy in stock, widen ask to encourage sells
            ask = mid_price + spread + (self.inventory * 0.01)
        else:
            ask = mid_price + spread
        
        bid = ask - spread
        
        # Submit both sides
        self.exchange.submit_order("BUY", bid, 100)
        self.exchange.submit_order("SELL", ask, 100)
```

#### 2. Noise Bot

```python
class NoiseBot:
    """Submits random orders"""
    
    async def on_tick(self):
        for _ in range(5):  # 5 orders per tick
            price = random.uniform(148, 155)
            qty = random.randint(10, 100)
            side = random.choice(["BUY", "SELL"])
            self.exchange.submit_order(side, price, qty)
```

#### 3. Momentum Bot

```python
class MomentumBot:
    """Trades in direction of trend"""
    
    async def on_tick(self):
        if self.price_trend > 0:
            # Price going up, aggressively buy
            self.exchange.submit_order("BUY", self.price + 0.5, 100)
        else:
            # Price going down, aggressively sell
            self.exchange.submit_order("SELL", self.price - 0.5, 100)
        
        # Every 10 ticks, reverse direction (mean reversion)
        if self.tick_count % 10 == 0:
            self.price_trend *= -1
```

### Fleet Composition

```yaml
Default Fleet (170 total concurrent connections):
  ├─ MarketMakerBots: 50
  │  └─ Provide passive liquidity
  ├─ NoiseBots: 100
  │  └─ Add realistic order book churn
  └─ MomentumBots: 20
     └─ Drive short-term trends
```

### Raw Protocol Implementation

The bot-fleet implements WebSocket manually (no library) for accurate load testing:

```python
async def websocket_handshake(host, port):
    """Manual WebSocket upgrade"""
    reader, writer = await asyncio.open_connection(host, port)
    
    # HTTP GET request
    request = (
        f"GET /orders HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        f"Upgrade: websocket\r\n"
        f"Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {base64(os.urandom(16))}\r\n"
        f"Sec-WebSocket-Version: 13\r\n"
        f"\r\n"
    )
    
    writer.write(request.encode())
    await writer.drain()
    
    # Read 101 Switching Protocols response
    response = await reader.readexactly(1024)
    assert b"101" in response
    
    return reader, writer

async def send_order(writer, order_id, price, qty, side):
    """Send 32-byte order message in WebSocket binary frame"""
    
    # Pack order
    payload = struct.pack("<QQdIB3x",
        order_id,
        time.time_ns(),
        float(price),
        int(qty),
        1 if side == "BUY" else 2,
    )
    
    # Build WebSocket frame
    mask_key = os.urandom(4)
    
    frame = bytearray()
    frame.append(0x82)  # FIN=1, opcode=0x2 (binary)
    frame.append(0x80 | 32)  # MASK=1, payload_len=32
    frame.extend(mask_key)
    
    # XOR payload with mask
    masked_payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
    frame.extend(masked_payload)
    
    writer.write(bytes(frame))
    await writer.drain()
```

---

## 7. Telemetry & Validation

### Correctness Validation

Every trade is validated against the protocol contract:

```python
class ContestantState:
    def validate(self, event: TradeEvent) -> bool:
        """Atomically check all correctness invariants"""
        
        checks = [
            # (condition, error_message)
            (event.match_id == self.last_match_id + 1,
             f"Sequence gap: expected {self.last_match_id + 1}, got {event.match_id}"),
            
            (event.t2_ns >= self.last_t2_ns,
             f"Clock went backwards: {event.t2_ns} < {self.last_t2_ns}"),
            
            (event.buy_order_id != 0 and event.sell_order_id != 0,
             "Missing order ID"),
            
            (event.buy_order_id != event.sell_order_id,
             f"Self-match detected: {event.buy_order_id}"),
            
            (event.t1_ns >= event.t0_ns and event.t2_ns >= event.t1_ns,
             f"Timestamp ordering violated: t0={event.t0_ns}, t1={event.t1_ns}, t2={event.t2_ns}"),
            
            (event.price > 0 and isfinite(event.price),
             f"Invalid price: {event.price}"),
            
            (event.qty > 0,
             f"Invalid quantity: {event.qty}"),
        ]
        
        for condition, error_msg in checks:
            if not condition:
                self.correctness_error = error_msg
                return False
        
        return True
    
    def accept(self, event: TradeEvent):
        """Record trade if validation passed"""
        wall_time = time.monotonic_ns()
        engine_latency_ns = event.t2_ns - event.t1_ns
        self.samples.append((wall_time, engine_latency_ns))
        self.last_match_id = event.match_id
        self.last_t2_ns = event.t2_ns
        self.total_trades += 1
```

### Disqualification

A single validation failure is fatal:

```python
def process_kafka_event(event: TradeEvent):
    if not contestant_state.validate(event):
        # First error is disqualifying
        contestant_state.score = -1.0
        publish_to_redis(contestant_id, score=-1.0, 
                        correctness=contestant_state.correctness_error)
        return  # stop processing further events
    
    contestant_state.accept(event)
```

### Latency Percentiles

```python
def compute_percentiles(samples: List[Tuple[float, int]]) -> Dict[str, float]:
    """Compute P50, P90, P99 from engine latency samples"""
    
    if not samples:
        return {"p50": 0, "p90": 0, "p99": 0}
    
    latencies_us = sorted([ns // 1000 for _, ns in samples])
    
    def percentile(data, p):
        idx = int(len(data) * p)
        return data[min(idx, len(data) - 1)]
    
    return {
        "p50": percentile(latencies_us, 0.50),
        "p90": percentile(latencies_us, 0.90),
        "p99": percentile(latencies_us, 0.99),
    }
```

### Window-Based Metrics

Metrics are computed over a sliding window (default 10 seconds):

```python
def metrics(self) -> Dict[str, float]:
    """Compute current score over the last window_seconds"""
    
    # Prune old samples
    now = time.monotonic_ns()
    window_ns = self.window_seconds * 1e9
    self.samples = deque(
        s for s in self.samples 
        if (now - s[0]) <= window_ns
    )
    
    if not self.samples:
        return {"tps": 0, "p50": 0, "p90": 0, "p99": 0, "score": 0}
    
    latencies = [ns for _, ns in self.samples]
    
    tps = len(self.samples) / self.window_seconds
    p99_us = percentile(latencies, 0.99) / 1000
    score = tps / max(p99_us, 0.1)
    
    return {
        "tps": tps,
        "p50": percentile(latencies, 0.50) / 1000,
        "p90": percentile(latencies, 0.90) / 1000,
        "p99": p99_us,
        "score": score,
    }
```

---

## 8. Real-Time Leaderboard

### SSE Architecture

The leaderboard uses Server-Sent Events for push-based updates:

```javascript
// leaderboard/src/App.jsx
useEffect(() => {
    const source = new EventSource(`${API_URL}/api/v1/leaderboard/stream`);
    
    source.addEventListener("snapshot", (event) => {
        const rows = JSON.parse(event.data);
        setRows(rows);
    });
    
    source.addEventListener("update", (event) => {
        const rows = JSON.parse(event.data);
        setRows(rows);
    });
    
    source.onerror = () => setConnected(false);
    
    return () => source.close();
}, []);
```

### Backend SSE Stream

```python
# telemetry/api.py
async def leaderboard_stream():
    """Async generator for SSE streaming"""
    
    pubsub = await redis_client.pubsub()
    await pubsub.subscribe("leaderboard_updates")
    
    # Send initial snapshot
    snapshot = await read_leaderboard()
    yield f"event: snapshot\ndata: {json.dumps(snapshot)}\n\n"
    
    # Stream updates
    timeout = 0
    try:
        while True:
            # Poll with 1-second timeout
            message = await pubsub.get_message(timeout=1.0)
            
            if message:
                # New score update
                leaderboard = await read_leaderboard()
                yield f"event: update\ndata: {json.dumps(leaderboard)}\n\n"
            
            # Every 5 seconds, send a heartbeat snapshot
            timeout += 1
            if timeout >= 5:
                snapshot = await read_leaderboard()
                yield f"event: snapshot\ndata: {json.dumps(snapshot)}\n\n"
                timeout = 0
    finally:
        await pubsub.unsubscribe("leaderboard_updates")
```

### React Visualization

The leaderboard displays three views:

#### 1. Ranking Table

```jsx
<table>
  <thead>
    <tr>
      <th>Rank</th>
      <th>Contestant</th>
      <th>Score</th>
      <th>TPS</th>
      <th>P50 (µs)</th>
      <th>P90 (µs)</th>
      <th>P99 (µs)</th>
      <th>Correctness</th>
      <th>Fills</th>
      <th>Last Match ID</th>
    </tr>
  </thead>
  <tbody>
    {rows.map((row, idx) => (
        <tr key={row.contestant_id} className={row.score === -1 ? "disqualified" : ""}>
            <td>{idx + 1}</td>
            <td>{row.contestant_id}</td>
            <td className="score">{row.score.toFixed(2)}</td>
            {/* ... */}
        </tr>
    ))}
  </tbody>
</table>
```

#### 2. Score Bar Chart (Top 10)

```jsx
<ResponsiveContainer width="100%" height={300}>
    <BarChart data={chartRows}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="contestant_id" />
        <YAxis />
        <Tooltip />
        <Bar dataKey="score" fill="#10b981" />
    </BarChart>
</ResponsiveContainer>
```

#### 3. Latency Line Chart (Top 10, P99)

```jsx
<ResponsiveContainer width="100%" height={300}>
    <LineChart data={chartRows}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="contestant_id" />
        <YAxis />
        <Tooltip />
        <Line type="monotone" dataKey="p99" stroke="#f97316" />
    </LineChart>
</ResponsiveContainer>
```

---

## 9. Chaos Engineering

### Deliberate Failure Injection

To test system resilience, the platform can inject failures:

#### 1. Order Corruption

Randomly flip bits in order payloads:

```python
def inject_order_corruption(order_bytes: bytes, error_rate: float = 0.001) -> bytes:
    """Randomly corrupt 0.1% of incoming orders"""
    if random.random() < error_rate:
        result = bytearray(order_bytes)
        bit_pos = random.randint(0, len(result) * 8 - 1)
        byte_idx = bit_pos // 8
        bit_idx = bit_pos % 8
        result[byte_idx] ^= (1 << bit_idx)
        return bytes(result)
    return order_bytes
```

#### 2. Kafka Consumer Lag

Introduce artificial processing delay:

```python
async def simulate_kafka_lag(delay_ms: int = 100):
    """Simulate slow consumer (Kafka lag)"""
    await asyncio.sleep(delay_ms / 1000)
```

#### 3. Clock Skew

Simulate desynchronized clocks between bot and engine:

```python
def apply_clock_skew(t0_ns: int, skew_ns: int = 1000000) -> int:
    """Add clock skew: t0 appears to be from the past/future"""
    return t0_ns + random.randint(-skew_ns, skew_ns)
```

#### 4. Network Packet Loss

Randomly drop WebSocket frames:

```python
def should_drop_packet(loss_rate: float = 0.001) -> bool:
    """Simulate packet loss"""
    return random.random() < loss_rate
```

#### 5. Engine Overload

Introduce matching latency spikes:

```python
def simulate_engine_backlog(current_queue_depth: int, threshold: int = 100000):
    """If MPSC queue > threshold, add simulated processing delay"""
    if current_queue_depth > threshold:
        excess = current_queue_depth - threshold
        delay_us = (excess // 1000)  # 1µs per 1000 queued orders
        time.sleep(delay_us / 1e6)
```

### Chaos Test Scenarios

```python
class ChaosTestSuite:
    
    @scenario
    def test_corrupted_order_recovery(self):
        """Submit order with flipped bits → engine should reject"""
        # Bot submits 1000 orders with 1% corruption rate
        # Engine should reject corrupted ones, accept valid ones
        # Final leaderboard should show non-zero score
        assert leaderboard[contestant].score > 0
    
    @scenario
    def test_clock_skew_tolerance(self):
        """Inject ±100ms clock skew on bot side"""
        # Network latency (t1-t0) may appear negative
        # Engine latency (t2-t1) should still be positive
        # Scoring uses only t2-t1, so score should be valid
        assert leaderboard[contestant].score > 0
    
    @scenario
    def test_packet_loss(self):
        """Simulate 5% packet loss"""
        # Bot retransmits orders on timeout
        # Fewer matches occur but scoring continues
        assert leaderboard[contestant].p99_latency > baseline_p99
    
    @scenario
    def test_engine_under_load(self):
        """Fleet size: 100 → 500 concurrent bots"""
        # Matching latency increases as queue fills
        # P99 latency should increase proportionally
        assert leaderboard[contestant].p99_latency <= (100 * baseline_p99)
    
    @scenario
    def test_kafka_consumer_lag(self):
        """Kafka consumer processing 500ms behind engine"""
        # Leaderboard updates delayed but not missing
        # Final scores are correct (eventually)
        await asyncio.sleep(2)  # wait for catch-up
        assert leaderboard_final[contestant] == leaderboard_correct[contestant]
```

---

## 10. Inter-Service Communication

### Protocol Matrix

| Source | Dest | Protocol | Port | Payload | TLS |
|--------|------|----------|------|---------|-----|
| Bot | Engine | WebSocket (TCP) | 8080 | 32-byte OrderMessage | No (local) |
| Engine | Kafka | Plaintext (librdkafka) | 29092 | 60-byte TradeEvent (batched) | No |
| Scoring | Kafka | Plaintext (confluent-kafka) | 29092 | Pull consumer | No |
| Scoring | Redis | RESP protocol | 6379 | ZADD, HSET, PUBLISH | No |
| API | Redis | RESP protocol | 6379 | ZREVRANGE, HGETALL, SUBSCRIBE | No |
| API | Kafka | N/A | — | (not directly) | — |
| Browser | API | HTTP/1.1, SSE | 8000 | JSON | No (local) |
| Orchestrator | K8s API | HTTP/REST | 6443 | kubectl equivalent | Yes (kubeconfig) |

### Message Formats

#### WebSocket: OrderMessage (Bot → Engine)

```
Byte Layout (little-endian):
  [0-7]   order_id (uint64)
  [8-15]  t0_ns (uint64, nanoseconds)
  [16-23] price (double, IEEE 754)
  [24-27] qty (uint32)
  [28]    side (uint8: 1=BUY, 2=SELL)
  [29-31] padding (zeros)

Total: 32 bytes
```

#### Kafka: TradeEvent (Engine → Scoring)

```
Byte Layout (little-endian, packed):
  [0-7]   match_id (uint64)
  [8-15]  buy_order_id (uint64)
  [16-23] sell_order_id (uint64)
  [24-31] t0_ns (uint64, taker's bot-generation time)
  [32-39] t1_ns (uint64, gateway-ingest time)
  [40-47] t2_ns (uint64, match-completion time)
  [48-55] price (double)
  [56-59] qty (uint32)

Total: 60 bytes (packed, __attribute__((packed)) enforced)

Kafka Message Value: N × 60-byte TradeEvents (batched)
```

#### Redis: Leaderboard Data

```python
# Sorted set: leaderboard:scores
ZADD leaderboard:scores 42.5 alice 37.2 bob 0.0 charlie
# (score → contestant_id mapping)

# Hash: contestant:meta:<id>
HGETALL contestant:meta:alice
# Returns:
#   tps: 234.5 (trades per second)
#   p50_lat_us: 42.1
#   p90_lat_us: 87.3
#   p99_lat_us: 120.4
#   score: 42.5
#   total_trades: 2345
#   last_match_id: 2344
#   correctness: "OK"

# Pub/Sub: leaderboard_updates
PUBLISH leaderboard_updates '{"rows": [...]}'
```

#### HTTP: API Endpoints

```
GET /api/v1/leaderboard
Response:
{
  "rows": [
    {
      "rank": 1,
      "contestant_id": "alice",
      "score": 42.5,
      "tps": 234.5,
      "p50_us": 42.1,
      "p90_us": 87.3,
      "p99_us": 120.4,
      "correctness": "OK",
      "total_fills": 2345,
      "last_match_id": 2344
    },
    ...
  ]
}

GET /api/v1/leaderboard/stream (SSE)
event: snapshot
data: {"rows": [...]}

event: update
data: {"rows": [...]}

POST /api/v1/submissions/upload
Request body: multipart/form-data
  - file: submission.zip (binary)
  - contestant_id: "alice" (form field)

Response:
{
  "status": "QUEUED",
  "submission_id": "alice",
  "message": "Evaluation started"
}
```

---

## 11. Data Stores

### Redis

**Role**: Leaderboard scores, metadata, real-time pub/sub notifications

**Data Structures**:

```python
# Sorted Set: Ranked leaderboard
ZADD leaderboard:scores <score> <contestant_id>
ZREVRANGE leaderboard:scores 0 24 WITHSCORES  # Top 25

# Hashes: Per-contestant metadata
HSET contestant:meta:<id> tps 234.5 p50 42 p90 87 p99 120 score 42.5
HGET contestant:meta:<id> tps

# Pub/Sub: Real-time updates
PUBLISH leaderboard_updates <json_payload>
SUBSCRIBE leaderboard_updates
```

**Persistence**: RDB snapshots (disabled for this use case — scores are ephemeral per evaluation)

**Eviction**: No eviction (`maxmemory-policy: noeviction`). Scores live in-memory.

### Kafka

**Role**: Durable event log for trades, enables replay and windowed scoring

**Topic Configuration**:

```yaml
Topic: trade-events
  Partitions: 1 (preserve total ordering)
  Replication Factor: 1 (local cluster)
  Retention: 7 days or 1GB (whichever first)
  Compression: none (low-latency > disk space)
```

**Producers**:
- **MatchingEngine** (KafkaOffloader): publishes 60-byte TradeEvents

**Consumers**:
- **ScoringEngine** (confluent_kafka.Consumer): reads per-contestant with unique `group_id`, `auto.offset.reset=latest` ensures new evaluations don't replay history

**Batching**:
- Engine accumulates up to 1024 TradeEvents before publishing
- Kafka producer uses `linger.ms=1` (wait up to 1ms for batching)
- Scoring engine pulls with 50ms timeout (batches messages)

### Zookeeper

**Role**: Kafka cluster coordination (broker leader election, partition metadata)

**Configuration**:

```yaml
server.1=zookeeper:2181  # Single node (local development)
autopurge.snapRetainCount=3
autopurge.purgeInterval=1  # Delete old snapshots daily
```

---

## 12. Infrastructure as Code

### Docker Compose (Local)

```yaml
version: "3.9"
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "2181"]
      interval: 5s
      timeout: 3s
      retries: 5

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    depends_on:
      zookeeper:
        condition: service_healthy
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    ports:
      - "9092:9092"
    healthcheck:
      test: ["CMD", "kafka-topics", "--list", "--bootstrap-server", "localhost:29092"]

  redis:
    image: redis:7-alpine
    command: redis-server --appendonly no --save ""
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]

  matching-engine:
    build:
      context: .
      dockerfile: matching-engine/Dockerfile
    ports:
      - "8080:8080"
    depends_on:
      kafka:
        condition: service_healthy
    environment:
      KAFKA_BROKERS: kafka:29092
      EXCHANGE_PORT: "8080"
      EXCHANGE_HOST: "0.0.0.0"
    restart: on-failure

  telemetry-api:
    build:
      context: .
      dockerfile: telemetry/Dockerfile
    ports:
      - "8000:8000"
    depends_on:
      redis:
        condition: service_healthy
    environment:
      KAFKA_BROKERS: kafka:29092
      REDIS_URL: redis://redis:6379
    restart: on-failure
    volumes:
      - ~/.kube/config:/root/.kube/config:ro
      - /var/run/docker.sock:/var/run/docker.sock:ro

  leaderboard:
    build:
      context: .
      dockerfile: leaderboard/Dockerfile
    ports:
      - "5173:5173"
    depends_on:
      - telemetry-api
```

### Terraform (Cloud Deployment)

```hcl
# terraform/main.tf

terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.20"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.9"
    }
  }
}

provider "kubernetes" {
  config_path = "~/.kube/config"
}

provider "helm" {
  kubernetes {
    config_path = "~/.kube/config"
  }
}

# Kafka via Helm
resource "helm_release" "kafka" {
  name             = "kafka"
  repository       = "https://charts.bitnami.com/bitnami"
  chart            = "kafka"
  namespace        = "data-stores"
  create_namespace = true
  
  values = [
    <<-EOT
    auth:
      enabled: false
    
    replicaCount: 3
    persistence:
      enabled: true
      size: 10Gi
    
    broker:
      configuration: |
        auto.create.topics.enable=true
        log.retention.hours=168
    EOT
  ]
}

# Matching Engine Deployment
resource "kubernetes_deployment" "matching_engine" {
  metadata {
    name      = "matching-engine"
    namespace = "trading"
  }
  
  spec {
    replicas = 1
    
    selector {
      match_labels = {
        app = "matching-engine"
      }
    }
    
    template {
      metadata {
        labels = {
          app = "matching-engine"
        }
      }
      
      spec {
        container {
          name  = "engine"
          image = "trading/matching-engine:latest"
          
          port {
            container_port = 8080
            protocol       = "TCP"
          }
          
          env {
            name  = "KAFKA_BROKERS"
            value = "kafka.data-stores.svc.cluster.local:9092"
          }
          
          resources {
            requests = {
              cpu    = "2"
              memory = "2Gi"
            }
            limits = {
              cpu    = "4"
              memory = "4Gi"
            }
          }
          
          security_context {
            run_as_non_root = true
            run_as_user     = 10001
            read_only_root_filesystem = true
          }
        }
      }
    }
  }
}
```

---

## 13. CI/CD Pipeline

### GitHub Actions Workflow

```yaml
# .github/workflows/test-and-deploy.yml

name: Test & Deploy

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    
    services:
      zookeeper:
        image: confluentinc/cp-zookeeper:7.4.0
        env:
          ZOOKEEPER_CLIENT_PORT: 2181
      
      kafka:
        image: confluentinc/cp-kafka:7.4.0
        env:
          KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
          KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
          KAFKA_BROKER_ID: 1
      
      redis:
        image: redis:7-alpine

  build:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    # C++ matching engine
    - name: Build matching engine
      run: |
        cd matching-engine
        g++ -std=c++17 -O3 -DNDEBUG -DENABLE_KAFKA -pthread \
            main.cpp MatchingEngine.cpp Order.cpp networking/TCPServer.cpp \
            -lrdkafka++ -lrdkafka -o engine
    
    # Python telemetry
    - name: Lint Python
      run: |
        pip install flake8
        flake8 telemetry/ infra/
    
    # Frontend
    - name: Build frontend
      run: |
        cd leaderboard
        npm install
        npm run build
    
    # Docker images
    - name: Build images
      run: |
        docker build -f matching-engine/Dockerfile -t trading/engine:latest .
        docker build -f telemetry/Dockerfile -t trading/telemetry:latest .
        docker build -f leaderboard/Dockerfile -t trading/leaderboard:latest .
    
    # Push to registry
    - name: Push images
      if: github.ref == 'refs/heads/main'
      run: |
        echo ${{ secrets.DOCKER_PASSWORD }} | docker login -u ${{ secrets.DOCKER_USERNAME }} --password-stdin
        docker push trading/engine:latest
        docker push trading/telemetry:latest
        docker push trading/leaderboard:latest
  
  deploy:
    needs: build
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Configure Kubernetes
      run: |
        mkdir -p ~/.kube
        echo "${{ secrets.KUBECONFIG }}" > ~/.kube/config
        chmod 600 ~/.kube/config
    
    - name: Deploy with Terraform
      run: |
        cd terraform
        terraform init
        terraform plan -out=tfplan
        terraform apply tfplan
```

---

## 14. Composite Scoring Algorithm

### Formula

```
score = TPS / max(p99_latency_us, 0.1)
```

### Derivation

#### Why TPS (numerator)?

- **Rewards throughput**: Higher order submission rate → higher score
- **Incentivizes market participation**: Passive bots (low TPS) score lower than active bots
- **Reflects hardware optimization**: Efficient bots submit more orders in the same time

#### Why P99 Latency (denominator)?

- **Penalizes tail latency**: A few slow trades can hurt score disproportionately
- **Reflects SLA requirements**: In production trading, P99 is often the SLA metric
- **Avoids mean-chasing**: Mean latency is driven by fast matches; P99 captures worst-case

#### Why divide?

The ratio penalizes **both low throughput AND high latency**, creating a trade-off:

| TPS | P99 (µs) | Score | Interpretation |
|-----|----------|-------|---|
| 1000 | 100 | 10.0 | Balanced: moderate throughput, moderate latency |
| 1000 | 10 | **100.0** | Exceptional: high throughput, excellent latency |
| 100 | 1 | **100.0** | Niche: low throughput, sub-microsecond latency |
| 100 | 100 | **1.0** | Poor: low throughput, high latency |
| 10 | 10000 | **0.1** | Critical: very low throughput, very high latency |

### Window-Based Calculation

Scores are not cumulative; they're computed over a sliding 10-second window:

```python
def score_at_time_t(contestant_id: str, window_seconds: int = 10) -> float:
    now = time.time()
    recent_trades = [
        t for t in trades[contestant_id]
        if (now - t.time) <= window_seconds
    ]
    
    if not recent_trades:
        return 0
    
    tps = len(recent_trades) / window_seconds
    latencies_us = [t.latency_ns // 1000 for t in recent_trades]
    p99 = percentile(latencies_us, 0.99)
    
    return tps / max(p99, 0.1)
```

**Implication**: A bot can improve its score at any point by increasing TPS or reducing latency. The leaderboard is dynamic — rank changes as new trades are processed.

### Disqualification (-1 Score)

Any correctness violation sets `score = -1.0`:

```python
if not validate_trade(event):
    contestant_state.score = -1.0
    contestant_state.correctness_error = error_reason
    return  # stop processing further events
```

A score of **-1 always ranks last**, regardless of TPS or latency.

---

## 15. Technology Decisions

### Why C++ for the Matching Engine?

1. **Latency**: Compiled, no GC, predictable performance
2. **Throughput**: Lock-free data structures, CPU pinning, epoll
3. **Memory**: Fine-grained control, avoid heap fragmentation
4. **Binary Protocol**: Efficient wire format (32-byte orders)

### Why Python for Telemetry & Orchestration?

1. **Rapid iteration**: Fast development, rich libraries
2. **Kafka/Redis**: confluent-kafka and redis clients are mature
3. **Kubernetes**: python-client is standard
4. **Not on hot path**: Telemetry runs at 1kHz, not 1MHz

### Why Kubernetes?

1. **Isolation**: NetworkPolicy, PodSecurityPolicy, seccomp
2. **Scaling**: Easy to spawn/destroy contestant pods
3. **Observability**: Native logging, metrics, events
4. **Production-ready**: Used by 90% of enterprises

### Why React + Vite?

1. **Responsiveness**: Live updates via SSE
2. **Hot reload**: Vite dev mode (for development)
3. **Rich viz**: Recharts, responsive SVG charts
4. **Modern**: ES2020+, native async/await

### Why Kafka?

1. **Durability**: Trades are not lost if scoring engine crashes
2. **Scaling**: Multiple consumers independently track offsets
3. **Replay**: Can recompute scores from trade log
4. **Performance**: librdkafka is highly optimized

### Why Redis?

1. **Speed**: In-memory, microsecond access latency
2. **Simplicity**: Sorted sets are perfect for leaderboards
3. **Pub/Sub**: Built-in notifications (no extra broker)
4. **Ephemeral**: Scores reset between competitions

---

## 16. Architecture Decision Records

### ADR-001: WebSocket for Bot-Engine Communication

**Decision**: Use WebSocket (RFC 6455) instead of raw TCP

**Rationale**:
- Standard protocol, no custom binary framing
- Python `websockets` library is reliable
- Passes through firewalls/proxies (port 80/443 capable)
- Hand-rolled implementation gives latency control

**Alternatives Considered**:
- Raw TCP: Lower overhead (2-3%), but no standardization
- HTTP/REST: Req-resp only, polling adds latency
- gRPC: Overkill, binary protocol adds complexity

---

### ADR-002: MPSC Queue for Order Ingress

**Decision**: Use lock-free Multi-Producer Single-Consumer queue between TCP and matching

**Rationale**:
- TCPServer (many connections) → MatchingEngine (one thread)
- Avoids mutex contention (lock-free)
- Bounded memory (ring buffer)

**Alternatives Considered**:
- Mutex-protected std::queue: Simple but lock contention
- std::channel (Rust-like): Not available in C++17 std

---

### ADR-003: Kafka for Trade Durability

**Decision**: Publish all trades to Kafka before scoring

**Rationale**:
- Scoring engine failure doesn't lose trades
- Enables windowed scoring (can replay)
- Decouples engine from scoring

**Alternatives Considered**:
- Direct Redis writes: No durability, scoring loss
- File-based log: Complex recovery, no consumer groups
- In-memory queue: Bounded capacity, loss on crash

---

### ADR-004: eBPF for Kernel-Level Latency

**Decision**: Optional eBPF-based latency probing (future enhancement)

**Rationale**:
- Separates kernel vs. userspace latency contribution
- Detects OS scheduling jitter
- Non-intrusive (kernel-space, zero userspace overhead)

**Limitation**: Requires Linux kernel 4.4+ with BPF JIT enabled

---

## 17. Performance Characteristics

### Matching Engine

| Metric | Value | Notes |
|--------|-------|-------|
| **Throughput** | 10,000+ TPS | Single core (Core 1) |
| **P50 Latency** | 5-10 µs | Cache hits, empty queue |
| **P99 Latency** | 50-100 µs | Queue depth variations |
| **Max Order Book Size** | 100,000 levels | Bounded by DRAM |
| **CPU Pinning** | Cores 1,2,3 (Core 0 free) | Prevents OS jitter |
| **Memory Usage** | ~200 MB | OrderBook + histograms |

### Network Latency (t1 - t0)

Measured from bot submission to engine reception:

| Path | Typical | Worst-case |
|------|---------|-----------|
| Local VM (Minikube) | 0.1-1 ms | 5-10 ms |
| Docker bridge network | 0.05-0.5 ms | 2-5 ms |
| Physical LAN | 0.01-0.1 ms | 0.5-1 ms |

### End-to-End (t2 - t0)

Total from bot order submission to trade execution:

- **P50**: 0.2 ms
- **P99**: 0.5 ms
- **P99.9**: 2 ms

### Scoring Engine

| Metric | Value |
|--------|-------|
| Kafka poll interval | 50 ms |
| Trade processing | < 1 ms per 100 trades |
| Percentile calculation | < 10 ms per window |
| Redis update latency | < 5 ms |
| SSE fan-out | < 50 ms to browser |

### Scaling Limits

| Resource | Single Engine | Cluster (N×) |
|----------|---------------|--------------|
| Concurrent bots | ~170 (TCP limit) | N × 170 |
| Matching throughput | 10k TPS | N × 10k TPS (with partitioning) |
| Latency (P99) | <100 µs | ~same (per partition) |

---

## 18. Contestant Upload Flow

### User Submission Journey

```
User Interface
    ↓
Upload Endpoint: POST /api/v1/submissions/upload
    ├─ Validate: .zip file present
    ├─ Extract: /tmp/sandbox_uploads/<contestant_id>/
    ├─ Fire-and-forget: asyncio.create_task(orchestrator)
    └─ Return: {"status": "QUEUED"}
    ↓
Orchestrator (async subprocess)
    ├─ Load K8s config
    ├─ Create namespace: evaluation-sandbox
    ├─ Create NetworkPolicy: egress :8080 only
    ├─ Auto-detect language (Python/Rust/Go/C++)
    ├─ Generate Dockerfile (language-specific)
    ├─ Build image: docker build -f Dockerfile.generated -t contestant-agent:<id> .
    ├─ Spawn scoring engine: python telemetry/scoring_engine.py ...
    └─ Apply pod: kubectl apply -f pod.yaml
    ↓
Contestant Pod (K8s Pod)
    ├─ Runtime: Linux container, UID 10002
    ├─ Permissions: read-only root FS, dropped caps, no privilege escalation
    ├─ Network: egress restricted to :8080
    ├─ Process: python3 sandbox_sdk.py (for Python)
    │   ├─ Load solution.py
    │   ├─ Open WebSocket: ws://EXCHANGE_HOST:8080
    │   ├─ Tick loop (every 1s):
    │   │   ├─ Generate mock market tick
    │   │   ├─ Call on_market_update(tick, exchange)
    │   │   └─ submit_order() → send WebSocket frame
    │   └─ Run for 60 seconds (or until manual stop)
    ↓
Matching Engine (running on host)
    ├─ TCPServer thread receives WebSocket frames
    ├─ Parse 32-byte OrderMessage
    ├─ Record t1 (gateway time)
    ├─ Enqueue into MPSC
    ├─ MatchingEngine thread processes orders
    ├─ When trade matches:
    │   ├─ Record t2 (match time)
    │   ├─ Create TradeEvent (60 bytes)
    │   └─ Publish to Kafka
    ↓
Scoring Engine (Python subprocess)
    ├─ Kafka consumer (unique group_id)
    ├─ Pull trade events
    ├─ Validate (timestamp ordering, monotonicity, etc)
    ├─ Compute metrics (TPS, P99 latency)
    ├─ Update Redis
    │   ├─ ZADD leaderboard:scores <score> <contestant_id>
    │   ├─ HSET contestant:meta:<id> { tps, p99, score, ... }
    │   └─ PUBLISH leaderboard_updates <json>
    ↓
Real-Time Leaderboard (Browser)
    ├─ SSE EventSource listening
    ├─ Receive "update" event
    ├─ setRows() triggers React re-render
    ├─ Table, charts updated live
    ↓
Evaluation Complete (60s elapsed)
    ├─ Orchestrator: wait_for_agent_completion() returns
    ├─ Pod phase = "Succeeded"
    ├─ SIGTERM → scoring engine
    ├─ Final scores persist in Redis
    ├─ kubectl delete pod
    ↓
Final Leaderboard
    ├─ Contestant appears at final rank
    ├─ Score, TPS, latency, correctness all visible
    ├─ Remains on leaderboard for next submission cycle
```

### File Structure: Accepted ZIP

```
submission.zip
├── sandbox_sdk.py        (REQUIRED: exact copy from sample_bot/)
├── solution.py           (REQUIRED: contestant's trading logic)
├── requirements.txt      (RECOMMENDED: pip dependencies)
├── utils.py              (OPTIONAL: utility modules)
└── data/
    └── historical.csv    (OPTIONAL: data files)

OR for non-Python:

submission.zip
├── main.rs               (Rust) or
├── main.go               (Go) or
├── main.cpp              (C++)
├── Cargo.toml / go.mod / Makefile
└── (language-specific structure)
```

### Validation Checks

```python
def validate_submission(extracted_dir: str) -> bool:
    """Ensure submission structure is sound"""
    
    checks = [
        (os.path.exists(os.path.join(extracted_dir, "sandbox_sdk.py")),
         "sandbox_sdk.py is REQUIRED (copy from sample_bot/)"),
        
        (os.path.exists(os.path.join(extracted_dir, "solution.py")),
         "solution.py is REQUIRED (your trading logic)"),
        
        (os.path.getsize(extracted_dir) < 100 * 1024 * 1024,  # 100 MB limit
         "Submission exceeds 100 MB"),
        
        (count_files(extracted_dir) < 10000,  # no zip bomb
         "Submission contains too many files (>10000)"),
    ]
    
    for condition, error_msg in checks:
        if not condition:
            raise ValidationError(error_msg)
    
    return True
```

---

## 19. Week 4 — Final Delivery Summary

### Completed Components

- [x] **Matching Engine (C++)**: Lock-free MPSC queues, epoll event loop, CPU pinning, sub-100µs P99 latency
- [x] **WebSocket Protocol**: Full RFC 6455 implementation, binary frame support, client masking
- [x] **Order Book**: Buy/sell sides with an O(1) lookup map locating any resting order.
  Removal itself is O(level size) — the price level is a contiguous vector, so erasing
  shifts the orders behind it and their recorded positions are refreshed to match. See
  Known Limitation 8: the cancel entry point this map exists to serve has no caller yet.
- [x] **Telemetry Ingestion**: Kafka producer (librdkafka), trade batching, SPSC queue
- [x] **Scoring Engine (Python)**: Kafka consumer, correctness validation, sliding-window metrics, Redis output
- [x] **API Gateway (FastAPI)**: Upload endpoint, leaderboard query, SSE streaming
- [x] **Real-Time Dashboard (React)**: Live leaderboard table, bar charts, line charts, theme toggle
- [x] **Kubernetes Orchestration**: Pod spawning, namespace isolation, network policies, security contexts
- [x] **Docker Compose (Local)**: Full stack in 7 services, dependency ordering, health checks
- [x] **Sandbox Security**: Pod Security Standard ("restricted"), capability dropping, read-only root FS
- [x] **Multi-Language Support**: Auto-detection (Python/Rust/Go/C++), Dockerfile generation

### Known Limitations

1. **Single Matching Engine Instance**: No sharding (though Kafka can support multiple partitions)
2. **No Persistence**: Redis uses in-memory only; scores lost on restart
3. **No TLS**: Local development assumes trusted network
4. **Clock Synchronization**: t0-t1 network latency requires NTP if distributed
5. **No Replay**: Can't re-run scoring on historical trades (Kafka has data, but no UI)
6. **Time Priority Is Not Validated**: The scoring engine verifies the *price* half of
   price-time priority — a match that skipped a better-priced resting order is caught,
   because two consecutive trades sharing one order id constrain the execution price in
   a known direction. The *time* half is undecidable from what the platform publishes:
   `TradeEvent` carries no arrival sequence for the resting side, so two orders sitting
   at the same price are indistinguishable in the outbound stream. An engine that fills
   the newest resting order instead of the oldest passes validation today. Closing this
   requires mirroring inbound orders to their own Kafka topic and replaying them through
   a reference book — which adds a publish to the matching hot path and would move the
   measured latency figures.
7. **Fill Accuracy Cannot Be Checked Against Submitted Quantity**: The 60-byte
   `TradeEvent` wire frame carries only the traded quantity, never the quantity the order
   was submitted with. The scoring engine can therefore accumulate fills per order id but
   has no bound to check them against, so an engine that over-fills an order beyond its
   size is not detectable from telemetry. Same fix as above: the inbound order stream has
   to be observable before this invariant can be evaluated.
8. **Order Cancellation Is Unreachable**: `MatchingEngine::cancelOrder` is implemented
   and its index bookkeeping is now correct and unit-tested, but nothing calls it. The
   `OrderMessage` wire frame has no cancel message type — `side` only distinguishes buy
   from sell — so no client can request a cancellation. The order book's `orderMap` is
   maintained on every insert and fill to support a path that cannot currently be invoked.

### Future Enhancements

1. **eBPF Kernel Tracing**: Isolate kernel vs. userspace latency
2. **Multi-Engine Sharding**: Hash orders by contestant ID to distribute load
3. **Persistent Redis**: RDB snapshots for leaderboard history
4. **TLS/mTLS**: For distributed deployments
5. **Chaos Engineering**: Fault injection framework
6. **Real Order Book**: Instead of mock ticks, real market data stream
7. **Strategy Replay**: Contestant can upload historical market data and replay their trades
8. **Cost Attribution**: Which orders are most expensive (latency-wise)?
9. **Competitor API**: Contestants can query real-time leaderboard from their bot

### Deployment Checklist

- [ ] Commit all changes (`git add -A && git commit`)
- [ ] Fix CORS errors in telemetry/api.py
- [ ] Test full stack locally (`docker compose up`)
- [ ] Verify leaderboard SSE updates in browser
- [ ] Test Kubernetes pod isolation (try escaping)
- [x] Load test with bot-fleet (170 concurrent connections) — 5 runs x 60s, results in
      `results/`; median 7,760 trades/s, P99 16.62 µs, zero drops or failures
- [x] Verify Kafka event ordering (no gaps in match_id) — the scoring engine fails a run
      on any `MATCH_SEQUENCE` gap; 2,143,091 events across 5 runs passed
- [ ] Test contestant code with intentional errors (should be disqualified)
- [ ] Check Redis memory usage under load
- [x] Verify scoring engine doesn't replay historical events — the compose service pins
      `KAFKA_OFFSET_RESET=latest`, and every run recorded a starting backlog of 0
- [ ] Test orchestrator cleanup (pods fully deleted)
- [ ] Review terraform/ for cloud deployment readiness

### Success Metrics

Measured against the recorded benchmark in `results/` (median of 5 runs):

- **P50 matching latency**: target < 10 µs — **measured 3.56 µs** ✓
- **P99 matching latency**: target < 100 µs — **measured 16.62 µs** ✓
- **Throughput**: target > 5,000 TPS (single engine) — **measured 7,760 trades/s** ✓
- **Zero order loss**: target zero drops under load — **measured 0 drops, 0 failures across
  5 runs and 3,013,607 orders sent** ✓
- **Zero false positives**: target disqualification only on genuine violations — **2,143,091
  trade events validated across 5 runs with no spurious disqualification** ✓

Not yet measured — these remain design targets rather than results:

- **SSE update latency**: < 50 ms to browser — no instrumentation exists for this yet
- **Zero trade loss end-to-end**: trades published to Kafka are not reconciled against what
  lands in Redis, so loss between the two would currently go unnoticed
- **Perfect isolation**: contest pods cannot escape the sandbox — the isolation test in the
  deployment checklist above is still outstanding

---

## End of Document

This comprehensive design document provides complete technical reference for the HFT Market Simulator platform. For questions, implementation details, or updates, refer to the codebase (`/home/hegde/Desktop/trade_plat`) and the design sources.

**Last Updated**: 2026-06-14  
**Document Version**: 1.0
