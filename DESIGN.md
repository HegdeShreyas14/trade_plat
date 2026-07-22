# HFT Market Simulator — System Design Document

> A deep technical reference covering every file, every design decision, and the full end-to-end data flow from a contestant's `on_market_update` call to a score on the live leaderboard.

---

## Table of Contents

1. [System Purpose](#1-system-purpose)
2. [Architecture Overview](#2-architecture-overview)
3. [End-to-End Data Flow](#3-end-to-end-data-flow)
4. [The Matching Engine (C++)](#4-the-matching-engine-c)
   - [main.cpp](#maincpp)
   - [Order.h / Order.cpp](#orderh--ordercpp)
   - [trade.h](#tradeh)
   - [Protocol.h — The Inbound Wire Format](#protocolh--the-inbound-wire-format)
   - [MarketEvents.h — The Outbound Wire Format](#marketeventsh--the-outbound-wire-format)
   - [OrderBook.h](#orderbookh)
   - [MatchingEngine.h / MatchingEngine.cpp](#matchingengineh--matchingenginecpp)
   - [MPSCQueue.h](#mpscqueueh)
   - [SPSCQueue.h](#spscqueueh)
   - [MemoryPool.h](#memorypoolh)
   - [KafkaOffloader.h](#kafkaoffloaderh)
   - [networking/TCPServer.h / TCPServer.cpp](#networkingtcpserverh--tcpservercpp)
   - [matching-engine/Dockerfile](#matching-enginedockerfile)
5. [The Bot SDK and Contestant Interface](#5-the-bot-sdk-and-contestant-interface)
   - [sample_bot/sandbox_sdk.py](#sample_botsandbox_sdkpy)
   - [sample_bot/solution.py](#sample_botsolutionpy)
   - [sample_bot/requirements.txt](#sample_botrequirementstxt)
   - [bot-fleet/main.py](#bot-fleetmainpy)
   - [bot-fleet/Dockerfile](#bot-fleetdockerfile)
6. [Telemetry and Scoring Layer (Python)](#6-telemetry-and-scoring-layer-python)
   - [telemetry/scoring_engine.py](#telemetryscoring_enginepy)
   - [telemetry/api.py](#telemetryapipy)
   - [telemetry/Dockerfile](#telemetrydockerfile)
   - [telemetry/requirements.txt](#telemetryrequirementstxt)
7. [Orchestration Layer](#7-orchestration-layer)
   - [infra/orchestrator.py](#infraorchestratorpy)
   - [infra/requirements.txt](#infrarequirementstxt)
   - [infra/k8s/](#infrak8s)
8. [Frontend Dashboard](#8-frontend-dashboard)
   - [leaderboard/src/App.jsx](#leaderboardsrcappjsx)
   - [leaderboard/src/main.jsx](#leaderboardsrcmainjsx)
   - [leaderboard/src/styles.css](#leaderboardsrcstylescss)
   - [leaderboard/index.html / vite.config.js](#leaderboardindexhtml--viteconfigjs)
   - [leaderboard/Dockerfile](#leaderboarddockerfile)
9. [Infrastructure and Configuration](#9-infrastructure-and-configuration)
   - [docker-compose.yml](#docker-composeyml)
   - [.dockerignore](#dockerignore)
   - [.gitignore](#gitignore)
10. [Technology Deep-Dives](#10-technology-deep-dives)
    - [Linux epoll and Non-Blocking I/O](#linux-epoll-and-non-blocking-io)
    - [WebSocket Protocol (RFC 6455)](#websocket-protocol-rfc-6455)
    - [Lock-Free Queues and Cache Line Alignment](#lock-free-queues-and-cache-line-alignment)
    - [CPU Pinning with pthread_setaffinity_np](#cpu-pinning-with-pthread_setaffinity_np)
    - [Apache Kafka](#apache-kafka)
    - [Redis Sorted Sets and Pub/Sub](#redis-sorted-sets-and-pubsub)
    - [FastAPI and Server-Sent Events](#fastapi-and-server-sent-events)
    - [Kubernetes Sandbox Isolation](#kubernetes-sandbox-isolation)
    - [React, Vite, and SSE Consumption](#react-vite-and-sse-consumption)
11. [Wire Format Reference](#11-wire-format-reference)
12. [Timestamp and Latency Model](#12-timestamp-and-latency-model)
13. [Scoring Formula](#13-scoring-formula)
14. [Thread and Process Map](#14-thread-and-process-map)

---

## 1. System Purpose

This platform is a real-time algorithmic trading competition infrastructure. A contestant writes a trading strategy in Python (or other languages), zips it up, and uploads it through a web interface. The platform:

1. Extracts and containerizes the code automatically.
2. Runs the bot inside a sandboxed Kubernetes pod.
3. The bot trades against a low-latency C++ matching engine over WebSocket.
4. Every executed trade is published to Apache Kafka and consumed by a scoring engine.
5. Scores (based on throughput and latency) are written to Redis and streamed live to a leaderboard in the browser.

The point of the system is not just to evaluate strategies but to stress-test their *order submission quality* — how fast they submit, how often they generate real trades (crossing the spread), and whether they respect the engine's wire protocol correctly.

---

## 2. Architecture Overview

```
┌────────────────────────────────────────────────────────────────┐
│                        HOST MACHINE                            │
│                                                                │
│  ┌──────────────┐    ┌──────────────┐    ┌─────────────────┐  │
│  │   Zookeeper  │    │    Kafka     │    │     Redis       │  │
│  │  (port 2181) │◄───│ (port 29092) │    │  (port 6379)   │  │
│  └──────────────┘    └──────┬───────┘    └────────┬────────┘  │
│                             │                     │            │
│  ┌──────────────────────────▼─────┐   ┌───────────▼─────────┐ │
│  │     Matching Engine (C++)      │   │   Telemetry API     │ │
│  │         port 8080              │   │   (FastAPI)         │ │
│  │  WebSocket · epoll · CPU-pinned│   │   port 8000         │ │
│  └──────────────────────────┬─────┘   └───────────┬─────────┘ │
│                             │                     │            │
│          TradeEvents        │              SSE    │            │
│          (binary, 60B)      │              stream │            │
│                             ▼                     ▼            │
│  ┌──────────────────────────────┐   ┌─────────────────────┐   │
│  │      Kafka (trade-events)    │   │  Leaderboard UI     │   │
│  │         topic                │   │  (React/Vite)       │   │
│  └──────────────────────────┬───┘   │  port 5173          │   │
│                             │       └─────────────────────┘   │
│  ┌──────────────────────────▼──┐                              │
│  │     Scoring Engine (Python) │                              │
│  │  Kafka consumer · validates │                              │
│  │  · scores · writes Redis    │                              │
│  └─────────────────────────────┘                              │
└────────────────────────────────────────────────────────────────┘
                         ▲
                    ws://10.0.2.2:8080
                         │
┌────────────────────────┴───────────────┐
│            Minikube Cluster            │
│                                        │
│  ┌──────────────────────────────────┐  │
│  │  evaluation-sandbox (Namespace)  │  │
│  │                                  │  │
│  │  ┌────────────────────────────┐  │  │
│  │  │ active-contestant-sandbox  │  │  │
│  │  │       (Pod)                │  │  │
│  │  │  - Runs contestant bot     │  │  │
│  │  │  - NetworkPolicy: egress   │  │  │
│  │  │    only to port 8080       │  │  │
│  │  └────────────────────────────┘  │  │
│  └──────────────────────────────────┘  │
└────────────────────────────────────────┘
```

**Key design principle:** The matching engine and all telemetry infrastructure run on the host (inside Docker Compose). The contestant's bot runs inside Minikube, isolated from the internet by a `NetworkPolicy`, and can only egress to port 8080 — the matching engine's WebSocket port exposed on the host.

---

## 3. End-to-End Data Flow

Tracing a single order from the moment a contestant's strategy function runs to the moment a score appears on screen:

```
[1] solution.py::on_market_update() calls exchange.submit_order("BUY", 150.0, 10)

[2] sandbox_sdk.py::ExchangeConnection.submit_order():
    - records t0 = time.time_ns()  (bot-side timestamp)
    - packs struct: <QQdIB3x> → 32 bytes
    - schedules ws.send(payload) as an asyncio task

[3] websockets library:
    - wraps 32 bytes in a WebSocket binary frame (opcode 0x2)
    - applies a 4-byte random XOR mask (RFC 6455 client requirement)
    - sends over TCP

[4] TCPServer.cpp::handleClientData() on the engine:
    - epoll reports EPOLLIN on the client's file descriptor
    - reads raw bytes into per-client RingBuffer
    - if websocket_ready == false: parses HTTP upgrade, sends 101 response
    - if websocket_ready == true: parses WS frame header, verifies opcode == 0x2,
      unmasks 32 bytes, writes into payload buffer
    - records t1 = system_clock::now() (gateway-ingest timestamp)
    - calls engine_.enqueueOrder(Order(order_id, qty, isBuy, price, t0, t1))

[5] MPSCQueue::enqueue() inside TCPServer thread:
    - lock-free CAS loop claims a slot in the 65536-element ring buffer
    - writes Order into the slot
    - memory_order_release on sequence counter makes it visible to the consumer

[6] MatchingEngine::processLoop() on Core 1:
    - MPSCQueue::dequeue() acquires the Order
    - calls addOrder() → inserts into the std::map buy/sell side
    - calls matching():
      - checks bestBid >= bestAsk
      - determines taker by seqNo (lower seqNo = arrived first = maker)
      - computes tradedQty = min(buyQty, sellQty)
      - records t2 = system_clock::now()
      - constructs TradeEvent{match_id, buy_id, sell_id, t0, t1, t2, price, qty}
    - calls kafkaOffloader.publish(event)

[7] SPSCQueue inside KafkaOffloader:
    - MatchingEngine thread (Core 1) pushes TradeEvent into 65536-element SPSC queue
    - KafkaOffloader worker thread (Core 3) drains up to 1024 events per iteration
    - calls librdkafka Producer::produce() with the raw 60-byte struct as payload
    - librdkafka batches and sends to kafka:29092

[8] Kafka topic "trade-events":
    - stores the 60-byte binary payload as a Kafka message value
    - retains until consumer group commits offset

[9] scoring_engine.py (spawned by orchestrator per evaluation):
    - confluent_kafka Consumer polls with 50ms timeout
    - receives Kafka message
    - calls unpack_trade_events(): struct.unpack_from("<QQQQQQdI", payload, offset)
      → iterates through all TradeEvent structs in the payload (engine batches them)
    - ContestantState.validate(event): checks match_id monotonic, t0<t1<t2,
      no self-match, valid price/qty
    - ContestantState.accept(event): stores (wall_time, engine_latency_ns) in a deque
    - every 1 second: ContestantState.metrics() computes TPS and p50/p90/p99 latency
    - calls publish_metrics():
      - redis.zadd("leaderboard:scores", {contestant_id: score})
      - redis.hset("contestant:meta:<id>", {tps, p50, p90, p99, score, ...})
      - redis.publish("leaderboard_updates", json_payload)

[10] telemetry/api.py SSE stream:
    - FastAPI coroutine subscribed to Redis pub/sub channel
    - receives published JSON update
    - calls read_leaderboard() → ZREVRANGE + HGETALL from Redis
    - yields SSE "update" event with full sorted leaderboard payload

[11] leaderboard/src/App.jsx:
    - EventSource receives SSE event
    - setRows(parseRows(event)) → triggers React re-render
    - BarChart, LineChart, and table all update live with new scores
```

---

## 4. The Matching Engine (C++)

The engine is a standalone C++17 binary. It has no external runtime dependencies except `librdkafka` (for Kafka publishing) and the Linux kernel (for epoll and pthreads). It runs as a single process with three dedicated OS threads, each pinned to a specific CPU core.

### `main.cpp`

The entry point. It performs three actions and then sleeps forever:

1. Constructs `MatchingEngine` and calls `engine.start()`, which spawns the processing and metrics threads and starts the `KafkaOffloader`.
2. Constructs `TCPServer` on port 8080 and calls `server.start()`, which creates the epoll descriptor and spawns the TCP thread.
3. Enters an infinite `sleep_for(1s)` loop — the main thread has no work to do. The actual work is distributed across the three pinned threads.

The `server.stop()` and `engine.stop()` calls after the loop are dead code (the loop never exits), but they represent the correct teardown order: first drain the network, then drain the matching queue.

### `Order.h` / `Order.cpp`

The in-memory representation of an order inside the engine. Distinct from the wire format (`OrderMessage`).

```cpp
struct Order {
    int    OrderId;    // from wire: msg.order_id (truncated to int)
    int    quantity;   // from wire: msg.qty
    bool   IsBuy;      // derived from side byte
    double price;      // from wire: msg.price
    long long t0;      // bot-generation timestamp (nanoseconds)
    long long t1;      // gateway-ingest timestamp (nanoseconds, set in TCPServer)
    long long seqNo;   // global monotonic counter, assigned on construction
    long long timestamp; // wall-clock in milliseconds (unused in hot path)
};
```

`seqNo` is critical: it is a global static counter incremented atomically every time an `Order` is constructed. This gives a total ordering across all orders regardless of which TCP connection they arrived on. The matching engine uses `seqNo` to determine the **maker** (the order that arrived first) vs the **taker** (the order that crossed the spread). The execution price is set to the maker's limit price, which is standard price-time priority matching.

### `trade.h`

An older internal struct that holds executed trade data in memory. It is declared in `MatchingEngine.h` (`std::vector<Trade> tradeHistory`) but `tradeHistory` is never populated in the current matching logic — the engine switched to publishing `TradeEvent` directly to Kafka. `trade.h` is a legacy artifact from a version before Kafka was added.

### `Protocol.h` — The Inbound Wire Format

```
Offset  Size  Field         Description
──────  ────  ────────────  ─────────────────────────────────────────
0       8     order_id      Bot-assigned unique ID (uint64, little-endian)
8       8     timestamp_ns  Bot-generation time (uint64, nanoseconds since epoch)
16      8     price         Limit price (IEEE 754 double, little-endian)
24      4     qty           Quantity (uint32, little-endian)
28      1     side          1 = Buy, 2 = Sell (or ASCII 'B'/'b'/'S'/'s')
29      3     padding       Explicit zero-fill to reach 32 bytes
```

The `static_assert(sizeof(OrderMessage) == 32)` is a compile-time contract: if the struct layout ever drifts from 32 bytes, the build fails immediately. The engine accepts both numeric side codes (1/2) and ASCII characters ('B'/'S') for backwards compatibility with older bots.

The `timestamp_ns` field becomes `t0` in the `Order` struct — it is the moment the bot decided to submit the order. This is the start of the latency measurement chain.

### `MarketEvents.h` — The Outbound Wire Format

```
Offset  Size  Field           Description
──────  ────  ──────────────  ─────────────────────────────────────────
0       8     match_id        Monotonic match counter (uint64)
8       8     buy_order_id    Buyer's order_id from the wire (uint64)
16      8     sell_order_id   Seller's order_id from the wire (uint64)
24      8     t0_ns           Taker's bot-generation timestamp (uint64)
32      8     t1_ns           Taker's gateway-ingest timestamp (uint64)
40      8     t2_ns           Engine match-completion timestamp (uint64)
48      8     price           Execution price (double)
56      4     qty             Traded quantity (uint32)
            ──────
Total   60 bytes
```

`__attribute__((packed))` is applied to ensure no compiler-inserted padding between fields. Without it, the double at offset 48 would naturally be 8-byte aligned, potentially adding 4 bytes of padding after the uint32 at offset 56, making the struct 64 bytes instead of 60. The `static_assert(sizeof(TradeEvent) == 60)` guards against this.

The three timestamps `t0`/`t1`/`t2` form the core of the latency measurement model (explained in detail in Section 12).

### `OrderBook.h`

```cpp
class OrderBook {
    std::map<double, std::vector<Order>, std::greater<double>> buyOrders;
    std::map<double, std::vector<Order>, std::less<double>>    sellOrders;
    std::unordered_map<int, OrderLocation> orderMap;
};
```

**Buy side:** `std::map` with `std::greater<double>` comparator — iteration starts at the highest bid (best price first). Each map key is a price level; the value is a vector of orders at that price, in arrival order (FIFO within a price level).

**Sell side:** `std::map` with `std::less<double>` comparator — iteration starts at the lowest ask (best price first).

**Why `std::map<double, ...>` for prices?** Using a floating-point key in a `std::map` is safe here because prices arrive as exact double values from the wire and are compared exactly, never computed. Two orders at "price 150.0" will always key to the same bucket.

**Order lookup map:** `std::unordered_map<int, OrderLocation>` enables O(1) cancellation. `OrderLocation` stores `{isBuy, price, vector_index}` — enough to find and erase the order in the buy or sell side map without scanning.

**Matching logic** (in `MatchingEngine::matching()`): The engine takes `buyOrders.begin()` (highest bid) and `sellOrders.begin()` (lowest ask). If `bestBid >= bestAsk`, a match occurs. It trades `min(buyQty, sellQty)`. If either order is fully filled, it is erased from both the price-level vector and the lookup map. If the vector becomes empty, the price-level key is removed from the map. This repeats in a loop until no crossing condition exists.

### `MatchingEngine.h` / `MatchingEngine.cpp`

The engine has four internal components that run concurrently:

**1. Ingress queue (`MPSCQueue<Order>`, capacity 65536)**
TCPServer threads push orders in. The matching loop pulls orders out. This is the only point of synchronization between the TCP layer and the matching layer — there are no mutexes on the hot path.

**2. `processLoop()` — pinned to Core 1**

Drains orders from the MPSC queue. For each order:
- Calls `addOrder()` → inserts into `OrderBook` → calls `matching()`.
- Records `t2 = system_clock::now()`.
- Computes `engine_latency = t2 - t1` (microseconds) and `network_latency = t1 - t0`.
- Writes to thread-local histogram arrays (no locking needed since only this thread writes).
- Every 65,536 orders, acquires a mutex and flushes thread-local histograms into the shared histograms.

The histogram approach avoids per-order mutex acquisition. Instead, the processing loop accumulates latency counts locally and flushes in bulk every ~65K orders. This reduces lock contention by a factor of 65,536.

**3. `metricsLoop()` — pinned to Core 2 (separate from Core 1)**

Wakes every second. Reads atomic counters and snapshots the shared histograms under the mutex (holds the lock only for a `std::copy`, not for computation). Computes p50/p90/p99 percentiles by scanning the histogram and finding the bucket index where the cumulative count crosses the target. Prints stats to stdout.

**4. `KafkaOffloader` — worker pinned to Core 3**

Drains trade events from the SPSC queue and publishes them to Kafka. See `KafkaOffloader.h` section below.

**Histogram design:** Each histogram has 100,000 buckets. Bucket index `i` represents a latency of exactly `i` microseconds. The maximum tracked latency is therefore 100ms (100,000 × 1µs). Any latency above 100ms is silently discarded (the `if (e_us < LATENCY_BUCKETS)` guard). This covers essentially all realistic HFT scenarios — anything above 100ms is a system anomaly, not a meaningful data point for the percentile distribution.

### `MPSCQueue.h`

A **Multi-Producer Single-Consumer** lock-free ring buffer, capacity must be a power of 2.

**How it works:**

The queue uses a `Cell` array where each cell contains:
- `std::atomic<size_t> sequence`: tracks the cell's lifecycle state
- `T data`: the stored item

The sequence number encodes whether the cell is:
- Empty and ready to be written: `sequence == enqueue_pos`
- Written and ready to be read: `sequence == enqueue_pos + 1`
- Read and ready to be reused: `sequence == dequeue_pos + capacity`

**Enqueue (multiple producers):** Each producer loads `enqueue_pos` and does a CAS (compare-and-swap) to claim it. Only one producer wins each slot. The losing producers retry. After claiming, the winner writes data and increments the sequence with `memory_order_release`, making the data visible to the consumer.

**Dequeue (single consumer):** The consumer checks if `sequence == dequeue_pos + 1`. If yes, it reads the data and advances `dequeue_pos`. No CAS needed because there is only one consumer.

**Why MPSC?** The TCPServer can have many simultaneous client connections. Each connection's `handleClientData()` call can `enqueueOrder()`. These calls happen on the same TCPServer thread (since it's single-threaded epoll), but in principle the design supports multiple producers. The single consumer is always the `processLoop()` thread.

**Cache line alignment:** `enqueue_pos_` and `dequeue_pos_` are each on separate 64-byte aligned cache lines. Without this, both positions would share a cache line. Every time one thread writes to `enqueue_pos_`, the CPU would invalidate the cache line containing `dequeue_pos_` on the other core, causing a "false sharing" cache miss even though the two values are logically independent.

### `SPSCQueue.h`

A **Single-Producer Single-Consumer** lock-free ring buffer, simpler than MPSC.

**How it works:** Uses two atomic head/tail pointers. The producer checks `head - tail < capacity` (not full) before writing. The consumer checks `tail != head` (not empty) before reading. Because there is exactly one of each, no CAS is needed — simple store/load with appropriate memory ordering is sufficient.

`head_` uses `memory_order_release` on store (after writing data) so the consumer sees the data before it sees the updated head. `tail_` can be `memory_order_relaxed` on the consumer's store since only the consumer updates it.

This is used between the matching loop (producer, Core 1) and the Kafka offloader (consumer, Core 3). The ring buffer absorbs bursts: if Kafka is temporarily slow, the engine can keep producing matches without blocking.

### `MemoryPool.h`

A fixed-size slab allocator with a free-list. Pre-allocates a `block_size` array of `Slot` structs (each containing aligned storage for one `T`). Freed slots are linked into a singly-linked list via `Slot::next`. Allocation pops from the free list; deallocation pushes back.

This is defined but **not currently used in the hot path** — the engine allocates `Order` objects on the stack and passes them by value into the MPSC queue. `MemoryPool` is present for future use if the design moves to heap-allocated orders.

### `KafkaOffloader.h`

Manages the Kafka producer lifecycle and the bridge from the matching engine to the message broker.

**Compile-time toggle:** All Kafka code is wrapped in `#ifdef ENABLE_KAFKA`. The Dockerfile compiles with `-DENABLE_KAFKA`. Without the flag, `publishBatch()` is a no-op — but the queue, thread, and `delivered_` counter still function. This allows building the engine without `librdkafka` for local testing.

**`start()` method:**
- Creates an `RdKafka::Conf` with `linger.ms=1` (micro-batching: wait 1ms before sending to allow burst accumulation), `acks=1` (leader acknowledgement only, not full ISR), `compression.codec=none` (low latency over throughput).
- Creates `RdKafka::Producer` and `RdKafka::Topic`.
- Starts the worker thread pinned to Core 3.

**Worker loop:**
```
while running or queue not empty:
    drain up to 1024 TradeEvents from SPSC queue into local batch[]
    if count > 0:
        publishBatch(): produce raw bytes (count × 60 bytes) as single Kafka message
        delivered_ += count
    else:
        _mm_pause()  // spin-wait hint to CPU pipeline
```

Crucially, `publishBatch` serializes a **batch** of `TradeEvent` structs as a single Kafka message payload. The scoring engine's `unpack_trade_events()` iterates through the payload in 60-byte chunks, yielding one `TradeEvent` per chunk. This reduces Kafka overhead when the engine is matching many orders per second.

**`PARTITION_UA`** (unassigned partition): Kafka chooses the partition. Since there is only one partition by default, all events go to partition 0 in order.

### `networking/TCPServer.h` / `TCPServer.cpp`

The TCP gateway — the most complex component in the codebase. It implements a full WebSocket server from scratch using Linux epoll.

**Why implement WebSocket manually?** Using a library like Boost.Beast would add a large dependency and its own threading model. A hand-rolled implementation gives precise control over buffer management and eliminates extra copies.

**Socket lifecycle:**

```
socket() → SO_REUSEADDR → bind() → O_NONBLOCK → listen(SOMAXCONN)
→ epoll_create1() → EPOLL_CTL_ADD server_fd
→ thread start, pinned to Core 2
```

**The epoll event loop:**

`epoll_wait(timeout=100ms)` blocks until events arrive or the timeout fires. The 100ms timeout is why `running_` is checked periodically even without activity — the server needs to notice when `stop()` is called.

When `server_fd_` becomes readable (new connection):
- Loop calling `accept()` up to 256 times (drain all pending connections in one pass — important for `EPOLLET` edge-triggered semantics on the server socket, though the server socket actually uses level-triggered here).
- For each accepted fd: set `TCP_NODELAY` (disable Nagle's algorithm — critical for latency; Nagle would buffer small writes for up to 200ms waiting to coalesce), set non-blocking, pre-allocate 64KB receive buffer, register in epoll with `EPOLLIN | EPOLLET`.

**Edge-triggered mode (`EPOLLET`):** When the client fd becomes readable, epoll fires exactly once. The handler must read until `EAGAIN` to drain all available data. If it doesn't, epoll won't fire again until new data arrives. This is why `handleClientData` has an outer `while(true)` that breaks on `EAGAIN`.

**Per-client state (`RingBuffer`):**
```cpp
struct RingBuffer {
    vector<uint8_t> data;          // raw receive buffer
    size_t read_ptr;               // how far we've parsed into data
    vector<uint8_t> payload;       // accumulates unmasked WS payloads
    size_t payload_read_ptr;       // how far we've consumed into payload
    bool websocket_ready;          // false until HTTP upgrade completed
};
```

**WebSocket handshake:** When `websocket_ready == false`, the handler scans the receive buffer for the `\r\n\r\n` header terminator. It extracts `Sec-WebSocket-Key`, computes SHA-1(key + GUID) and base64-encodes it, and sends the `101 Switching Protocols` response. Both SHA-1 and base64 are implemented from scratch in the anonymous namespace — no OpenSSL dependency.

**Frame parsing (after upgrade):** Each WebSocket frame has:
```
byte 0:  FIN bit + opcode (0x2 = binary, 0x8 = close)
byte 1:  MASK bit (must be 1 for client→server) + payload length (7-bit, or 126/127 for extended)
bytes 2–5: 4-byte mask key (if masked)
remaining: XOR-masked payload
```

The server enforces:
- `masked == false` → close the connection (RFC 6455 requires clients to mask)
- `payload_len > 1MB` → close the connection (sanity limit)
- `opcode == 0x8` → close (client-initiated close frame)
- `opcode == 0x2 or 0x0` → unmask and append to `payload` buffer
- Other opcodes (including `0x1` text frames) → silently skipped, `read_ptr` advances past the frame

This last point is critical: **text frames are silently discarded**. The old SDK sent JSON text frames; they were ignored by the engine without any error. The current SDK sends binary frames (opcode `0x2`) which are processed.

**Order extraction from payload:** `process_payload_orders` reads 32 bytes at a time from the payload buffer, casts to `OrderMessage`, records `t1`, constructs an `Order`, and calls `engine_.enqueueOrder()`. Multiple orders can be batched in a single WebSocket frame or across multiple frames — the code handles both.

**Buffer compaction:** To avoid unbounded memory growth, once `read_ptr` exceeds 16KB, the consumed portion is erased from the front of the buffer. This is an `O(N)` erase-from-front operation but it only happens every 16KB, not every frame.

### `matching-engine/Dockerfile`

Multi-stage build:

**Builder stage (Alpine 3.19):**
```dockerfile
FROM alpine:3.19 AS builder
RUN apk add g++ make linux-headers librdkafka-dev
```
`linux-headers` provides `<sys/epoll.h>`, `<pthread.h>`, and `<sched.h>`. These are kernel headers not included in the Alpine C++ toolchain by default.

Compile command:
```
g++ -std=c++17 -O3 -DNDEBUG -DENABLE_KAFKA -pthread \
    main.cpp MatchingEngine.cpp Order.cpp networking/TCPServer.cpp \
    -lrdkafka++ -lrdkafka -o engine
```
`-O3`: maximum optimization. `-DNDEBUG`: disables `assert()` calls. `-DENABLE_KAFKA`: activates Kafka code in `KafkaOffloader.h`.

**Runner stage (Alpine 3.19):**
Copies only the compiled binary. Adds `libstdc++` and `librdkafka` (runtime, no dev headers). Creates a non-root user `exchange_user` (UID 10001). Sets the binary to `555` (read+execute, no write).

The final image contains exactly two files: the engine binary and the Alpine base. No source code, no compiler, no build tools. Attack surface is minimal.

---

## 5. The Bot SDK and Contestant Interface

### `sample_bot/sandbox_sdk.py`

The platform harness that every Python contestant's ZIP must include (unchanged). It handles all network complexity so contestants only write strategy logic.

**What it does:**

1. Reads `EXCHANGE_GATEWAY_URL` from environment (set to `ws://10.0.2.2:8080` inside the K8s pod).
2. Reads `CONTESTANT_ID` from environment (set by orchestrator to the submission ID).
3. Opens an async WebSocket connection using the `websockets` library.
4. In a loop: generates a mock market tick with a random price in [148, 155] and calls the contestant's `on_market_update(tick, exchange)`.
5. Sleeps 1 second between ticks.

**`ExchangeConnection.submit_order(direction, price, quantity)`:**

```python
side_code = 1 if direction == "BUY" else 2
payload = struct.pack("<QQdIB3x",
    self.order_sequence,   # uint64 order_id
    time.time_ns(),        # uint64 t0 (bot-generation time)
    float(price),          # float64 price
    int(quantity),         # uint32 qty
    side_code,             # uint8 side
    # 3x padding bytes automatically zeroed by "3x"
)
asyncio.create_task(self.ws.send(payload))
```

`struct.pack("<QQdIB3x", ...)` with the `<` prefix means little-endian byte order — this matches the x86 native byte order on both the bot and engine sides, so no byte-swapping occurs.

`"3x"` is padding bytes. Unlike `"3s"`, it does not require a value argument — the three bytes are automatically zero-filled. This produces exactly 32 bytes matching the `OrderMessage` struct.

`ws.send(bytes_object)` in the `websockets` library automatically sends a **binary frame** (opcode `0x2`) and applies client-side masking as required by RFC 6455.

`asyncio.create_task()` schedules the coroutine on the running event loop without awaiting. This is fire-and-forget — if the frame fails to send, there is no retry. For a 32-byte frame on a local socket, the failure rate is effectively zero.

**Important limitation:** The mock market tick (`{"symbol": "BTCUSD", "price": random.randint(148, 155)}`) is generated locally by the SDK, not received from the engine. The engine does not broadcast order book state. Contestants trade based on simulated data — the competition is about submission speed and protocol compliance, not price prediction.

### `sample_bot/solution.py`

The template a contestant replaces with their strategy. The only interface contract is:

```python
def on_market_update(market_snapshot: dict, exchange: ExchangeConnection) -> None:
    ...
    exchange.submit_order("BUY" | "SELL", price: float, quantity: int)
```

The sample strategy: if price < 152, buy at `price + 1`. Since prices are always in [148, 155], this triggers a buy on about 80% of ticks. This is intentionally simple — the strategy is not evaluated for profitability, only for the correctness and rate of order submission.

### `sample_bot/requirements.txt`

Lists `websockets` (the Python WebSocket client library). When the orchestrator builds the contestant's Docker image, it runs `pip install -r requirements.txt` to install this dependency inside the container.

### `bot-fleet/main.py`

A standalone load testing tool — not part of the contest submission flow. Used by the platform operator to stress-test the matching engine before a contest.

**Architecture:** Spawns N async `Bot` instances of three types:
- `MarketMakerBot`: places bids and asks symmetrically around a mid-price, adjusting for inventory skew.
- `NoiseBot`: submits random orders at random prices, adding liquidity noise.
- `MomentumBot`: aggressively buys or sells in the direction of a trend, periodically reversing.

**Protocol compliance:** Unlike the old SDK, `bot-fleet/main.py` implements the WebSocket protocol at the raw TCP level (without the `websockets` library) and is confirmed working:
- Performs the HTTP upgrade handshake manually.
- Builds masked binary frames manually with `websocket_binary_frame()`.
- Reads a drain loop with `asyncio.open_connection` to prevent TCP buffer backpressure.
- Uses `TCP_NODELAY` to disable Nagle's algorithm.

The default fleet (50 market makers + 100 noise bots + 20 momentum bots = 170 concurrent connections) is what `bench/run_loadtest.py` drives to produce the recorded benchmark results under `results/`.

**Measured baseline** — median across 5 runs of 60s each, with a full stack teardown between runs, on an AMD Ryzen 9 8945HX (32 cores):

| Metric | Median | Spread across runs |
| --- | ---: | ---: |
| Engine throughput | 7,760 trades/s | 1.07x |
| P50 engine latency | 3.56 µs | 1.00x |
| P90 engine latency | 8.31 µs | 1.33x |
| P99 engine latency | 16.62 µs | 1.42x |
| Fleet send rate | 10,658 orders/s | 1.01x |

All 5 runs passed correctness validation with zero dropped or failed orders. Latency here is engine-internal (`t2 - t1`), not wire-to-wire.

Two caveats belong with these numbers. First, the run does not establish a throughput ceiling: the fleet's own sleep intervals cap offered load near 11.2k orders/s, and the engine absorbed everything it was given without backpressure, so this measures latency under fixed modest load rather than capacity. Second, the figures are only meaningful with a clean stack — runs sharing a stack with accumulated Kafka backlog were observed to differ by more than 3x at P99, because catch-up consumption competes with the engine for CPU. The harness restarts the stack between runs for exactly this reason.

See `results/` for the full per-run breakdown and the exact command to reproduce.

### `bot-fleet/Dockerfile`

Packages `bot-fleet/main.py` into a Python 3.12 Alpine image. Runs as UID 10002 (non-root). No pip dependencies — the bot only uses stdlib plus `asyncio`, `struct`, `hashlib`, `base64`, `socket`.

---

## 6. Telemetry and Scoring Layer (Python)

### `telemetry/scoring_engine.py`

The per-evaluation Kafka consumer and scorer. One instance is spawned per contestant evaluation by the orchestrator.

**Kafka consumer setup:**
```python
Consumer({
    "bootstrap.servers": kafka_brokers,
    "group.id": f"scoring-{contestant_id}-{timestamp}",  # unique per run
    "auto.offset.reset": "latest",                        # only score THIS run's trades
    "enable.auto.commit": "true",
    "fetch.wait.max.ms": "1",        # poll aggressively, don't wait for batch
    "queued.min.messages": "100000", # pre-fetch buffer
})
```

`auto.offset.reset=latest` is critical: when the consumer starts, it begins reading from the end of the topic, not the beginning. This ensures that trades from a previous contestant's run (still in Kafka retention) are not counted against the current contestant. A unique `group.id` per run ensures no stale committed offset is reused.

**`unpack_trade_events(payload)`:**
```python
TRADE_EVENT_FORMAT = "<QQQQQQdI"  # 6×uint64 + double + uint32 = 60 bytes
for offset in range(0, len(payload), TRADE_EVENT_SIZE):
    yield TradeEvent(*struct.unpack_from(TRADE_EVENT_FORMAT, payload, offset))
```

The engine produces messages where the payload is N×60 bytes (a batch of trade events). This loop unpacks each one. The format string `<QQQQQQdI` must match `MarketEvents.h` exactly — `match_id, buy_order_id, sell_order_id, t0_ns, t1_ns, t2_ns, price, qty`.

**`ContestantState.validate(event)`** — Correctness checks:

| Check | What it catches |
|---|---|
| `match_id == last_match_id + 1` | Any gap or duplicate in match sequence |
| `t2_ns >= last_t2_ns` | Clock going backwards (system clock anomaly) |
| `buy_order_id != 0, sell_order_id != 0` | Malformed order |
| `buy_order_id != sell_order_id` | Self-match (trading with yourself) |
| `t0_ns, t1_ns, t2_ns != 0` | Missing timestamps |
| `t1_ns >= t0_ns, t2_ns >= t1_ns` | Timestamp ordering violated |
| `price > 0 and isfinite(price)` | Invalid price (NaN, Inf, zero, negative) |
| `qty > 0` | Invalid quantity |

Any violation immediately **disqualifies** the contestant: sets `correctness = "CRITICAL_ERR: ..."`, writes score `-1.0` to Redis, and terminates the scoring engine. The first failure is fatal — there is no partial scoring.

**`ContestantState.accept(event)`:**
Appends `(wall_time_monotonic, engine_latency_ns)` to a `deque` and calls `prune()` which removes samples older than `window_seconds`. This is a sliding window — only the last N seconds of trades contribute to the current score.

**`ContestantState.metrics()`:**
```python
latencies = [sample[1] for sample in self.samples]  # list of latency values in ns
p99 = percentile(latencies, 0.99) / 1000            # convert to microseconds
tps = len(self.samples) / self.window_seconds        # trades per second in window
score = tps / max(p99, 0.1)                         # see Section 13
```

**Metrics emission timing:** The timer check is **outside** the `if msg is not None` guard. This means metrics are published every `metrics_interval_ms` milliseconds regardless of whether Kafka messages arrived. Without this fix, a slow bot that rarely generates trades would never emit score updates to Redis (the old bug where the leaderboard showed nothing for quiet bots).

### `telemetry/api.py`

FastAPI application serving three endpoints. The API is the intermediary between all backend systems and the browser.

**`POST /api/v1/submissions/upload`:**

1. Validates `.zip` extension.
2. Extracts ZIP to `/tmp/sandbox_uploads/<contestant_id>/` (overwrites if exists).
3. Deletes the raw ZIP file after extraction.
4. Calls `asyncio.create_task(run_detached_orchestrator())` — spawns `infra/orchestrator.py` as a detached subprocess. The API returns `{"status": "QUEUED"}` immediately without waiting for the orchestrator to finish. The orchestrator runs asynchronously (potentially for 60+ seconds).

The subprocess is launched as:
```python
process = await asyncio.create_subprocess_exec(sys.executable, "infra/orchestrator.py", "--sub-id", contestant_id)
await process.wait()
```

`await process.wait()` inside the task means the coroutine stays alive until the orchestrator exits, logging completion. The API itself is unblocked because the wait is in a background task.

**`GET /api/v1/leaderboard`:**

```python
scores = await client.zrevrange("leaderboard:scores", 0, 24, withscores=True)
for rank, (contestant_id, score) in enumerate(scores, start=1):
    meta = await client.hgetall(f"contestant:meta:{contestant_id}")
```

`ZREVRANGE` returns the sorted set members in descending score order (rank 1 = highest score). For each member, `HGETALL` fetches the full metadata hash. Returns a list of up to 25 contestants.

**`GET /api/v1/leaderboard/stream`:**

A long-lived SSE (Server-Sent Events) connection. Keeps one Redis pub/sub connection open per browser tab.

```
Browser connects → yields initial "snapshot" event → enters loop:
  - awaits pub/sub message (timeout 1s)
  - if message arrives: yields "update" event
  - if 5 seconds elapsed without message: yields "snapshot" event
  - sleeps 50ms (prevents busy-loop)
```

The two event types allow the frontend to distinguish between driven updates (a scoring event just fired) and periodic heartbeats (no activity, but still keep the display current).

**CORS:** `allow_origins=["*"]` allows the browser to call the API from any origin. This is necessary because the browser loads the frontend from `localhost:5173` and calls the API on `localhost:8000` — these are different origins, so CORS applies. In a production deployment, this should be restricted to the frontend's exact domain.

### `telemetry/Dockerfile`

```dockerfile
FROM python:3.11-slim
COPY telemetry/requirements.txt ./telemetry/requirements.txt
COPY infra/requirements.txt     ./infra/requirements.txt
RUN pip install -r telemetry/requirements.txt -r infra/requirements.txt
COPY telemetry/ ./telemetry/
COPY infra/ ./infra/
CMD ["python3", "-u", "-m", "uvicorn", "telemetry.api:app", "--host", "0.0.0.0", "--port", "8000"]
```

The same image serves two purposes in Docker Compose: the `telemetry-api` service runs the default CMD (uvicorn); the scoring engine is launched by the orchestrator as a subprocess inside the container, running `telemetry/scoring_engine.py` directly.

The `python3 -u` flag disables stdout buffering. This is essential: without it, Python buffers stdout when writing to a pipe or file, and log lines may not appear for minutes.

### `telemetry/requirements.txt`

- `fastapi`, `uvicorn[standard]`: API framework and ASGI server.
- `redis`: async Redis client (used as `redis.asyncio` in the API, synchronous in scoring engine).
- `confluent-kafka`: C-based Kafka client (significantly faster than `kafka-python` for high-throughput consumption).
- `sse-starlette`: SSE response type for FastAPI (optional, the code uses `StreamingResponse` directly).

---

## 7. Orchestration Layer

### `infra/orchestrator.py`

The lifecycle manager for a single contestant's evaluation. Spawned by the API as a subprocess; not a long-running service.

**`BenchmarkOrchestrator.connect()`:**
```python
config.load_incluster_config()  # if running inside a K8s pod
config.load_kube_config()       # fallback: uses ~/.kube/config
```
This is why the telemetry API container needs `~/.kube/config` (or Minikube's kubeconfig) available. In the Docker Compose setup, the kubeconfig is implicitly available because the orchestrator runs as a subprocess of the API, which shares the host filesystem.

**`ensure_namespace()`:** Creates the `evaluation-sandbox` K8s namespace with `pod-security.kubernetes.io/enforce: restricted` label. The "restricted" Pod Security Standard requires non-root users, read-only root filesystem, dropped capabilities, and no privilege escalation.

**`ensure_network_policy()`:** Creates a `NetworkPolicy` that allows egress from contestant pods (labeled `tier=contestant-agent`) only to port 8080/TCP, on any destination IP. All other egress is blocked. This prevents bots from exfiltrating data or calling external APIs.

**`auto_detect_and_build(unzip_dir, submission_id)`:**

Scans the extracted ZIP recursively for language indicators:
- `Cargo.toml` or `.rs` files → Rust
- `go.mod` or `.go` files → Go
- `requirements.txt`, `sandbox_sdk.py`, or `.py` files → Python
- Otherwise → C++

Generates a `Dockerfile.generated` tailored for the detected language and calls `docker build -f Dockerfile.generated -t contestant-agent:<sub_id> <unzip_dir>`.

For Python submissions, the orchestrator locates `sandbox_sdk.py` within the ZIP tree and uses it as the entrypoint:
```dockerfile
ENTRYPOINT ["python", "sandbox_sdk.py"]
```
If `sandbox_sdk.py` is nested inside a subdirectory of the ZIP, the relative path is computed and used directly.

**`start_scoring_engine(contestant_id, duration)`:**

```python
cmd = [sys.executable, "-u", "telemetry/scoring_engine.py",
    "--kafka-brokers", kafka_brokers,
    "--group-id", f"scoring-{contestant_id}-{timestamp}",
    "--offset-reset", "latest",
    "--contestant-id", contestant_id,
    "--redis-url", redis_url,
    "--window-seconds", "10",
    "--metrics-interval-ms", "1000"]
return subprocess.Popen(cmd)
```

Started before the contestant pod to minimize the window between bot connection and score tracking. Terminated (SIGTERM → SIGKILL with 10s timeout) after the evaluation completes.

**`spawn_contestant_sandbox_pod(image_tag, duration)`:**

```python
target_host = os.getenv("EXCHANGE_HOST", "10.0.2.2")
env=[
    V1EnvVar("EXCHANGE_GATEWAY_URL", f"ws://{target_host}:8080"),
    V1EnvVar("CONTESTANT_ID", image_tag.split(":")[-1])
]
```

The pod spec enforces all "restricted" Pod Security Standard requirements:
- `runAsNonRoot: true`, `runAsUser: 10002`
- `allowPrivilegeEscalation: false`
- `capabilities: drop: [ALL]`
- `seccompProfile: RuntimeDefault`
- `restartPolicy: Never` (it runs once and exits)

`delete_old_sandbox_pod(wait=True)` before creating ensures no leftover pod from a previous run. It polls `read_namespaced_pod` until the pod is fully gone (404 status).

**`wait_for_agent_completion(timeout)`:**

Polls `read_namespaced_pod_status` every second. Returns when `phase in ("Succeeded", "Failed")`. On success, fetches the last 50 lines of the pod's stdout log. On timeout, raises `TimeoutError` after fetching diagnostic logs.

**`run()` method — the full evaluation lifecycle:**

```
1. connect()                          # load kubeconfig
2. ensure_namespace()                 # create namespace if needed
3. ensure_network_policy()            # enforce egress restrictions
4. auto_detect_and_build()            # docker build → contestant-agent image
5. start_scoring_engine()             # spawn scoring process (Popen)
6. spawn_contestant_sandbox_pod()     # kubectl apply equivalent
7. wait_for_agent_completion()        # block until pod exits
8. [finally] terminate scoring engine
9. [finally] cleanup() → delete pod
```

### `infra/requirements.txt`

`kubernetes`: the official Python Kubernetes client. Provides the `client`, `config`, and API group classes used throughout the orchestrator.

### `infra/k8s/`

Static Kubernetes YAML manifests — a reference implementation of what the orchestrator creates dynamically via the Python API. These can be applied with `kubectl apply -k infra/k8s` for debugging or manual deployments.

- `namespace.yaml`: `evaluation-sandbox` namespace with restricted Pod Security labels.
- `network-policy.yaml`: egress restriction for `tier=contestant-agent` pods.
- `exchange-deployment.yaml`: the matching engine as a K8s Deployment (replicas=1, CPU/memory requests, security context).
- `exchange-service.yaml`: ClusterIP service exposing port 8080.
- `kustomization.yaml`: lists all resources for `kubectl apply -k`.

---

## 8. Frontend Dashboard

### `leaderboard/src/App.jsx`

The single React component that is the entire frontend. Key subsystems:

**State:**
```javascript
const [rows, setRows]               // leaderboard data
const [connected, setConnected]     // SSE connection status
const [file, setFile]               // selected ZIP file
const [contestantId, setContestantId] // name input
const [uploading, setUploading]     // upload in flight
const [uploadStatus, setUploadStatus] // success/error message
const [isDark, setIsDark]           // theme toggle
```

**SSE connection (`useEffect`):**

```javascript
const source = new EventSource(`${API_URL}/api/v1/leaderboard/stream`);
source.addEventListener("snapshot", onRows);
source.addEventListener("update", onRows);
source.onerror = () => setConnected(false);
```

`EventSource` is the browser's native SSE client. It automatically reconnects if the connection drops. The `snapshot` and `update` event types map to the two `event:` lines the API produces. Both call the same `onRows` handler which calls `setRows()`, triggering a re-render.

On mount, `loadSnapshot()` also calls `GET /api/v1/leaderboard` directly to populate the table immediately (before the SSE stream fires its first event).

**Upload flow:**

```javascript
const formData = new FormData();
formData.append("file", file);
formData.append("contestant_id", trimmedId);
fetch(`${API_URL}/api/v1/submissions/upload`, { method: "POST", body: formData })
```

A `multipart/form-data` POST. The FastAPI endpoint reads `file: UploadFile` and `contestant_id: str = Form(...)`. The `trimmedId` is validated client-side to be non-empty before submission.

**Scoring display:**

The leaderboard table columns: Rank, Contestant ID, Score, TPS, P50/P90/P99 latency, Correctness, Total fills, Last match ID.

Two Recharts visualizations:
- `BarChart`: score by contestant (top 10).
- `LineChart`: p99 latency profile (top 10).

`useMemo` on `totals` recomputes aggregate stats only when `rows` changes, avoiding redundant calculations on unrelated state updates.

### `leaderboard/src/main.jsx`

The React entry point. Mounts `<App />` into `<div id="root">` using `ReactDOM.createRoot`. Imports `styles.css` globally.

### `leaderboard/src/styles.css`

Uses CSS custom properties (`--text-color`, `--bg-color`, `--border-color`) that change based on the `data-theme` attribute on `<html>`. The `App.jsx` theme toggle sets `document.documentElement.setAttribute("data-theme", "dark" | "light")`, which causes CSS variable overrides to cascade through all components without any component-level style changes.

### `leaderboard/index.html` / `vite.config.js`

`index.html` is Vite's entry HTML. Vite injects a `<script type="module">` tag pointing to `src/main.jsx` during development (HMR) and replaces it with hashed bundle filenames on `npm run build`.

`vite.config.js` is minimal — just `plugins: [react()]` to enable JSX transformation.

### `leaderboard/Dockerfile`

```dockerfile
FROM node:20-alpine
COPY package*.json ./
RUN npm install
COPY . .
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "5173"]
```

Runs the Vite **development** server (not a production build). The dev server enables HMR and serves source files with sourcemaps. `--host 0.0.0.0` binds to all interfaces so the container's port 5173 is reachable from the host.

`COPY package*.json ./` before `COPY . .` is a layer caching optimization: the `npm install` layer is only invalidated when `package.json` or `package-lock.json` changes, not when source files change.

---

## 9. Infrastructure and Configuration

### `docker-compose.yml`

Defines six services with explicit startup ordering via `depends_on` + `condition: service_healthy`.

```
zookeeper (health: nc -z localhost 2181)
    └── kafka (health: kafka-topics --list) — depends on zookeeper
            └── matching-engine — depends on kafka
            └── scoring engines (started by orchestrator, not compose)
redis (health: redis-cli ping)
    └── telemetry-api — depends on redis
leaderboard — depends on telemetry-api (no healthcheck)
```

**Kafka dual-listener setup:**
```yaml
KAFKA_LISTENERS:           INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
```

`INTERNAL://kafka:29092` is used for container-to-container communication. Docker Compose puts all services on a shared bridge network where `kafka` resolves to the Kafka container's IP. The matching engine sets `KAFKA_BROKERS=kafka:29092`.

`EXTERNAL://localhost:9092` is used for host-side access (the `ports: - "9092:9092"` mapping). This allows tools on the host (`kafka-console-consumer`, Python scripts, etc.) to connect.

Without two listeners, containers would try to connect using `localhost:9092` (the external address), which from inside a container resolves to the container itself, not the Kafka container.

**Build context:** `matching-engine` and `telemetry-api` use `context: .` (the repo root) so their Dockerfiles can `COPY` from sibling directories. `leaderboard` uses `context: leaderboard/` since it only needs files from within that directory.

**`restart: on-failure`:** If the matching engine crashes (e.g., a bug causes a segfault), Docker Compose automatically restarts it. This is important because contestants might submit malformed orders that trigger edge cases.

### `.dockerignore`

Prevents large or sensitive directories from being sent as Docker build context. Without this, `docker build` would send the entire repo (including `node_modules`, `.git`, etc.) to the Docker daemon as build context, even if the Dockerfile doesn't need them. This would be slow and waste memory.

Key exclusions:
- `.git/`, `**/__pycache__/`: version control and bytecode, never needed in images.
- `matching-engine/engine*`, `matching-engine/*.o`: build artifacts; the Dockerfile recompiles from source.
- `node_modules/`, `leaderboard/dist/`: npm artifacts; the Dockerfile runs `npm install`.
- `submission_test/`, `BUILD_RUN_REPORT.md`: dev/debug artifacts.

### `.gitignore`

Prevents build artifacts from being committed. Key patterns:
- `*.o`, `*.debug`, `matching-engine/engine`: C++ build outputs.
- `__pycache__/`, `*.pyc`: Python bytecode.
- `leaderboard/node_modules/`, `leaderboard/dist/`: npm artifacts.
- `node_modules/`: root-level spurious npm dir.
- `submission_test/`, `BUILD_RUN_REPORT.md`, `sample_bot.zip`: dev artifacts.
- `.agents/`, `.codex/`: AI tool workspace directories.

---

## 10. Technology Deep-Dives

### Linux epoll and Non-Blocking I/O

`epoll` is the Linux kernel's scalable I/O event notification system. It is the foundation that allows `TCPServer` to handle many simultaneous bot connections on a single thread.

**Why not `select` or `poll`?** Both `select` and `poll` require passing the entire set of file descriptors to the kernel on every call, and the kernel must scan all of them. At 170 bots, `poll` scans 170 fds on every iteration. `epoll` maintains an internal kernel data structure (a red-black tree of registered fds); `epoll_wait` only returns the fds that are actually ready. Performance scales as O(active events), not O(registered fds).

**Edge-triggered vs. level-triggered:**
- Level-triggered (default): `epoll_wait` returns the fd as ready as long as data is available. If you read 100 bytes but 200 are buffered, the next `epoll_wait` immediately returns the fd again.
- Edge-triggered (`EPOLLET`): `epoll_wait` returns the fd only when new data arrives (on the rising edge of the buffer level). After that, you must read until `EAGAIN` to consume all buffered data, or you'll miss the next notification.

The client fds use `EPOLLET`. This is more efficient (fewer epoll_wait wakeups) but requires the drain-until-EAGAIN pattern in `handleClientData`. The server_fd uses level-triggered (no `EPOLLET` flag) which is safer for `accept()`.

**Non-blocking mode (`O_NONBLOCK`):** Without this, `read()` would block if no data is available, and `accept()` would block if no connections are pending. With non-blocking mode, these calls return `-1` with `errno = EAGAIN` immediately when there's nothing to read/accept. This allows a single thread to handle many connections without ever blocking.

### WebSocket Protocol (RFC 6455)

WebSocket is a protocol that begins as HTTP and upgrades to a full-duplex binary channel.

**Why WebSocket instead of raw TCP?** Two reasons:
1. The Python `websockets` library (used by the SDK) handles connection management, reconnection, and frame framing automatically.
2. WebSocket runs over port 80/443 — it passes through firewalls and proxies that would block raw TCP connections on custom ports.

**Upgrade handshake:**

```
Client →  GET /orders HTTP/1.1
          Upgrade: websocket
          Sec-WebSocket-Key: <random 16 bytes, base64-encoded>

Server →  HTTP/1.1 101 Switching Protocols
          Upgrade: websocket
          Sec-WebSocket-Accept: <SHA1(key + GUID), base64-encoded>
```

The `Sec-WebSocket-Accept` header proves the server understands the WebSocket protocol. The GUID `"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"` is hardcoded in RFC 6455 and is concatenated with the client's key before hashing. An HTTP proxy that doesn't understand WebSocket would fail to produce the correct accept value, preventing accidental proxying.

**Frame format (client to server):**

```
Byte 0: 1000 0010  →  FIN=1, RSV=000, opcode=0010 (binary)
Byte 1: 1xxx xxxx  →  MASK=1, payload_len=xxx (0–125), or 126/127 for extended
Bytes 2–5:  4-byte masking key (random, per-frame)
Payload: each byte XOR'd with mask[i % 4]
```

The masking requirement for client frames (not required for server frames) exists to prevent cache poisoning attacks. Without masking, a malicious page could send WebSocket traffic that looks like HTTP requests to a proxy, poisoning the proxy's cache.

### Lock-Free Queues and Cache Line Alignment

Modern CPUs operate on cache lines of 64 bytes. When two threads access different variables that happen to share a cache line, each write by one thread invalidates the cache line in all other cores. The other cores must then refetch the entire cache line from L3 or DRAM, even though they only needed one variable in it. This is **false sharing**.

The MPSC and SPSC queues use `alignas(64)` to place producer and consumer cursors on separate cache lines:

```cpp
alignas(64) std::atomic<size_t> enqueue_pos_;  // cache line 1
alignas(64) std::atomic<size_t> dequeue_pos_;  // cache line 2
```

Without alignment, both positions could share one 64-byte cache line. Every time the producer advances `enqueue_pos_`, the consumer's core would see its cache line as stale and stall to re-read it — even though the consumer only cares about `dequeue_pos_`. With alignment, the producer and consumer never compete for the same cache line.

**Memory ordering:** `std::memory_order_release` on a store ensures that all preceding writes are visible to threads that subsequently do `memory_order_acquire` on the same atomic. In the SPSC queue:
```cpp
buffer_[head & mask_] = value;       // write data
head_.store(next, memory_order_release); // fence: data visible before head update
```
The consumer:
```cpp
if (tail == head_.load(memory_order_acquire)) return false; // sees data after head
value = buffer_[tail & mask_];       // safe to read
```
Without these fences, the compiler or CPU could reorder the head increment before the data write, causing the consumer to read uninitialized data.

### CPU Pinning with `pthread_setaffinity_np`

```cpp
cpu_set_t cpuset;
CPU_ZERO(&cpuset);
CPU_SET(1, &cpuset);
pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
```

This pins the calling thread to CPU core 1 exclusively. The OS scheduler will never migrate this thread to another core.

**Why pin?** Cache locality. If the matching loop runs on core 1, its working set (the order book, local histograms, the MPSC queue's consumer side) stays in core 1's L1/L2 cache. Every time the OS migrates the thread to another core, the new core starts with a cold cache and must refetch everything from L3 or DRAM. A cache miss to L3 costs ~40 cycles; a cache miss to DRAM costs ~200+ cycles.

**Thread assignments:**
- Core 1: `processLoop()` — the hot path, matching every single order
- Core 2: `TCPServer::run()` — the network I/O loop
- Core 3: `KafkaOffloader::run()` — async Kafka publishing

Core 0 is intentionally left free for the OS, the metrics thread, and other system activity. Pinning all threads to cores 1–3 ensures that OS jitter (scheduler preemptions, interrupt handlers) happens on core 0 and does not disrupt the matching loop.

### Apache Kafka

Kafka is a distributed log. Messages are appended to a topic, stored durably on disk, and delivered to consumers in order. Consumers track their position (offset) independently, allowing multiple consumer groups to read the same data.

**Why Kafka instead of a direct Redis write?**

The matching engine is C++ with minimal dependencies. Writing directly to Redis from C++ would require a Redis client library and add latency to the hot path. Kafka provides a durable buffer: even if the scoring engine is temporarily down or slow, trade events are not lost — they accumulate in the Kafka log until the consumer catches up. The SPSC queue inside `KafkaOffloader` provides a second buffer (65536 events × 60 bytes = ~4MB) between the matching loop and the Kafka producer.

**Batching:** `librdkafka` with `linger.ms=1` waits up to 1ms to accumulate messages before sending. The `KafkaOffloader` also drains up to 1024 events per iteration of its loop, passing them all in one `produce()` call. The result is that Kafka messages often contain dozens of `TradeEvent` structs, reducing per-message Kafka overhead.

**Single partition:** `PARTITION_UA` lets Kafka choose. With `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` and the default of 1 partition, all messages go to partition 0, preserving strict ordering. The scoring engine consumes from partition 0 and sees events in the exact order they were produced.

**Retention:** Default Kafka retention is 7 days or 1GB. Old trade events from previous evaluation runs stay in the log. The `--offset-reset latest` on the scoring engine ensures it ignores these historical events and only processes trades from the current session.

### Redis Sorted Sets and Pub/Sub

Redis serves two roles: persistent score storage and real-time update broadcast.

**Sorted set (`ZADD`, `ZREVRANGE`):**
```
ZADD leaderboard:scores <score> <contestant_id>
ZREVRANGE leaderboard:scores 0 24 WITHSCORES
```
A sorted set automatically maintains members in sorted order by score. `ZADD` with the same `contestant_id` updates the score in-place. `ZREVRANGE` returns the top 25 members in descending score order (highest score = rank 1). Queries are O(log N + K) where K is the number of returned elements — effectively instant for 25 elements.

**Hash (`HSET`, `HGETALL`):**
```
HSET contestant:meta:<id>  tps 1234.5  p50_lat_us 45.2  ...
HGETALL contestant:meta:<id>
```
The sorted set stores only the numeric score (used for ranking). The hash stores rich metadata (TPS, latency percentiles, correctness, total events). The API joins these two structures per contestant to build the full leaderboard row.

**Pub/Sub (`PUBLISH`, `SUBSCRIBE`):**
```
PUBLISH leaderboard_updates <json_payload>
```
The scoring engine publishes a JSON message every time it updates the leaderboard. The API maintains one pub/sub subscriber per connected SSE client. When a message arrives on the channel, the API reads the full leaderboard from the sorted set + hashes and emits it as an SSE event. This is a fan-out pattern: one publish → one read from Redis → N SSE events (one per connected browser tab).

### FastAPI and Server-Sent Events

FastAPI is an ASGI (Asynchronous Server Gateway Interface) framework built on Starlette. Uvicorn is the ASGI server (using uvloop internally on Linux for faster async I/O).

**Why FastAPI over Flask?** FastAPI's async support is native: route handlers can be `async def` and `await` other coroutines without threads. A Flask route that awaits a Redis operation would block the entire process during the wait; a FastAPI route yields control to other requests while awaiting.

**SSE vs. WebSocket for the leaderboard:** SSE is unidirectional (server → browser only), which is all the leaderboard needs. SSE is simpler than WebSocket: no handshake, auto-reconnect built into `EventSource`, and works over standard HTTP/2. The SSE wire format is plain text:
```
event: update
data: {"rank":1,"id":"alice",...}

```
(blank line terminates the event)

**The streaming generator:** `StreamingResponse(events(), media_type="text/event-stream")` wraps an async generator function. FastAPI's Starlette layer iterates the generator and flushes each yielded chunk to the client. Because the generator uses `await pubsub.get_message(timeout=1.0)`, it yields control to other requests during the wait — thousands of concurrent SSE clients can coexist on a single uvicorn worker thread.

### Kubernetes Sandbox Isolation

The contestant's code runs in an environment they cannot escape.

**Namespace isolation:** The `evaluation-sandbox` namespace scopes all resources (pods, services, network policies). Resources in other namespaces are invisible to pods in this namespace by default.

**NetworkPolicy enforcement:** The `NetworkPolicy` applied to pods labeled `tier=contestant-agent` is the primary security boundary:
```yaml
spec:
  podSelector:
    matchLabels:
      tier: contestant-agent
  policyTypes: [Egress]
  egress:
  - ports:
    - port: 8080
      protocol: TCP
```
This policy allows **only** TCP egress to port 8080. Any connection attempt to any other IP:port (internet APIs, internal services, cloud metadata endpoints) is dropped by the kernel. A malicious bot cannot exfiltrate data, communicate with a C2 server, or mine cryptocurrency.

**Pod Security Standard ("restricted"):**
- `runAsNonRoot: true`: the container process cannot run as UID 0.
- `allowPrivilegeEscalation: false`: prevents `setuid` binaries from gaining root.
- `capabilities: drop: [ALL]`: removes all Linux capabilities (no `CAP_NET_RAW`, `CAP_SYS_ADMIN`, etc.).
- `readOnlyRootFilesystem: true` (on engine pods): the container cannot write to its own filesystem.
- `seccompProfile: RuntimeDefault`: restricts available syscalls to a curated safe set.
- `automountServiceAccountToken: false`: no Kubernetes API access from inside the pod.

**`restartPolicy: Never`:** Contestants get exactly one run. If their bot crashes, it stays down. The orchestrator's `wait_for_agent_completion` detects the `Failed` phase and records the pod logs for debugging.

### React, Vite, and SSE Consumption

React's component model means state changes (`setRows()`) trigger a reconciliation diff and minimal DOM updates. When a new leaderboard snapshot arrives, only the table cells whose values changed are re-rendered.

`useMemo` on `totals` and `chartRows` ensures that these derived values (which require iterating `rows`) are only recomputed when `rows` changes, not on every render triggered by unrelated state (e.g., `isDark` toggle).

Vite's dev server provides HMR (Hot Module Replacement): when `App.jsx` is saved, only the changed module is re-evaluated in the browser without a full page reload. Component state is preserved across hot updates. This is valuable during development of the leaderboard UI.

`recharts` renders SVG-based charts. `ResponsiveContainer` measures the parent div's width and passes it to the chart as a prop, making charts fluid without manual size management.

---

## 11. Wire Format Reference

### Inbound: `OrderMessage` (bot → engine)

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
├───────────────────────────────────────────────────────────────────┤
│                       order_id (uint64)                           │  bytes 0–7
│                       (little-endian)                             │
├───────────────────────────────────────────────────────────────────┤
│                     timestamp_ns (uint64)                         │  bytes 8–15
│                  bot generation time, nanoseconds                 │
├───────────────────────────────────────────────────────────────────┤
│                       price (float64)                             │  bytes 16–23
│                    IEEE 754 double, little-endian                 │
├───────────────────────────────────────────────────────────────────┤
│                qty (uint32)              │  side  │  padding[3]  │  bytes 24–31
│               (little-endian)           │  (u8)  │  (zeros)     │
└───────────────────────────────────────────────────────────────────┘
Total: 32 bytes
struct.pack format: "<QQdIB3x"
```

`side`: `1` = Buy, `2` = Sell (numeric). Also accepts ASCII `'B'`/`'b'` (66/98) and `'S'`/`'s'` (83/115) — the engine does `side == 'B' || side == 'b' || side == 1`.

### Outbound: `TradeEvent` (engine → Kafka)

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
├───────────────────────────────────────────────────────────────────┤
│                      match_id (uint64)                            │  bytes 0–7
│              global monotonic match counter                       │
├───────────────────────────────────────────────────────────────────┤
│                   buy_order_id (uint64)                           │  bytes 8–15
├───────────────────────────────────────────────────────────────────┤
│                  sell_order_id (uint64)                           │  bytes 16–23
├───────────────────────────────────────────────────────────────────┤
│                      t0_ns (uint64)                               │  bytes 24–31
│               taker's bot-generation timestamp                    │
├───────────────────────────────────────────────────────────────────┤
│                      t1_ns (uint64)                               │  bytes 32–39
│             taker's gateway-ingest timestamp                      │
├───────────────────────────────────────────────────────────────────┤
│                      t2_ns (uint64)                               │  bytes 40–47
│             engine match-completion timestamp                     │
├───────────────────────────────────────────────────────────────────┤
│                      price (float64)                              │  bytes 48–55
│              execution price (maker's limit price)                │
├───────────────────────────────────────────────────────────────────┤
│                qty (uint32)              │  (struct end)          │  bytes 56–59
└───────────────────────────────────────────────────────────────────┘
Total: 60 bytes (packed, no padding — __attribute__((packed)))
struct.unpack format: "<QQQQQQdI"
```

Kafka messages may contain multiple `TradeEvent` structs concatenated: a 120-byte Kafka message value contains exactly 2 trade events.

---

## 12. Timestamp and Latency Model

Three timestamps are recorded for every executed trade, each from a different point in the system:

```
t0  ──────────────────────────────────────────────────────────────────
    Bot calls time.time_ns() in submit_order() before packing the frame.
    Represents the moment the trading strategy decided to submit an order.
    Written into the OrderMessage.timestamp_ns field.

                    [WebSocket frame travels over TCP/network]

t1  ──────────────────────────────────────────────────────────────────
    TCPServer records system_clock::now() immediately after
    unmasking the WebSocket frame and before calling enqueueOrder().
    Represents the moment the engine's network layer received the order.
    Written into Order.t1.

                    [Order sits in MPSC queue until processLoop dequeues it]

t2  ──────────────────────────────────────────────────────────────────
    MatchingEngine records system_clock::now() inside matching()
    immediately after the match executes.
    Represents the moment the trade was executed.
    Written into TradeEvent.t2_ns.
```

**Derived latencies:**

| Metric | Formula | What it measures |
|---|---|---|
| Network latency | `t1 - t0` | Time for the frame to travel from the bot to the engine gateway |
| Engine latency | `t2 - t1` | Time from gateway receipt to match execution (includes MPSC queue wait) |
| Total latency | `t2 - t0` | End-to-end order lifecycle time |

**Important:** `t0` and `t2` are recorded on the bot's machine and the engine's machine respectively. If these clocks are not synchronized (e.g., different VMs with different system clocks), the computed "network latency" will be meaningless or even negative. In a local setup (all on the same host), both clocks use `CLOCK_REALTIME` and should be consistent. In a distributed deployment, NTP or PTP synchronization is required for `t0`–`t1` to be meaningful.

**Scoring uses `t2 - t1` (engine latency) only.** This is the latency from gateway receipt to match completion — entirely within the engine's control and immune to bot-side clock differences. The `t2 - t0` total latency is recorded in the TradeEvent for diagnostic purposes but not used in the score formula.

---

## 13. Scoring Formula

The score rewards **throughput** (TPS) while penalizing **high latency** (p99):

```
score = TPS / max(p99_latency_us, 0.1)
```

Where:
- `TPS` = trades per second in the rolling window (default 10 seconds)
- `p99_latency_us` = 99th percentile engine latency in microseconds (t2 - t1, converted from ns)
- `max(..., 0.1)` prevents division by zero if p99 is extremely small (sub-100ns matches)

**Interpretation:**

| Scenario | Score |
|---|---|
| 1000 TPS, p99 = 100µs | 1000 / 100 = **10.0** |
| 1000 TPS, p99 = 10µs | 1000 / 10 = **100.0** |
| 100 TPS, p99 = 1µs | 100 / 1 = **100.0** |
| 100 TPS, p99 = 100µs | 100 / 100 = **1.0** |

A strategy that submits many orders (high TPS) but generates trades at a slow rate (orders cross the spread infrequently) will score lower than one that submits fewer but better-targeted orders. Latency comes from the MPSC queue depth — if the matching engine is backlogged, `t2 - t1` grows, and the score drops.

**Disqualification:** Any correctness violation sets `score = -1.0`. A score of -1 appears at the bottom of the leaderboard regardless of TPS. This discourages intentionally malformed submissions.

---

## 14. Thread and Process Map

```
Host Machine

Docker Compose:
├── Container: zookeeper
│   └── Main thread: ZooKeeper server
│
├── Container: kafka
│   └── Main thread: Kafka broker
│
├── Container: redis
│   └── Main thread: Redis event loop
│
├── Container: matching-engine
│   └── Process: engine
│       ├── Main thread (Core 0): infinite sleep loop
│       ├── Thread (Core 1): MatchingEngine::processLoop()
│       │     - dequeues Orders from MPSCQueue
│       │     - calls addOrder() → matching()
│       │     - records t2 timestamps
│       │     - accumulates latency histograms
│       ├── Thread (Core 2): TCPServer::run()
│       │     - epoll_wait() for I/O events
│       │     - accept() new connections
│       │     - handleClientData() → websocket frame parsing
│       │     - records t1 timestamps
│       │     - enqueueOrder() into MPSCQueue
│       ├── Thread (Core 2, same as TCPServer): MatchingEngine::metricsLoop()
│       │     [NOTE: should ideally be a different core, currently conflicts with TCPServer]
│       └── Thread (Core 3): KafkaOffloader::run()
│             - drains TradeEvents from SPSCQueue
│             - publishes to Kafka via librdkafka
│
├── Container: telemetry-api
│   └── Process: uvicorn (1 worker by default)
│       └── asyncio event loop:
│           ├── HTTP request handlers (coroutines)
│           ├── SSE streaming generators (coroutines)
│           └── Background task: orchestrator subprocess + wait
│
└── Container: leaderboard
    └── Process: Vite dev server (Node.js)
        └── HTTP server + WebSocket HMR server

Minikube Cluster (separate VM/container):
└── Namespace: evaluation-sandbox
    └── Pod: active-contestant-sandbox
        └── Container: contestant-agent
            └── Process: python3 sandbox_sdk.py
                └── asyncio event loop:
                    ├── WebSocket connection to ws://EXCHANGE_HOST:8080
                    └── 1-second tick loop → on_market_update() → submit_order()

Host Process (spawned by telemetry-api container):
└── Process: python3 infra/orchestrator.py --sub-id <id>
    ├── Talks to Minikube API server (kubectl equivalent)
    ├── Calls docker build (Minikube's daemon via eval $(minikube docker-env))
    └── Subprocess: python3 telemetry/scoring_engine.py
        └── Kafka consumer loop:
            ├── polls "trade-events" topic
            ├── validates and scores TradeEvents
            └── writes to Redis (ZADD + HSET + PUBLISH)
```

This map shows the full concurrency picture: the matching engine achieves low latency by dedicating CPU cores to specific tasks, while the Python layer achieves concurrency through asyncio coroutines rather than threads.
