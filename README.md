# High-Performance Matching Engine

A C++ implementation of a price-time priority order matching engine inspired by modern electronic exchanges and quantitative trading infrastructure.

## Features

* Buy and sell order books using maps and lists
* Price-time priority matching
* Partial order fills
* Trade history recording
* Sequence-number based order precedence
* Maker/taker style execution pricing
* Memory Pool and Multiple Producer Single Consumer(MPSC) queue added for minimal overhead


## Example

```cpp
engine.addOrder(Order(1, 100, true, 105));
engine.addOrder(Order(2, 30, false, 104));
```

Output:

```text
BUY LOT 1 SELL LOT 2 QTY 30 PRICE 105
```

## Further Scope

* TCP trading gateway(in progress)
* Multi-client support
* Bot fleet simulation(in progress)
* Telemetry and benchmarking
* Dockerized deployment
* Submission and evaluation framework


## Tech Stack
Most of the layers yet to be updated
| Layer | Technology | Reason |
| :--- | :--- | :--- |
| **Exchange Core** | C++23 | Zero-overhead, direct memory control. |
| **External Protocol** | WebSocket | Required by spec, standardized, low overhead. |
| **Internal Messaging** | Kafka | Decouples load generation from telemetry; allows micro-batching. |
| **Container Runtime** | Docker + Kubernetes | Strict sandboxing, CPU pinning, PID limits. |
| **Metric Storage** | TimescaleDB | Time-series queries on raw telemetry data. |
| **Live State** | Redis ZSET + Pub/Sub | Sub-millisecond leaderboard reads, push updates. |
| **Telemetry Backend** | Python / FastAPI | WebSocket support, async I/O, Kafka consumer. |
| **Leaderboard UI** | React | Component-based live analytics. |
| **Orchestration** | Python | Kubernetes API, bot lifecycle management. |
| **Build System** | CMake | Standard C++ build tooling. |
| **IaC** | Kubernetes manifests | Reproducible, scalable cloud deployment. |

## Goal

Foundation for a distributed trading infrastructure benchmarking platform capable of evaluating exchange implementations under high-concurrency workloads.
