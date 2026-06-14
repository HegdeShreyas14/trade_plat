#!/usr/bin/env python3
import argparse
import json
import math
import os
import signal
import struct
import time
from collections import deque
from dataclasses import dataclass

try:
    from confluent_kafka import Consumer, KafkaException
except ImportError:  # pragma: no cover
    Consumer = None
    KafkaException = RuntimeError

try:
    import redis
except ImportError:  # pragma: no cover
    redis = None


TRADE_EVENT_FORMAT = "<QQQQQQdI"
TRADE_EVENT_SIZE = struct.calcsize(TRADE_EVENT_FORMAT)


@dataclass(frozen=True)
class TradeEvent:
    match_id: int
    buy_order_id: int
    sell_order_id: int
    t0_ns: int
    t1_ns: int
    t2_ns: int
    price: float
    qty: int

    @property
    def engine_latency_ns(self):
        return max(0, self.t2_ns - self.t1_ns)


class ContestantState:
    def __init__(self, window_seconds):
        self.last_match_id = None
        self.last_t2_ns = 0
        self.order_fills = {}
        self.window_seconds = window_seconds
        self.samples = deque()
        self.correctness = "PASSED"
        self.total_events = 0

    def validate(self, event):
        if self.last_match_id is not None and event.match_id != self.last_match_id + 1:
            return f"CRITICAL_ERR: MATCH_SEQUENCE expected={self.last_match_id + 1} got={event.match_id}"
        if event.t2_ns < self.last_t2_ns:
            return "CRITICAL_ERR: T2_RETROGRADE"
        if event.buy_order_id == 0 or event.sell_order_id == 0:
            return "CRITICAL_ERR: ZERO_ORDER_ID"
        if event.buy_order_id == event.sell_order_id:
            return "CRITICAL_ERR: SELF_MATCH"
        if event.t0_ns == 0 or event.t1_ns == 0 or event.t2_ns == 0:
            return "CRITICAL_ERR: MISSING_TIMESTAMP"
        if event.t1_ns < event.t0_ns or event.t2_ns < event.t1_ns:
            return "CRITICAL_ERR: TIMESTAMP_ORDER"
        if not math.isfinite(event.price) or event.price <= 0:
            return "CRITICAL_ERR: INVALID_PRICE"
        if event.qty <= 0:
            return "CRITICAL_ERR: INVALID_QTY"
        return None

    def accept(self, event):
        now = time.monotonic()
        self.last_match_id = event.match_id
        self.last_t2_ns = event.t2_ns
        self.order_fills[event.buy_order_id] = self.order_fills.get(event.buy_order_id, 0) + event.qty
        self.order_fills[event.sell_order_id] = self.order_fills.get(event.sell_order_id, 0) + event.qty
        self.samples.append((now, event.engine_latency_ns))
        self.total_events += 1
        self.prune(now)

    def prune(self, now=None):
        if now is None:
            now = time.monotonic()
        cutoff = now - self.window_seconds
        while self.samples and self.samples[0][0] < cutoff:
            self.samples.popleft()

    def metrics(self):
        self.prune()
        latencies = [sample[1] for sample in self.samples]
        p50 = percentile(latencies, 0.50) / 1000 if latencies else 0
        p90 = percentile(latencies, 0.90) / 1000 if latencies else 0
        p99 = percentile(latencies, 0.99) / 1000 if latencies else 0
        tps = len(self.samples) / self.window_seconds if self.window_seconds > 0 else 0
        score = 0 if self.correctness != "PASSED" else tps / max(p99, 0.1)
        return {
            "tps": round(tps, 2),
            "p50_lat_us": round(p50, 2),
            "p90_lat_us": round(p90, 2),
            "p99_lat_us": round(p99, 2),
            "score": round(score, 4),
            "correctness": self.correctness,
            "total_events": self.total_events,
            "tracked_orders": len(self.order_fills),
            "last_match_id": self.last_match_id or 0,
        }


def percentile(values, p):
    if not values:
        return 0
    ordered = sorted(values)
    idx = int((len(ordered) - 1) * p)
    return ordered[idx]


def unpack_trade_events(payload):
    if len(payload) % TRADE_EVENT_SIZE != 0:
        raise ValueError(f"payload size {len(payload)} is not a multiple of {TRADE_EVENT_SIZE}")
    for offset in range(0, len(payload), TRADE_EVENT_SIZE):
        yield TradeEvent(*struct.unpack_from(TRADE_EVENT_FORMAT, payload, offset))


def make_consumer(args):
    if Consumer is None:
        raise RuntimeError("confluent-kafka is not installed. Run: pip install -r telemetry/requirements.txt")
    consumer = Consumer(
        {
            "bootstrap.servers": args.kafka_brokers,
            "group.id": args.group_id,
            "auto.offset.reset": args.offset_reset,
            "enable.auto.commit": "true",
            "fetch.wait.max.ms": "1",
            "queued.min.messages": "100000",
        }
    )
    consumer.subscribe([args.topic])
    return consumer


def publish_metrics(redis_client, contestant_id, metrics, channel, leaderboard_key):
    redis_client.zadd(leaderboard_key, {contestant_id: metrics["score"]})
    redis_client.hset(f"contestant:meta:{contestant_id}", mapping=metrics)
    payload = {"id": contestant_id, **metrics, "updated_at_ns": time.time_ns()}
    redis_client.publish(channel, json.dumps(payload, separators=(",", ":")))


def disqualify(redis_client, contestant_id, state, reason, channel, leaderboard_key):
    state.correctness = reason
    metrics = state.metrics()
    metrics["score"] = -1.0
    publish_metrics(redis_client, contestant_id, metrics, channel, leaderboard_key)
    return metrics


def run_self_test():
    now = time.time_ns()
    state = ContestantState(window_seconds=1.0)
    events = [
        TradeEvent(1, 100, 200, now - 5000, now - 2000, now, 10.0, 5),
        TradeEvent(2, 100, 201, now - 5000, now - 2000, now + 1000, 10.0, 3),
    ]
    for event in events:
        violation = state.validate(event)
        if violation:
            raise AssertionError(violation)
        state.accept(event)

    bad = TradeEvent(4, 100, 201, now - 5000, now - 2000, now + 2000, 10.0, 1)
    assert state.validate(bad).startswith("CRITICAL_ERR: MATCH_SEQUENCE")
    print(json.dumps({"passed": True, "metrics": state.metrics()}, indent=2))


def parse_args():
    parser = argparse.ArgumentParser(description="Hybrid validation and scoring worker")
    parser.add_argument("--kafka-brokers", default=os.getenv("KAFKA_BROKERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "trade-events"))
    parser.add_argument("--group-id", default=os.getenv("SCORING_GROUP_ID", "scoring-engine"))
    parser.add_argument("--offset-reset", default=os.getenv("KAFKA_OFFSET_RESET", "earliest"))
    parser.add_argument("--contestant-id", default=os.getenv("CONTESTANT_ID", "default"))
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--leaderboard-key", default=os.getenv("REDIS_LEADERBOARD_KEY", "leaderboard:scores"))
    parser.add_argument("--channel", default=os.getenv("REDIS_CHANNEL", "leaderboard_updates"))
    parser.add_argument("--window-seconds", type=float, default=float(os.getenv("WINDOW_SECONDS", "1.0")))
    parser.add_argument("--metrics-interval-ms", type=float, default=float(os.getenv("METRICS_INTERVAL_MS", "1000")))
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.self_test:
        run_self_test()
        return
    if redis is None:
        raise RuntimeError("redis is not installed. Run: pip install -r telemetry/requirements.txt")

    running = True

    def stop(_signum, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    redis_client = redis.Redis.from_url(args.redis_url, decode_responses=True)
    redis_client.ping()
    consumer = make_consumer(args)
    state = ContestantState(window_seconds=args.window_seconds)
    last_metrics = time.monotonic()

    try:
        while running:
            msg = consumer.poll(0.05)
            if msg is not None:
                if msg.error():
                    raise KafkaException(msg.error())

                for event in unpack_trade_events(msg.value()):
                    violation = state.validate(event)
                    if violation:
                        metrics = disqualify(
                            redis_client,
                            args.contestant_id,
                            state,
                            violation,
                            args.channel,
                            args.leaderboard_key,
                        )
                        print(json.dumps({"status": "disqualified", **metrics}, separators=(",", ":")), flush=True)
                        return
                    state.accept(event)

            now = time.monotonic()
            if now - last_metrics >= args.metrics_interval_ms / 1000:
                metrics = state.metrics()
                publish_metrics(redis_client, args.contestant_id, metrics, args.channel, args.leaderboard_key)
                print(json.dumps({"status": "ok", **metrics}, separators=(",", ":")), flush=True)
                last_metrics = now
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
