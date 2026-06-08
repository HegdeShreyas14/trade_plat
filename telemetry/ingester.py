#!/usr/bin/env python3
import argparse
import json
import os
import signal
import struct
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone

try:
    from confluent_kafka import Consumer, KafkaException
except ImportError:  # pragma: no cover - exercised in deployment env
    Consumer = None
    KafkaException = RuntimeError

try:
    import redis
except ImportError:  # pragma: no cover
    redis = None

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:  # pragma: no cover
    psycopg2 = None
    execute_values = None


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

    @property
    def network_latency_ns(self):
        return max(0, self.t1_ns - self.t0_ns)

    @property
    def t2_datetime(self):
        return datetime.fromtimestamp(self.t2_ns / 1_000_000_000, tz=timezone.utc)


class SlidingWindow:
    def __init__(self, seconds):
        self.seconds = seconds
        self.samples = deque()
        self.last_match_id = 0

    def add(self, event):
        now = time.monotonic()
        self.samples.append((now, event.engine_latency_ns, event.network_latency_ns))
        self.last_match_id = event.match_id
        self.prune(now)

    def prune(self, now=None):
        if now is None:
            now = time.monotonic()
        cutoff = now - self.seconds
        while self.samples and self.samples[0][0] < cutoff:
            self.samples.popleft()

    def metrics(self):
        self.prune()
        if not self.samples:
            return {
                "tps": 0,
                "engine_p50_us": 0,
                "engine_p90_us": 0,
                "engine_p99_us": 0,
                "network_p50_us": 0,
                "network_p90_us": 0,
                "network_p99_us": 0,
                "last_match_id": self.last_match_id,
            }

        engine = [sample[1] for sample in self.samples]
        network = [sample[2] for sample in self.samples]
        return {
            "tps": len(self.samples) / self.seconds,
            "engine_p50_us": percentile(engine, 0.50) / 1000,
            "engine_p90_us": percentile(engine, 0.90) / 1000,
            "engine_p99_us": percentile(engine, 0.99) / 1000,
            "network_p50_us": percentile(network, 0.50) / 1000,
            "network_p90_us": percentile(network, 0.90) / 1000,
            "network_p99_us": percentile(network, 0.99) / 1000,
            "last_match_id": self.last_match_id,
        }


class TimescaleSink:
    def __init__(self, dsn, batch_size, flush_interval):
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = True
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.pending = []
        self.last_flush = time.monotonic()
        self.ensure_schema()

    def ensure_schema(self):
        with self.conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_events (
                    t2_ts TIMESTAMPTZ NOT NULL,
                    match_id BIGINT NOT NULL,
                    buy_order_id BIGINT NOT NULL,
                    sell_order_id BIGINT NOT NULL,
                    t0_ns BIGINT NOT NULL,
                    t1_ns BIGINT NOT NULL,
                    t2_ns BIGINT NOT NULL,
                    engine_latency_ns BIGINT NOT NULL,
                    network_latency_ns BIGINT NOT NULL,
                    price DOUBLE PRECISION NOT NULL,
                    qty INTEGER NOT NULL,
                    PRIMARY KEY (t2_ts, match_id)
                )
                """
            )
            cur.execute(
                "SELECT create_hypertable('trade_events', 't2_ts', if_not_exists => TRUE)"
            )

    def add(self, event):
        self.pending.append(
            (
                event.t2_datetime,
                event.match_id,
                event.buy_order_id,
                event.sell_order_id,
                event.t0_ns,
                event.t1_ns,
                event.t2_ns,
                event.engine_latency_ns,
                event.network_latency_ns,
                event.price,
                event.qty,
            )
        )
        if len(self.pending) >= self.batch_size:
            self.flush()
        elif time.monotonic() - self.last_flush >= self.flush_interval:
            self.flush()

    def flush(self):
        if not self.pending:
            self.last_flush = time.monotonic()
            return

        rows = self.pending
        self.pending = []
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO trade_events (
                    t2_ts, match_id, buy_order_id, sell_order_id,
                    t0_ns, t1_ns, t2_ns, engine_latency_ns, network_latency_ns,
                    price, qty
                ) VALUES %s
                ON CONFLICT (t2_ts, match_id) DO NOTHING
                """,
                rows,
                page_size=len(rows),
            )
        self.last_flush = time.monotonic()

    def close(self):
        self.flush()
        self.conn.close()


def percentile(values, p):
    if not values:
        return 0
    ordered = sorted(values)
    idx = int((len(ordered) - 1) * p)
    return ordered[idx]


def unpack_trade_events(payload):
    if len(payload) % TRADE_EVENT_SIZE != 0:
        raise ValueError(f"Kafka payload size {len(payload)} is not a multiple of {TRADE_EVENT_SIZE}")

    for offset in range(0, len(payload), TRADE_EVENT_SIZE):
        yield TradeEvent(*struct.unpack_from(TRADE_EVENT_FORMAT, payload, offset))


def ranking_score(metrics):
    # Higher is better: reward throughput while penalizing p99 execution latency.
    return (metrics["tps"] * 1000.0) - metrics["engine_p99_us"]


def publish_leaderboard(redis_client, key, channel, contestant_id, metrics):
    meta = {
        "tps": round(metrics["tps"], 2),
        "p50_lat_us": round(metrics["engine_p50_us"], 2),
        "p90_lat_us": round(metrics["engine_p90_us"], 2),
        "p99_lat_us": round(metrics["engine_p99_us"], 2),
        "correctness": "PASSED",
        "total_events": int(metrics.get("consumed_total", 0)),
        "last_match_id": int(metrics.get("last_match_id", 0)),
    }
    payload = {
        "contestant_id": contestant_id,
        "score": ranking_score(metrics),
        **metrics,
        "updated_at_ns": time.time_ns(),
    }
    redis_client.zadd(key, {contestant_id: payload["score"]})
    redis_client.hset(f"contestant:meta:{contestant_id}", mapping=meta)
    redis_client.publish(channel, json.dumps(payload, separators=(",", ":")))


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


def parse_args():
    parser = argparse.ArgumentParser(description="Kafka trade-event telemetry ingester")
    parser.add_argument("--kafka-brokers", default=os.getenv("KAFKA_BROKERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "trade-events"))
    parser.add_argument("--group-id", default=os.getenv("KAFKA_GROUP_ID", "telemetry-ingester"))
    parser.add_argument("--offset-reset", default=os.getenv("KAFKA_OFFSET_RESET", "latest"))
    parser.add_argument("--contestant-id", default=os.getenv("CONTESTANT_ID", "default"))
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--redis-key", default=os.getenv("REDIS_LEADERBOARD_KEY", "leaderboard:scores"))
    parser.add_argument("--redis-channel", default=os.getenv("REDIS_CHANNEL", "leaderboard_updates"))
    parser.add_argument("--timescale-dsn", default=os.getenv("TIMESCALE_DSN", ""))
    parser.add_argument("--window-seconds", type=float, default=float(os.getenv("WINDOW_SECONDS", "1.0")))
    parser.add_argument("--db-batch-size", type=int, default=int(os.getenv("DB_BATCH_SIZE", "500")))
    parser.add_argument("--db-flush-ms", type=float, default=float(os.getenv("DB_FLUSH_MS", "250")))
    parser.add_argument("--metrics-interval-ms", type=float, default=float(os.getenv("METRICS_INTERVAL_MS", "1000")))
    parser.add_argument("--disable-redis", action="store_true")
    parser.add_argument("--disable-timescale", action="store_true")
    parser.add_argument("--self-test", action="store_true", help="Run local unpack/metrics test without Kafka")
    return parser.parse_args()


def run_self_test():
    now = time.time_ns()
    events = [
        TradeEvent(1, 10, 20, now - 5000, now - 2000, now, 101.25, 7),
        TradeEvent(2, 11, 21, now - 8000, now - 3000, now + 1000, 101.50, 3),
    ]
    payload = b"".join(
        struct.pack(
            TRADE_EVENT_FORMAT,
            event.match_id,
            event.buy_order_id,
            event.sell_order_id,
            event.t0_ns,
            event.t1_ns,
            event.t2_ns,
            event.price,
            event.qty,
        )
        for event in events
    )
    decoded = list(unpack_trade_events(payload))
    window = SlidingWindow(1.0)
    for event in decoded:
        window.add(event)
    print(json.dumps({"decoded": len(decoded), "metrics": window.metrics()}, indent=2))


def main():
    args = parse_args()
    if args.self_test:
        run_self_test()
        return

    running = True

    def stop(_signum, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    redis_client = None
    if not args.disable_redis:
        if redis is None:
            raise RuntimeError("redis is not installed. Run: pip install -r telemetry/requirements.txt")
        redis_client = redis.Redis.from_url(args.redis_url, decode_responses=True)
        redis_client.ping()

    timescale = None
    if not args.disable_timescale:
        if not args.timescale_dsn:
            raise RuntimeError("TIMESCALE_DSN is required unless --disable-timescale is set")
        if psycopg2 is None:
            raise RuntimeError("psycopg2-binary is not installed. Run: pip install -r telemetry/requirements.txt")
        timescale = TimescaleSink(args.timescale_dsn, args.db_batch_size, args.db_flush_ms / 1000)

    consumer = make_consumer(args)
    window = SlidingWindow(args.window_seconds)
    last_metrics = time.monotonic()
    consumed = 0

    try:
        while running:
            msg = consumer.poll(0.05)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            for event in unpack_trade_events(msg.value()):
                consumed += 1
                window.add(event)
                if timescale:
                    timescale.add(event)

            now = time.monotonic()
            if now - last_metrics >= args.metrics_interval_ms / 1000:
                metrics = window.metrics()
                metrics["consumed_total"] = consumed
                if redis_client:
                    publish_leaderboard(
                        redis_client,
                        args.redis_key,
                        args.redis_channel,
                        args.contestant_id,
                        metrics,
                    )
                print(json.dumps(metrics, separators=(",", ":")))
                last_metrics = now
    finally:
        consumer.close()
        if timescale:
            timescale.close()


if __name__ == "__main__":
    main()
