#!/usr/bin/env python3
import argparse
import json
import math
import os
import signal
import struct
import sys
import time
from dataclasses import dataclass

try:
    from confluent_kafka import Consumer, KafkaException
except ImportError:  # pragma: no cover - exercised in deployment env
    Consumer = None
    KafkaException = RuntimeError


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


class CorrectnessViolation(Exception):
    pass


class CorrectnessValidator:
    def __init__(self, allow_start_match_id=None):
        self.last_match_id = None
        self.last_t2_ns = 0
        self.order_fills = {}
        self.events_validated = 0
        self.allow_start_match_id = allow_start_match_id

    def validate_payload(self, payload):
        if len(payload) % TRADE_EVENT_SIZE != 0:
            self.disqualify(
                f"Corrupted Kafka payload size: expected a multiple of {TRADE_EVENT_SIZE}, got {len(payload)}"
            )

        for offset in range(0, len(payload), TRADE_EVENT_SIZE):
            self.validate_event(payload[offset:offset + TRADE_EVENT_SIZE])

    def validate_event(self, raw_bytes):
        if len(raw_bytes) != TRADE_EVENT_SIZE:
            self.disqualify(f"Corrupted frame size: expected {TRADE_EVENT_SIZE}, got {len(raw_bytes)}")

        event = TradeEvent(*struct.unpack(TRADE_EVENT_FORMAT, raw_bytes))

        if self.last_match_id is None:
            if self.allow_start_match_id is not None and event.match_id != self.allow_start_match_id:
                self.disqualify(
                    f"Invalid starting Match ID: expected {self.allow_start_match_id}, got {event.match_id}"
                )
        else:
            expected_match_id = self.last_match_id + 1
            if event.match_id != expected_match_id:
                self.disqualify(f"Sequence broken: expected Match ID {expected_match_id}, got {event.match_id}")

            if event.t2_ns < self.last_t2_ns:
                self.disqualify(
                    f"Temporal monotonicity broken: engine timestamp moved backward "
                    f"{event.t2_ns} < {self.last_t2_ns}"
                )

        self.validate_trade_sanity(event)

        self.order_fills[event.buy_order_id] = self.order_fills.get(event.buy_order_id, 0) + event.qty
        self.order_fills[event.sell_order_id] = self.order_fills.get(event.sell_order_id, 0) + event.qty

        self.last_match_id = event.match_id
        self.last_t2_ns = event.t2_ns
        self.events_validated += 1

    def validate_trade_sanity(self, event):
        if event.buy_order_id == 0 or event.sell_order_id == 0:
            self.disqualify(f"Invalid zero order id at Match {event.match_id}")
        if event.buy_order_id == event.sell_order_id:
            self.disqualify(f"Self-match detected at Match {event.match_id}: order {event.buy_order_id}")
        if event.t0_ns == 0 or event.t1_ns == 0 or event.t2_ns == 0:
            self.disqualify(f"Missing timestamp detected at Match {event.match_id}")
        if event.t1_ns < event.t0_ns:
            self.disqualify(f"Gateway timestamp precedes bot timestamp at Match {event.match_id}")
        if event.t2_ns < event.t1_ns:
            self.disqualify(f"Engine timestamp precedes gateway timestamp at Match {event.match_id}")
        if not math.isfinite(event.price) or event.price <= 0:
            self.disqualify(f"Invalid execution price at Match {event.match_id}: {event.price}")
        if event.qty <= 0:
            self.disqualify(f"Invalid transaction quantity at Match {event.match_id}: {event.qty}")

    def disqualify(self, reason):
        raise CorrectnessViolation(reason)

    def snapshot(self):
        return {
            "events_validated": self.events_validated,
            "last_match_id": self.last_match_id,
            "last_t2_ns": self.last_t2_ns,
            "tracked_orders": len(self.order_fills),
        }


def pack_event(match_id, buy_order_id, sell_order_id, t0_ns, t1_ns, t2_ns, price, qty):
    return struct.pack(
        TRADE_EVENT_FORMAT,
        match_id,
        buy_order_id,
        sell_order_id,
        t0_ns,
        t1_ns,
        t2_ns,
        price,
        qty,
    )


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


def run_self_test():
    validator = CorrectnessValidator(allow_start_match_id=1)
    valid_payload = b"".join(
        [
            pack_event(1, 1001, 2001, 100, 150, 200, 100.50, 50),
            pack_event(2, 1001, 2002, 110, 160, 210, 100.50, 25),
            pack_event(3, 1003, 2002, 120, 170, 230, 100.75, 10),
        ]
    )
    validator.validate_payload(valid_payload)

    failures = [
        ("sequence_gap", b"".join([pack_event(1, 1, 2, 100, 150, 200, 10.0, 1), pack_event(3, 1, 2, 100, 150, 210, 10.0, 1)])),
        ("retrograde_t2", b"".join([pack_event(1, 1, 2, 100, 150, 200, 10.0, 1), pack_event(2, 1, 2, 100, 150, 199, 10.0, 1)])),
        ("bad_price", pack_event(1, 1, 2, 100, 150, 200, float("nan"), 1)),
        ("bad_qty", pack_event(1, 1, 2, 100, 150, 200, 10.0, 0)),
        ("self_match", pack_event(1, 1, 1, 100, 150, 200, 10.0, 1)),
    ]

    caught = []
    for name, payload in failures:
        failing_validator = CorrectnessValidator(allow_start_match_id=1)
        try:
            failing_validator.validate_payload(payload)
        except CorrectnessViolation:
            caught.append(name)
        else:
            raise AssertionError(f"self-test failed to catch {name}")

    print(
        json.dumps(
            {
                "passed": True,
                "valid_events": validator.events_validated,
                "caught_failures": caught,
                "snapshot": validator.snapshot(),
            },
            indent=2,
        )
    )


def parse_args():
    parser = argparse.ArgumentParser(description="IICPC trade-event correctness validator")
    parser.add_argument("--kafka-brokers", default=os.getenv("KAFKA_BROKERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "trade-events"))
    parser.add_argument("--group-id", default=os.getenv("VALIDATOR_GROUP_ID", "correctness-validator"))
    parser.add_argument("--offset-reset", default=os.getenv("KAFKA_OFFSET_RESET", "earliest"))
    parser.add_argument("--allow-start-match-id", type=int, default=None)
    parser.add_argument("--status-interval-ms", type=float, default=float(os.getenv("VALIDATOR_STATUS_MS", "1000")))
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


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

    validator = CorrectnessValidator(allow_start_match_id=args.allow_start_match_id)
    consumer = make_consumer(args)
    last_status = time.monotonic()

    try:
        while running:
            msg = consumer.poll(0.05)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            validator.validate_payload(msg.value())

            now = time.monotonic()
            if now - last_status >= args.status_interval_ms / 1000:
                print(json.dumps({"status": "ok", **validator.snapshot()}, separators=(",", ":")))
                last_status = now
    except CorrectnessViolation as exc:
        print(json.dumps({"status": "disqualified", "reason": str(exc), **validator.snapshot()}), file=sys.stderr)
        sys.exit(1)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
