#!/usr/bin/env python3
import argparse
import json
import os
import re
import sys

try:
    import psycopg2
except ImportError:  # pragma: no cover
    psycopg2 = None


IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def quote_identifier(value):
    if not IDENTIFIER.match(value):
        raise ValueError(f"unsafe SQL identifier: {value!r}")
    return f'"{value}"'


def checks(table):
    return [
        (
            "duplicate_match_id",
            f"""
            SELECT match_id, count(*) AS copies
            FROM {table}
            GROUP BY match_id
            HAVING count(*) > 1
            ORDER BY match_id
            LIMIT %s
            """,
        ),
        (
            "match_sequence_gap",
            f"""
            WITH ordered AS (
                SELECT match_id, lag(match_id) OVER (ORDER BY match_id) AS prev_match_id
                FROM {table}
            )
            SELECT prev_match_id + 1 AS expected_match_id, match_id AS actual_match_id
            FROM ordered
            WHERE prev_match_id IS NOT NULL
              AND match_id <> prev_match_id + 1
            ORDER BY actual_match_id
            LIMIT %s
            """,
        ),
        (
            "retrograde_engine_timestamp",
            f"""
            WITH ordered AS (
                SELECT match_id, t2_ns, lag(t2_ns) OVER (ORDER BY match_id) AS prev_t2_ns
                FROM {table}
            )
            SELECT match_id, prev_t2_ns, t2_ns
            FROM ordered
            WHERE prev_t2_ns IS NOT NULL
              AND t2_ns < prev_t2_ns
            ORDER BY match_id
            LIMIT %s
            """,
        ),
        (
            "invalid_trade_fields",
            f"""
            SELECT match_id, buy_order_id, sell_order_id, t0_ns, t1_ns, t2_ns, price, qty
            FROM {table}
            WHERE buy_order_id = 0
               OR sell_order_id = 0
               OR buy_order_id = sell_order_id
               OR t0_ns = 0
               OR t1_ns = 0
               OR t2_ns = 0
               OR t1_ns < t0_ns
               OR t2_ns < t1_ns
               OR NOT (price > 0)
               OR price::text IN ('NaN', 'Infinity', '-Infinity')
               OR qty <= 0
            ORDER BY match_id
            LIMIT %s
            """,
        ),
    ]


def fetch_dicts(cur, query, limit):
    cur.execute(query, (limit,))
    columns = [column.name for column in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def run_audit(dsn, table_name, limit):
    if psycopg2 is None:
        raise RuntimeError("psycopg2-binary is not installed. Run: pip install -r telemetry/requirements.txt")

    table = quote_identifier(table_name)
    results = {}
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT count(*) AS total_events,
                       min(match_id) AS first_match_id,
                       max(match_id) AS last_match_id,
                       min(t2_ns) AS first_t2_ns,
                       max(t2_ns) AS last_t2_ns
                FROM {table}
                """
            )
            summary_columns = [column.name for column in cur.description]
            results["summary"] = dict(zip(summary_columns, cur.fetchone()))

            for name, query in checks(table):
                rows = fetch_dicts(cur, query, limit)
                results[name] = {
                    "passed": len(rows) == 0,
                    "examples": rows,
                }

    results["passed"] = all(
        value.get("passed", True)
        for key, value in results.items()
        if key != "summary"
    )
    return results


def parse_args():
    parser = argparse.ArgumentParser(description="Post-contest TimescaleDB correctness audit")
    parser.add_argument("--timescale-dsn", default=os.getenv("TIMESCALE_DSN", ""))
    parser.add_argument("--table", default=os.getenv("TRADE_EVENTS_TABLE", "trade_events"))
    parser.add_argument("--limit", type=int, default=20)
    return parser.parse_args()


def main():
    args = parse_args()
    if not args.timescale_dsn:
        raise RuntimeError("TIMESCALE_DSN is required")
    result = run_audit(args.timescale_dsn, args.table, args.limit)
    print(json.dumps(result, indent=2, default=str))
    if not result["passed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
