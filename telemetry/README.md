# Telemetry Ingester

Consumes binary `TradeEvent` batches from Kafka topic `trade-events`, computes rolling latency/TPS metrics, updates Redis leaderboard state, and bulk-writes historical records into TimescaleDB.

The companion correctness validator consumes the same Kafka topic and rejects corrupt execution streams with broken match sequencing, backward timestamps, invalid prices/quantities, or self-matches.

## Binary Format

Matches `matching-engine/MarketEvents.h`:

```python
TRADE_EVENT_FORMAT = "<QQQQQQdI"
TRADE_EVENT_SIZE = 60
```

Fields:

`match_id, buy_order_id, sell_order_id, t0_ns, t1_ns, t2_ns, price, qty`

## Run

```bash
pip install -r telemetry/requirements.txt

TIMESCALE_DSN="postgresql://user:pass@localhost:5432/trading" \
python3 telemetry/ingester.py \
  --kafka-brokers localhost:9092 \
  --topic trade-events \
  --contestant-id local-engine
```

For a local decode/metrics sanity check without infrastructure:

```bash
python3 telemetry/ingester.py --self-test
python3 telemetry/validator.py --self-test
```

Redis can be skipped with `--disable-redis`; TimescaleDB can be skipped with `--disable-timescale`.

Run the validator against Kafka:

```bash
python3 telemetry/validator.py \
  --kafka-brokers localhost:9092 \
  --topic trade-events \
  --allow-start-match-id 1
```
