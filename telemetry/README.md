# Telemetry Ingester

Consumes binary `TradeEvent` batches from Kafka topic `trade-events`, computes rolling latency/TPS metrics, updates Redis leaderboard state, and bulk-writes historical records into TimescaleDB.

The companion scoring worker consumes the same Kafka topic, validates the stream, writes live Redis leaderboard metadata, and publishes Server-Sent Event triggers for the API. The standalone correctness validator is still useful for focused CI and terminal checks.

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
python3 telemetry/scoring_engine.py --self-test
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

Run the live scoring worker:

```bash
python3 telemetry/scoring_engine.py \
  --kafka-brokers localhost:9092 \
  --topic trade-events \
  --contestant-id local-engine
```

Serve the leaderboard API:

```bash
uvicorn telemetry.api:app --host 0.0.0.0 --port 8000
```

Run the React dashboard:

```bash
cd leaderboard
npm install
VITE_API_URL=http://localhost:8000 npm run dev
```

Run the post-contest TimescaleDB audit:

```bash
TIMESCALE_DSN="postgresql://user:pass@localhost:5432/trading" \
python3 telemetry/final_audit.py
```
