# sandbox_sdk.py — Platform harness: connects the bot to the exchange and handles the wire protocol.
# Contestants should NOT modify this file; edit solution.py instead.
import asyncio
import os
import random
import struct
import time
import websockets
from solution import on_market_update

GATEWAY_URL = os.environ.get("EXCHANGE_GATEWAY_URL", "ws://localhost:8080")
CONTESTANT_ID = os.environ.get("CONTESTANT_ID", "anonymous_quant_bot")

# 32-byte little-endian OrderMessage: order_id(u64) ts_ns(u64) price(f64) qty(u32) side(u8) pad(3x)
_ORDER_FMT = "<QQdIB3x"


class ExchangeConnection:
    def __init__(self, websocket):
        self.ws = websocket
        self.order_sequence = random.randint(10000, 99999)

    def submit_order(self, direction: str, price: float, quantity: int):
        """Submit a limit order. direction must be 'BUY' or 'SELL'."""
        self.order_sequence += 1
        side_code = 1 if direction == "BUY" else 2
        payload = struct.pack(
            _ORDER_FMT,
            self.order_sequence,
            time.time_ns(),
            float(price),
            int(quantity),
            side_code,
        )
        # ws.send(bytes) sends a binary frame (opcode 0x2), auto-masked per RFC 6455
        asyncio.create_task(self.ws.send(payload))


async def run_sandbox_harness():
    print(f"[SDK] Connecting to {GATEWAY_URL} as {CONTESTANT_ID}...")
    async with websockets.connect(GATEWAY_URL) as ws:
        exchange = ExchangeConnection(ws)
        print("[SDK] Connected to exchange. Starting evaluation loop...")
        while True:
            market_tick = {"symbol": "BTCUSD", "price": random.randint(148, 155)}
            on_market_update(market_tick, exchange)
            await asyncio.sleep(1.0)


if __name__ == "__main__":
    asyncio.run(run_sandbox_harness())
