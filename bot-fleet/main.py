import struct
import asyncio
import random
import time
import argparse

class Bot:
    def __init__(self, bot_id, host, port):
        self.bot_id = bot_id
        self.host = host
        self.port = port
        self.reader = None
        self.writer = None
        self.running = False
        self.order_id_counter = bot_id * 1000000 
        
        # Metrics
        self.orders_sent = 0
        self.failed_messages = 0
        self.connection_drops = 0

    async def connect(self):
        backoff = 1.0
        while self.running:
            try:
                self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
                backoff = 1.0  # Reset on successful connection
                return True
            except (ConnectionRefusedError, OSError):
                self.connection_drops += 1
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10.0)  # Exponential backoff, max 10s
        return False

    async def disconnect(self):
        self.running = False
        if self.writer:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except Exception:
                pass

    def generate_order_id(self):
        self.order_id_counter += 1
        return self.order_id_counter

    def format_order(self, side, qty, price):
        # Format: (uint64_t)ID (uint64_t)TIMESTAMP_NS (double)PRICE (uint32_t)QTY (uint8_t)SIDE
        timestamp_ns = time.time_ns()
        side_val = getattr(side, 'encode', lambda: b'B')()[0] if isinstance(side, str) else side
        # Struct <QQdIB corresponds to Little Endian: 8+8+8+4+1 bytes = 29 bytes exact
        return struct.pack('<QQdIB', self.generate_order_id(), timestamp_ns, float(price), int(qty), side_val)

    async def read_loop(self):
        while self.running and self.reader:
            try:
                line = await self.reader.readline()
                if not line:
                    break  # Connection closed by server
                # Future: Parse ACKs, Executions, Rejects here
                # e.g., if "FILLED" update inventory models
            except Exception:
                break
        
        # If loop exits while bot is supposed to be running, connection dropped
        if self.running:
            self.connection_drops += 1
            if self.writer:
                self.writer.close()

    async def send_order(self, order_bytes):
        if not self.writer or self.writer.is_closing():
            if not await self.connect():
                return
            asyncio.create_task(self.read_loop())

        try:
            self.writer.write(order_bytes)
            await self.writer.drain()
            self.orders_sent += 1
        except Exception:
            self.failed_messages += 1
            if self.writer:
                self.writer.close()

    async def run(self):
        self.running = True
        # Stagger initial bot connection burst to prevent saturating TCP Listen Backlogs
        await asyncio.sleep(random.uniform(0.0, 3.0))
        if await self.connect():
            asyncio.create_task(self.read_loop())
        await self.bot_logic()

    async def bot_logic(self):
        pass


class MarketMakerBot(Bot):
    def __init__(self, bot_id, host, port):
        super().__init__(bot_id, host, port)
        self.inventory = 0  # + means long, - means short

    async def bot_logic(self):
        mid_price = 100.0
        spread = 0.5
        while self.running:
            # Inventory adjustment:
            # If long (too many buys), skew prices lower to encourage selling
            # If short (too many sells), skew prices higher to encourage buying
            skew = (self.inventory / 1000.0) * spread 
            
            bid_price = mid_price - spread - skew
            ask_price = mid_price + spread - skew
            qty = random.randint(10, 100)

            await self.send_order(self.format_order('B', qty, bid_price))
            await self.send_order(self.format_order('S', qty, ask_price))

            # Simulate inventory accumulation slightly for testing purposes until real ACKs
            self.inventory += random.randint(-5, 5)

            mid_price += random.uniform(-0.1, 0.1)
            await asyncio.sleep(0.01)


class NoiseBot(Bot):
    async def bot_logic(self):
        base_price = 100.0
        while self.running:
            side = random.choice(['B', 'S'])
            qty = random.randint(1, 50)
            price = base_price + random.uniform(-2.0, 2.0)
            
            await self.send_order(self.format_order(side, qty, price))
            await asyncio.sleep(random.uniform(0.05, 0.2))


class MomentumBot(Bot):
    async def bot_logic(self):
        current_price = 100.0
        trend_direction = 1  
        while self.running:
            side = 'B' if trend_direction > 0 else 'S'
            qty = random.randint(20, 80)
            
            await self.send_order(self.format_order(side, qty, current_price))
            
            if random.random() < 0.1:
                trend_direction *= -1

            current_price += trend_direction * random.uniform(0.01, 0.2)
            await asyncio.sleep(0.05)


async def metrics_reporter(bots, interval=5.0):
    last_sent = 0
    while True:
        await asyncio.sleep(interval)
        total_sent = sum(b.orders_sent for b in bots)
        total_fails = sum(b.failed_messages for b in bots)
        total_drops = sum(b.connection_drops for b in bots)
        
        ops = (total_sent - last_sent) / interval
        last_sent = total_sent
        
        print(f"[Metrics] Total Sent: {total_sent:,} | OPS: {ops:,.0f}/sec | Drops: {total_drops:,} | Fails: {total_fails:,}")


async def main():
    parser = argparse.ArgumentParser(description="Trading Bot Fleet")
    parser.add_argument("--host", default='127.0.0.1')
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--mm", type=int, default=50, help="Number of Market Makers")
    parser.add_argument("--noise", type=int, default=100, help="Number of Noise Bots")
    parser.add_argument("--momentum", type=int, default=20, help="Number of Momentum Bots")
    parser.add_argument("--duration", type=int, default=30, help="Run duration in seconds (0 for infinite)")
    args = parser.parse_args()

    print(f"Starting Bot Fleet -> Connecting to {args.host}:{args.port}")
    bots = []
    
    bot_configs = [
        (MarketMakerBot, args.mm),
        (NoiseBot, args.noise),
        (MomentumBot, args.momentum)
    ]
    
    bot_id = 1
    for bot_class, count in bot_configs:
        for _ in range(count):
            bots.append(bot_class(bot_id, args.host, args.port))
            bot_id += 1

    print(f"Spawning {len(bots)} bots...")
    bot_tasks = [asyncio.create_task(bot.run()) for bot in bots]
    reporter_task = asyncio.create_task(metrics_reporter(bots, interval=2.0))
    
    try:
        if args.duration > 0:
            await asyncio.sleep(args.duration)
        else:
            await asyncio.Event().wait()  # Run forever
    except KeyboardInterrupt:
        print("Stopping Bot Fleet...")
    finally:
        reporter_task.cancel()
        for bot in bots:
            await bot.disconnect()
        for task in bot_tasks:
            task.cancel()
            
    print("Bot Fleet shut down.")

if __name__ == "__main__":
    asyncio.run(main())
