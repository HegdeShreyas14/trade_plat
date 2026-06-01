import asyncio
import random
import time
import argparse
import struct
import socket
import os
import base64
import hashlib

class Bot:
    # Protocol V2: 32-byte little-endian wire frame with explicit 3-byte tail padding.
    ORDER_FORMAT = "<QQdIB3x"

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
                sock = self.writer.get_extra_info('socket')
                if sock:
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

                key = base64.b64encode(os.urandom(16)).decode("ascii")
                request = (
                    "GET /orders HTTP/1.1\r\n"
                    f"Host: {self.host}:{self.port}\r\n"
                    "Upgrade: websocket\r\n"
                    "Connection: Upgrade\r\n"
                    f"Sec-WebSocket-Key: {key}\r\n"
                    "Sec-WebSocket-Version: 13\r\n\r\n"
                )
                self.writer.write(request.encode("ascii"))
                await self.writer.drain()

                response = await self.reader.readuntil(b"\r\n\r\n")
                accept_src = (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")
                expected_accept = base64.b64encode(hashlib.sha1(accept_src).digest()).decode("ascii")
                if b" 101 " not in response or expected_accept.encode("ascii") not in response:
                    self.writer.close()
                    await self.writer.wait_closed()
                    raise OSError("WebSocket upgrade failed")

                backoff = 1.0  # Reset on successful connection
                return True
            except (ConnectionRefusedError, OSError, asyncio.IncompleteReadError):
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
        # Convert ASCII side to a 1-byte integer (1 for Buy, 2 for Sell)
        side_code = 1 if side == 'B' else 2
        
        # Pack the data directly into a 32-byte binary struct
        return struct.pack(
            self.ORDER_FORMAT,
            self.generate_order_id(),
            time.time_ns(),
            float(price),
            int(qty),
            side_code
        )

    def websocket_binary_frame(self, payload):
        payload_len = len(payload)
        mask = os.urandom(4)
        header = bytearray([0x82])
        if payload_len < 126:
            header.append(0x80 | payload_len)
        elif payload_len <= 0xFFFF:
            header.append(0x80 | 126)
            header.extend(payload_len.to_bytes(2, "big"))
        else:
            header.append(0x80 | 127)
            header.extend(payload_len.to_bytes(8, "big"))
        header.extend(mask)
        header.extend(byte ^ mask[i & 3] for i, byte in enumerate(payload))
        return bytes(header)

    async def read_loop(self):
        while self.running and self.reader:
            try:
                # Read chunks to drain the socket, preventing TCP backpressure.
                # Using read() instead of readline() so it works with binary ACKs.
                data = await self.reader.read(4096)
                if not data:
                    break  # Connection closed by server
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
            self.writer.write(self.websocket_binary_frame(order_bytes))
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
            skew = (self.inventory / 1000.0) * spread 
            
            bid_price = mid_price - spread - skew
            ask_price = mid_price + spread - skew
            qty = random.randint(10, 100)

            await self.send_order(self.format_order('B', qty, bid_price))
            await self.send_order(self.format_order('S', qty, ask_price))

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
    parser = argparse.ArgumentParser(description="Trading Bot Fleet (WebSocket Binary V2 Protocol)")
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
    
    # 2-second interval for faster metric feedback during benchmarks
    reporter_task = asyncio.create_task(metrics_reporter(bots, interval=2.0))
    
    try:
        if args.duration > 0:
            await asyncio.sleep(args.duration)
        else:
            await asyncio.Event().wait()  # Run forever
    except KeyboardInterrupt:
        print("\nStopping Bot Fleet...")
    finally:
        reporter_task.cancel()
        for bot in bots:
            await bot.disconnect()
        for task in bot_tasks:
            task.cancel()
            
    print("Bot Fleet shut down.")

if __name__ == "__main__":
    asyncio.run(main())
