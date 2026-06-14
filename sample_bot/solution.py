# solution.py
import random

def on_market_update(market_snapshot, exchange):
    symbol = market_snapshot.get("symbol", "BTCUSD")
    current_price = market_snapshot.get("price", 100)
    
    print(f"[Bot Strategy] Market update -> {symbol}: ${current_price}")
    
    # If price dips below $152, trigger an immediate buy order
    if current_price < 152:
        target_buy_price = current_price + 1
        print(f"[Bot Strategy] Target met! Sending BUY at ${target_buy_price}")
        
        exchange.submit_order(
            direction="BUY",
            price=target_buy_price,
            quantity=1
        )
