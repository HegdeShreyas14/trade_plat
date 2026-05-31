#include<chrono>
#include<iostream>
#include "OrderBook.h"
#include "MatchingEngine.h"

void MatchingEngine::addOrder(const Order& order) {
    if (order.IsBuy) {
        book.buyOrders[order.price].push_back(order);
        book.orderMap[order.OrderId] = {true, order.price, std::prev(book.buyOrders[order.price].end())};
    } else {
        book.sellOrders[order.price].push_back(order);
        book.orderMap[order.OrderId] = {false, order.price, std::prev(book.sellOrders[order.price].end())};
    }
    matching();
}

void MatchingEngine::matching() {
    while (!book.buyOrders.empty() && !book.sellOrders.empty()) {
        auto bestBuyIt = book.buyOrders.begin();
        auto bestSellIt = book.sellOrders.begin();

        if (bestBuyIt->first < bestSellIt->first) {
            break; 
        }

        auto& buyList = bestBuyIt->second;
        auto& sellList = bestSellIt->second;

        Order& buy = buyList.front();
        Order& sell = sellList.front();

        int tradedqty = std::min(buy.quantity, sell.quantity);

        Trade trade;
        trade.buyOrderId = buy.OrderId;
        trade.sellOrderId = sell.OrderId;
        trade.quantity = tradedqty;
        trade.buyLimitPrice = buy.price;
        trade.sellLimitPrice = sell.price;
        trade.timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        trade.executionPrice = (buy.seqNo < sell.seqNo) ? buy.price : sell.price; // Maker-taker logic
        tradeHistory.push_back(trade);

        buy.quantity -= tradedqty;
        sell.quantity -= tradedqty;

        std::cout << "BUY LOT " << trade.buyOrderId << " SELL LOT " << trade.sellOrderId << " QTY " << trade.quantity << " PRICE " << trade.executionPrice << std::endl;

        if (buy.quantity == 0) {
            book.orderMap.erase(buy.OrderId);
            buyList.pop_front();
        }
        if (sell.quantity == 0) {
            book.orderMap.erase(sell.OrderId);
            sellList.pop_front();
        }

        // Clean up empty price levels
        if (buyList.empty()) {
            book.buyOrders.erase(bestBuyIt);
        }
        if (sellList.empty()) {
            book.sellOrders.erase(bestSellIt);
        }
    }
}

void MatchingEngine::cancelOrder(int orderid) {
    auto it = book.orderMap.find(orderid);
    if (it != book.orderMap.end()) {
        auto loc = it->second;
        if (loc.isBuy) {
            auto& list = book.buyOrders[loc.price];
            list.erase(loc.iterator);
            if (list.empty()) {
                book.buyOrders.erase(loc.price);
            }
        } else {
            auto& list = book.sellOrders[loc.price];
            list.erase(loc.iterator);
            if (list.empty()) {
                book.sellOrders.erase(loc.price);
            }
        }
        book.orderMap.erase(it);
        std::cout << "CANCELLED ORDER " << orderid << std::endl;
    }
}

void MatchingEngine::printTradeHistory() {
    for(const auto& trade : tradeHistory) {
        std::cout << "BUY " << trade.buyOrderId << " SELL " << trade.sellOrderId << " QTY " << trade.quantity << " PRICE " << trade.executionPrice << std::endl;
    }
}

void MatchingEngine::printOrderBook() {
    std::cout << "\n========BUY SIDE=========\n";
    for (auto it = book.buyOrders.begin(); it != book.buyOrders.end(); ++it) {
        for (const auto& order : it->second) {
            std::cout << order.price << " x " << order.quantity << " (ID: " << order.OrderId << ")\n";
        }
    }

    std::cout << "\n========SELL SIDE=========\n";
    for (auto it = book.sellOrders.begin(); it != book.sellOrders.end(); ++it) {
        for (const auto& order : it->second) {
            std::cout << order.price << " x " << order.quantity << " (ID: " << order.OrderId << ")\n";
        }
    }
    std::cout << std::endl;
}
