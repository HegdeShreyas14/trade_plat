#pragma once
#include "trade.h"
#include<vector>
#include "OrderBook.h"

class MatchingEngine
{
private:
    OrderBook book;
    std::vector<Trade> tradeHistory;
public:

    void addOrder(const Order& order);

    void matching();

    void cancelOrder(int orderId);

    void printTradeHistory();

    void printOrderBook();
};
