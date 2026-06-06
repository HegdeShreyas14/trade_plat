#pragma once
#include <chrono>
#include <map>
#include <vector>
#include <unordered_map>
#include "Order.h"

struct OrderLocation {
    bool isBuy;
    double price;
    size_t vector_index; 
};

class OrderBook {
public:
    std::map<double, std::vector<Order>, std::greater<double>> buyOrders;
    std::map<double, std::vector<Order>, std::less<double>> sellOrders;
    std::unordered_map<int, OrderLocation> orderMap;
};