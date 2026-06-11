#pragma once
#include <chrono>
#include <map>
#include <vector>
#include <unordered_map>
#include "Order.h"

// Define a structured locator footprint *before* the main engine collections
struct OrderLocation {
    bool isBuy;
    double price;
    size_t vector_index; // Swapped node iterator out for direct O(1) continuous array offset indexing
};

class OrderBook {
public:
    // Continuous layout arrays cache-aligned for ultra-fast sequential matching access
    std::map<double, std::vector<Order>, std::greater<double>> buyOrders;
    std::map<double, std::vector<Order>, std::less<double>> sellOrders;

    // Fast tracking map for O(1) cancellation lookups
    std::unordered_map<int, OrderLocation> orderMap;
};