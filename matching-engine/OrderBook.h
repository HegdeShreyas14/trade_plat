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

    // Removes the order at `index` from `level` while keeping orderMap's recorded
    // positions truthful. Erasing shifts every later order down one slot, so their
    // stored vector_index must follow; skipping that leaves the map pointing at a
    // neighbour, which makes a subsequent lookup cancel the wrong order or fall
    // off the end of the level entirely.
    //
    // The shift itself is already O(level size), so refreshing the indices adds a
    // constant factor rather than changing the cost of the operation.
    void eraseAt(std::vector<Order>& level, std::size_t index) {
        if (index >= level.size()) return;
        level.erase(level.begin() + index);
        for (std::size_t i = index; i < level.size(); ++i) {
            auto it = orderMap.find(level[i].OrderId);
            if (it != orderMap.end()) {
                it->second.vector_index = i;
            }
        }
    }
};