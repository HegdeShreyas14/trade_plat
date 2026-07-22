// Regression tests for OrderBook index bookkeeping.
//
// orderMap records where each resting order sits inside its price level. Removing
// an order shifts everything behind it down one slot, so those recorded positions
// have to be refreshed. When they are not, a later lookup either addresses a
// neighbouring order or points past the end of the level -- which shows up as
// cancelling somebody else's order, or as a cancel that silently does nothing.
//
// Build and run:
//   g++ -std=c++17 -I matching-engine matching-engine/tests/test_orderbook.cpp -o /tmp/test_orderbook
//   /tmp/test_orderbook

#include <cassert>
#include <iostream>
#include <vector>

#include "../OrderBook.h"

namespace {

Order makeOrder(int id, double price, bool isBuy, int qty = 10) {
    Order order;
    order.OrderId = id;
    order.price = price;
    order.IsBuy = isBuy;
    order.quantity = qty;
    return order;
}

// Appends to a level the same way addOrder does, recording the new position.
void rest(OrderBook& book, std::vector<Order>& level, const Order& order) {
    level.push_back(order);
    book.orderMap[order.OrderId] = {order.IsBuy, order.price, level.size() - 1};
}

// Every tracked order must be findable at exactly the position recorded for it.
template <typename LevelMap>
void assertIndicesAddressTheirOwners(const OrderBook& book, const LevelMap& levels, bool isBuy) {
    for (const auto& entry : book.orderMap) {
        const int id = entry.first;
        const OrderLocation& loc = entry.second;
        if (loc.isBuy != isBuy) continue;

        auto levelIt = levels.find(loc.price);
        assert(levelIt != levels.end() && "tracked order references a level that is gone");
        assert(loc.vector_index < levelIt->second.size() && "recorded index runs past the level");
        assert(levelIt->second[loc.vector_index].OrderId == id &&
               "recorded index addresses a different order");
    }
}

std::vector<int> idsAt(const OrderBook& book, double price, bool isBuy) {
    std::vector<int> ids;
    if (isBuy) {
        auto it = book.buyOrders.find(price);
        if (it == book.buyOrders.end()) return ids;
        for (const auto& order : it->second) ids.push_back(order.OrderId);
    } else {
        auto it = book.sellOrders.find(price);
        if (it == book.sellOrders.end()) return ids;
        for (const auto& order : it->second) ids.push_back(order.OrderId);
    }
    return ids;
}

void test_erase_from_middle_reindexes_survivors() {
    OrderBook book;
    auto& level = book.buyOrders[100.0];
    for (int id : {1, 2, 3, 4}) rest(book, level, makeOrder(id, 100.0, true));

    // Drop order 2, which sits at index 1.
    book.orderMap.erase(2);
    book.eraseAt(level, 1);

    assert((idsAt(book, 100.0, true) == std::vector<int>{1, 3, 4}));
    // Orders 3 and 4 each moved down a slot; without reindexing they would still
    // claim 2 and 3, so cancelling 3 would remove 4 and cancelling 4 would overrun.
    assert(book.orderMap[3].vector_index == 1);
    assert(book.orderMap[4].vector_index == 2);
    assertIndicesAddressTheirOwners(book, book.buyOrders, true);
}

void test_repeated_front_erase_matches_fill_path() {
    OrderBook book;
    auto& level = book.sellOrders[101.0];
    for (int id : {10, 11, 12, 13, 14}) rest(book, level, makeOrder(id, 101.0, false));

    // The matching loop consumes the front of a level on every complete fill.
    for (int filled : {10, 11}) {
        book.orderMap.erase(filled);
        book.eraseAt(level, 0);
        assertIndicesAddressTheirOwners(book, book.sellOrders, false);
    }

    assert((idsAt(book, 101.0, false) == std::vector<int>{12, 13, 14}));
    assert(book.orderMap[12].vector_index == 0);
    assert(book.orderMap[13].vector_index == 1);
    assert(book.orderMap[14].vector_index == 2);
}

void test_erase_last_leaves_others_untouched() {
    OrderBook book;
    auto& level = book.buyOrders[99.5];
    for (int id : {7, 8, 9} ) rest(book, level, makeOrder(id, 99.5, true));

    book.orderMap.erase(9);
    book.eraseAt(level, 2);

    assert((idsAt(book, 99.5, true) == std::vector<int>{7, 8}));
    assert(book.orderMap[7].vector_index == 0);
    assert(book.orderMap[8].vector_index == 1);
    assertIndicesAddressTheirOwners(book, book.buyOrders, true);
}

void test_out_of_range_index_is_ignored() {
    OrderBook book;
    auto& level = book.buyOrders[98.0];
    rest(book, level, makeOrder(21, 98.0, true));

    book.eraseAt(level, 5);   // past the end
    book.eraseAt(level, 1);   // one past the only element

    assert((idsAt(book, 98.0, true) == std::vector<int>{21}));
    assertIndicesAddressTheirOwners(book, book.buyOrders, true);
}

// Interleaving fills and cancels is where stale indices previously compounded:
// each front-erase shifted the level while cancels kept writing older positions.
void test_interleaved_fill_and_cancel() {
    OrderBook book;
    auto& level = book.buyOrders[102.0];
    for (int id : {30, 31, 32, 33, 34, 35}) rest(book, level, makeOrder(id, 102.0, true));

    book.orderMap.erase(30);            // front fill
    book.eraseAt(level, 0);

    const std::size_t idxOf32 = book.orderMap[32].vector_index;
    book.orderMap.erase(32);            // cancel from the middle
    book.eraseAt(level, idxOf32);

    book.orderMap.erase(31);            // front fill again
    book.eraseAt(level, 0);

    assert((idsAt(book, 102.0, true) == std::vector<int>{33, 34, 35}));
    assertIndicesAddressTheirOwners(book, book.buyOrders, true);

    // The surviving orders must still be cancellable by id, addressing themselves.
    const std::size_t idxOf34 = book.orderMap[34].vector_index;
    assert(level[idxOf34].OrderId == 34);
}

}  // namespace

int main() {
    test_erase_from_middle_reindexes_survivors();
    test_repeated_front_erase_matches_fill_path();
    test_erase_last_leaves_others_untouched();
    test_out_of_range_index_is_ignored();
    test_interleaved_fill_and_cancel();

    std::cout << "orderbook index bookkeeping: all tests passed\n";
    return 0;
}
