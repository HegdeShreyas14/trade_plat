#include <chrono>
#include <iostream>
#include <thread>
#include <algorithm>
#include <emmintrin.h>
#include <pthread.h>
#include <sched.h>
#include "OrderBook.h"
#include "MatchingEngine.h"

MatchingEngine::~MatchingEngine() {
    stop();
}

void MatchingEngine::start() {
    isRunning = true;
    kafkaOffloader.start();
    workerThread = std::thread(&MatchingEngine::processLoop, this);
    metricsThread = std::thread(&MatchingEngine::metricsLoop, this);
}

void MatchingEngine::stop() {
    isRunning = false;
    if (workerThread.joinable()) workerThread.join();
    if (metricsThread.joinable()) metricsThread.join();
    kafkaOffloader.stop();
}

void MatchingEngine::enqueueOrder(const Order& order) {
    if (!incomingOrders.enqueue(order)) {
        orders_dropped.fetch_add(1, std::memory_order_relaxed);
    }
}

void MatchingEngine::processLoop() {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(1, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

    Order order;
    while (isRunning) {
        if (incomingOrders.dequeue(order)) {
            addOrder(order);
            
            long long t2 = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            
            {
                std::lock_guard<std::mutex> lock(latency_mutex);
                if (order.t1 > 0) engine_latencies.push_back(t2 - order.t1);
                if (order.t0 > 0 && order.t1 > 0) network_latencies.push_back(order.t1 - order.t0);
            }
            orders_processed.fetch_add(1, std::memory_order_relaxed);
            
        } else {
            _mm_pause();
        }
    }
}

void MatchingEngine::metricsLoop() {
    uint64_t last_orders_processed = 0;
    uint64_t last_trades_executed = 0;
    uint64_t last_orders_dropped = 0;
    uint64_t last_trade_events_dropped = 0;
    uint64_t last_trade_events_delivered = 0;

    while (isRunning) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        if (!isRunning) break;

        uint64_t current_orders = orders_processed.load(std::memory_order_relaxed);
        uint64_t current_trades = trades_executed.load(std::memory_order_relaxed);
        uint64_t current_dropped = orders_dropped.load(std::memory_order_relaxed);
        uint64_t current_trade_event_drops = trade_events_dropped.load(std::memory_order_relaxed);
        uint64_t current_trade_event_delivered = kafkaOffloader.delivered();
        
        uint64_t ops = current_orders - last_orders_processed;
        uint64_t tps = current_trades - last_trades_executed;
        uint64_t drops = current_dropped - last_orders_dropped;
        uint64_t event_drops = current_trade_event_drops - last_trade_events_dropped;
        uint64_t event_delivered = current_trade_event_delivered - last_trade_events_delivered;
        
        last_orders_processed = current_orders;
        last_trades_executed = current_trades;
        last_orders_dropped = current_dropped;
        last_trade_events_dropped = current_trade_event_drops;
        last_trade_events_delivered = current_trade_event_delivered;

        std::vector<long long> e_lat, n_lat;
        {
            std::lock_guard<std::mutex> lock(latency_mutex);
            e_lat.swap(engine_latencies);
            n_lat.swap(network_latencies);
        }

        auto calc_percentile = [](std::vector<long long>& v, double p) -> long long {
            if (v.empty()) return 0;
            size_t idx = static_cast<size_t>((v.size() - 1) * p);
            std::nth_element(v.begin(), v.begin() + idx, v.end());
            return v[idx];
        };

        std::cout << "\n=== Engine Metrics (1s window) ===\n";
        std::cout << "Orders / sec  : " << ops << "\n";
        std::cout << "Trades / sec  : " << tps << "\n";
        std::cout << "Queue Drops   : " << drops << "\n";
        std::cout << "Kafka Events  : " << event_delivered << "/sec, drops=" << event_drops
                  << ", depth=" << kafkaOffloader.queueDepth() << "\n";
        
        if (!n_lat.empty()) {
            std::cout << "Network Lat   : p50=" << calc_percentile(n_lat, 0.50) / 1000 << "us, p90=" 
                      << calc_percentile(n_lat, 0.90) / 1000 << "us, p99=" << calc_percentile(n_lat, 0.99) / 1000 << "us\n";
        }
        if (!e_lat.empty()) {
            std::cout << "Engine Lat    : p50=" << calc_percentile(e_lat, 0.50) / 1000 << "us, p90=" 
                      << calc_percentile(e_lat, 0.90) / 1000 << "us, p99=" << calc_percentile(e_lat, 0.99) / 1000 << "us\n";
        }
        std::cout << "==================================\n";
    }
}

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

        if (bestBuyIt->first < bestSellIt->first) break;

        auto& buyList = bestBuyIt->second;
        auto& sellList = bestSellIt->second;

        Order& buy = buyList.front();
        Order& sell = sellList.front();

        int tradedqty = std::min(buy.quantity, sell.quantity);

        long long t2_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        double executionPrice = (buy.seqNo < sell.seqNo) ? buy.price : sell.price;
        const Order& taker = (buy.seqNo > sell.seqNo) ? buy : sell;

        TradeEvent event{
            ++next_match_id,
            static_cast<uint64_t>(buy.OrderId),
            static_cast<uint64_t>(sell.OrderId),
            static_cast<uint64_t>(taker.t0),
            static_cast<uint64_t>(taker.t1),
            static_cast<uint64_t>(t2_ns),
            executionPrice,
            static_cast<uint32_t>(tradedqty)
        };
        if (!kafkaOffloader.publish(event)) {
            trade_events_dropped.fetch_add(1, std::memory_order_relaxed);
        }

        // suppressing std::cout logs for trade executions to avoid skewed benchmark latency!
        
        trades_executed.fetch_add(1, std::memory_order_relaxed);

        buy.quantity -= tradedqty;
        sell.quantity -= tradedqty;

        if (buy.quantity == 0) {
            book.orderMap.erase(buy.OrderId);
            buyList.pop_front();
        }
        if (sell.quantity == 0) {
            book.orderMap.erase(sell.OrderId);
            sellList.pop_front();
        }

        if (buyList.empty()) book.buyOrders.erase(bestBuyIt);
        if (sellList.empty()) book.sellOrders.erase(bestSellIt);
    }
}

void MatchingEngine::cancelOrder(int orderid) {
    auto it = book.orderMap.find(orderid);
    if (it != book.orderMap.end()) {
        auto loc = it->second;
        if (loc.isBuy) {
            auto& list = book.buyOrders[loc.price];
            list.erase(loc.iterator);
            if (list.empty()) book.buyOrders.erase(loc.price);
        } else {
            auto& list = book.sellOrders[loc.price];
            list.erase(loc.iterator);
            if (list.empty()) book.sellOrders.erase(loc.price);
        }
        book.orderMap.erase(it);
    }
}

void MatchingEngine::printTradeHistory() {}
void MatchingEngine::printOrderBook() {}
