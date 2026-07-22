#include <chrono>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <algorithm>
#include <emmintrin.h>
#include <pthread.h>
#include <sched.h>
#include <vector>
#include "MatchingEngine.h"

// Define a structural histogram ring-buffer dimension for lock-free telemetry tracking
constexpr size_t LATENCY_BUCKETS = 100000; // Tracks up to 100ms with 1us resolution

MatchingEngine::~MatchingEngine() {
    stop();
}

void MatchingEngine::start() {
    isRunning = true;
    const char* brokers_env = std::getenv("KAFKA_BROKERS");
    const char* topic_env   = std::getenv("KAFKA_TOPIC");
    kafkaOffloader.start(
        brokers_env ? brokers_env : "localhost:9092",
        topic_env   ? topic_env   : "trade-events"
    );
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
    CPU_SET(1, &cpuset); // Pin matching loop strictly to Core 1
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

    // Pre-allocate thread-local arrays to store latency buckets safely without mutex overhead
    // Bucket index = latency in microseconds
    std::vector<uint32_t> local_engine_hist(LATENCY_BUCKETS, 0);
    std::vector<uint32_t> local_network_hist(LATENCY_BUCKETS, 0);
    uint64_t loop_counter = 0;

    Order order;
    while (isRunning) {
        if (incomingOrders.dequeue(order)) {
            addOrder(order);
            
            long long t2 = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            
            // Lock-free metric collection: track microsecond values inside our metrics buckets
            if (order.t1 > 0) {
                long long e_us = (t2 - order.t1) / 1000;
                if (e_us >= 0 && e_us < static_cast<long long>(LATENCY_BUCKETS)) {
                    local_engine_hist[e_us]++;
                }
            }
            if (order.t0 > 0 && order.t1 > 0) {
                long long n_us = (order.t1 - order.t0) / 1000;
                if (n_us >= 0 && n_us < static_cast<long long>(LATENCY_BUCKETS)) {
                    local_network_hist[n_us]++;
                }
            }

            orders_processed.fetch_add(1, std::memory_order_relaxed);
            loop_counter++;

            // Periodically flush tracking buckets asynchronously to the metrics engine
            if (loop_counter % 65536 == 0) {
                std::lock_guard<std::mutex> lock(latency_mutex);
                for (size_t i = 0; i < LATENCY_BUCKETS; ++i) {
                    shared_engine_histogram[i] += local_engine_hist[i];
                    shared_network_histogram[i] += local_network_hist[i];
                }
                std::fill(local_engine_hist.begin(), local_engine_hist.end(), 0);
                std::fill(local_network_hist.begin(), local_network_hist.end(), 0);
            }
            
        } else {
            _mm_pause(); // Low latency backoff to preserve pipeline decode capacity
        }
    }
}

void MatchingEngine::metricsLoop() {
    uint64_t last_orders_processed = 0;
    uint64_t last_trades_executed = 0;
    uint64_t last_orders_dropped = 0;
    uint64_t last_trade_events_dropped = 0;
    uint64_t last_trade_events_delivered = 0;

    std::vector<uint64_t> snapshot_engine(LATENCY_BUCKETS, 0);
    std::vector<uint64_t> snapshot_network(LATENCY_BUCKETS, 0);

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

        // Snapshot histograms quickly to minimize contention window
        {
            std::lock_guard<std::mutex> lock(latency_mutex);
            std::copy(shared_engine_histogram.begin(), shared_engine_histogram.end(), snapshot_engine.begin());
            std::copy(shared_network_histogram.begin(), shared_network_histogram.end(), snapshot_network.begin());
            std::fill(shared_engine_histogram.begin(), shared_engine_histogram.end(), 0);
            std::fill(shared_network_histogram.begin(), shared_network_histogram.end(), 0);
        }

        auto calc_hist_percentile = [](const std::vector<uint64_t>& hist, double p) -> long long {
            uint64_t total_elements = 0;
            for (auto count : hist) total_elements += count;
            if (total_elements == 0) return 0;

            uint64_t target = static_cast<uint64_t>(total_elements * p);
            uint64_t accumulated = 0;
            for (size_t i = 0; i < hist.size(); ++i) {
                accumulated += hist[i];
                if (accumulated >= target) return i; // Returns calculated latency value directly in microseconds
            }
            return 0;
        };

        std::cout << "\n=== Engine Metrics (1s window) ===\n";
        std::cout << "Orders / sec  : " << ops << "\n";
        std::cout << "Trades / sec  : " << tps << "\n";
        std::cout << "Queue Drops   : " << drops << "\n";
        std::cout << "Kafka Events  : " << event_delivered << "/sec, drops=" << event_drops
                  << ", depth=" << kafkaOffloader.queueDepth() << "\n";
        
        std::cout << "Network Lat   : p50=" << calc_hist_percentile(snapshot_network, 0.50) << "us, p90=" 
                  << calc_hist_percentile(snapshot_network, 0.90) << "us, p99=" << calc_hist_percentile(snapshot_network, 0.99) << "us\n";

        std::cout << "Engine Lat    : p50=" << calc_hist_percentile(snapshot_engine, 0.50) << "us, p90=" 
                  << calc_hist_percentile(snapshot_engine, 0.90) << "us, p99=" << calc_hist_percentile(snapshot_engine, 0.99) << "us\n";
        std::cout << "==================================\n";
    }
}

void MatchingEngine::addOrder(const Order& order) {
    if (order.IsBuy) {
        auto& vec = book.buyOrders[order.price];
        vec.push_back(order);
        book.orderMap[order.OrderId] = {true, order.price, vec.size() - 1};
    } else {
        auto& vec = book.sellOrders[order.price];
        vec.push_back(order);
        book.orderMap[order.OrderId] = {false, order.price, vec.size() - 1};
    }
    matching();
}

void MatchingEngine::matching() {
    while (!book.buyOrders.empty() && !book.sellOrders.empty()) {
        auto bestBuyIt = book.buyOrders.begin();  // Highest Bid
        auto bestSellIt = book.sellOrders.begin(); // Lowest Ask

        if (bestBuyIt->first < bestSellIt->first) break;

        auto& buyVec = bestBuyIt->second;
        auto& sellVec = bestSellIt->second;

        // Vectors should never be empty if the key exists in the map
        if (buyVec.empty() || sellVec.empty()) {
            if (buyVec.empty()) book.buyOrders.erase(bestBuyIt);
            if (sellVec.empty()) book.sellOrders.erase(bestSellIt);
            continue;
        }

        // Target the front elements directly via array index 0
        Order& buy = buyVec.front();
        Order& sell = sellVec.front();

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
        
        trades_executed.fetch_add(1, std::memory_order_relaxed);

        buy.quantity -= tradedqty;
        sell.quantity -= tradedqty;

        // Consume the filled order off the front of its level. eraseAt refreshes the
        // stored positions of the orders behind it, which plain erase would leave stale.
        if (buy.quantity == 0) {
            book.orderMap.erase(buy.OrderId);
            book.eraseAt(buyVec, 0);
        }
        if (sell.quantity == 0) {
            book.orderMap.erase(sell.OrderId);
            book.eraseAt(sellVec, 0);
        }

        if (buyVec.empty()) book.buyOrders.erase(bestBuyIt);
        if (sellVec.empty()) book.sellOrders.erase(bestSellIt);
    }
}

void MatchingEngine::cancelOrder(int orderid) {
    auto it = book.orderMap.find(orderid);
    if (it == book.orderMap.end()) return;

    const OrderLocation loc = it->second;
    // Drop the tracking entry first so eraseAt only refreshes orders that survive.
    book.orderMap.erase(it);

    // Looked up rather than indexed: operator[] would materialise an empty level
    // for a price that no longer exists, only to erase it again below.
    if (loc.isBuy) {
        auto levelIt = book.buyOrders.find(loc.price);
        if (levelIt == book.buyOrders.end()) return;
        book.eraseAt(levelIt->second, loc.vector_index);
        if (levelIt->second.empty()) book.buyOrders.erase(levelIt);
    } else {
        auto levelIt = book.sellOrders.find(loc.price);
        if (levelIt == book.sellOrders.end()) return;
        book.eraseAt(levelIt->second, loc.vector_index);
        if (levelIt->second.empty()) book.sellOrders.erase(levelIt);
    }
}

void MatchingEngine::printTradeHistory() {}
void MatchingEngine::printOrderBook() {}