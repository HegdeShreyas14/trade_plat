#pragma once
#include "trade.h"
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include <array>
#include "OrderBook.h"
#include "MPSCQueue.h"
#include "KafkaOffloader.h"

// Invariant telemetry space: Matches the 100ms max metric resolution window
constexpr std::size_t H_BUCKETS = 100000;

class MatchingEngine
{
private:
    OrderBook book;
    std::vector<Trade> tradeHistory;
    std::atomic<bool> isRunning{false};
    std::thread workerThread;
    std::thread metricsThread;

    MPSCQueue<Order> incomingOrders{65536};
    KafkaOffloader kafkaOffloader;
    uint64_t next_match_id{0};

    // Lock-Free Aggregators (Relaxed Atomics)
    std::atomic<uint64_t> orders_processed{0};
    std::atomic<uint64_t> trades_executed{0};
    std::atomic<uint64_t> orders_dropped{0};
    std::atomic<uint64_t> trade_events_dropped{0};
    
    // Core telemetry memory boundaries
    std::mutex latency_mutex;
    
    // Padded/Aligned fixed histograms to prevent cache thrashing during metrics snapshot dumps
    alignas(64) std::array<uint64_t, H_BUCKETS> shared_engine_histogram{};
    alignas(64) std::array<uint64_t, H_BUCKETS> shared_network_histogram{};

public:
    MatchingEngine() = default;
    ~MatchingEngine();

    // Structural Lifecycles
    void start();
    void stop();
    void processLoop();
    void metricsLoop();

    // Hot-Path Execution Ingress
    void enqueueOrder(const Order& order);
    void addOrder(const Order& order);
    void matching();
    void cancelOrder(int orderId);
    
    // Debug & Validation Stubs
    void printTradeHistory();
    void printOrderBook();
};