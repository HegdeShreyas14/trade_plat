#pragma once
#include "trade.h"
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include "OrderBook.h"
#include "MPSCQueue.h"
#include "KafkaOffloader.h"

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

    // Metrics
    std::atomic<uint64_t> orders_processed{0};
    std::atomic<uint64_t> trades_executed{0};
    std::atomic<uint64_t> orders_dropped{0};
    std::atomic<uint64_t> trade_events_dropped{0};
    
    std::mutex latency_mutex;
    std::vector<long long> engine_latencies;
    std::vector<long long> network_latencies;

public:
    MatchingEngine() = default;
    ~MatchingEngine();

    void start();
    void stop();
    void processLoop();
    void metricsLoop();

    void enqueueOrder(const Order& order);

    void addOrder(const Order& order);
    void matching();
    void cancelOrder(int orderId);
    void printTradeHistory();
    void printOrderBook();
};
