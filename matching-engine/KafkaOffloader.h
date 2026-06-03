#pragma once

#include "MarketEvents.h"
#include "SPSCQueue.h"
#include <atomic>
#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>
#include <chrono>
#include <emmintrin.h>
#include <pthread.h>
#include <sched.h>

#ifdef ENABLE_KAFKA
#include <rdkafkacpp.h>
#include <memory>
#endif

class KafkaOffloader {
private:
    static constexpr size_t QUEUE_CAPACITY = 1 << 16;
    static constexpr size_t MAX_BATCH_EVENTS = 1024;

    SPSCQueue<TradeEvent> queue_{QUEUE_CAPACITY};
    std::atomic<bool> running_{false};
    std::thread worker_;

    std::atomic<uint64_t> published_{0};
    std::atomic<uint64_t> dropped_{0};
    std::atomic<uint64_t> delivered_{0};

#ifdef ENABLE_KAFKA
    std::unique_ptr<RdKafka::Producer> producer_;
    std::unique_ptr<RdKafka::Topic> topic_;
    std::unique_ptr<RdKafka::Conf> conf_;
#endif

public:
    KafkaOffloader() = default;
    ~KafkaOffloader() {
        stop();
    }

    bool start(const std::string& brokers = "localhost:9092", const std::string& topic = "trade-events") {
#ifdef ENABLE_KAFKA
        std::string errstr;
        conf_.reset(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
        if (!conf_ ||
            conf_->set("bootstrap.servers", brokers, errstr) != RdKafka::Conf::CONF_OK ||
            conf_->set("linger.ms", "1", errstr) != RdKafka::Conf::CONF_OK ||
            conf_->set("acks", "1", errstr) != RdKafka::Conf::CONF_OK ||
            conf_->set("compression.codec", "none", errstr) != RdKafka::Conf::CONF_OK) {
            return false;
        }

        producer_.reset(RdKafka::Producer::create(conf_.get(), errstr));
        if (!producer_) return false;

        topic_.reset(RdKafka::Topic::create(producer_.get(), topic, nullptr, errstr));
        if (!topic_) return false;
#else
        (void)brokers;
        (void)topic;
#endif

        running_.store(true, std::memory_order_release);
        worker_ = std::thread(&KafkaOffloader::run, this);
        return true;
    }

    void stop() {
        running_.store(false, std::memory_order_release);
        if (worker_.joinable()) worker_.join();

#ifdef ENABLE_KAFKA
        if (producer_) producer_->flush(1000);
#endif
    }

    bool publish(const TradeEvent& event) {
        if (!queue_.tryPush(event)) {
            dropped_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        published_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    uint64_t published() const {
        return published_.load(std::memory_order_relaxed);
    }

    uint64_t dropped() const {
        return dropped_.load(std::memory_order_relaxed);
    }

    uint64_t delivered() const {
        return delivered_.load(std::memory_order_relaxed);
    }

    size_t queueDepth() const {
        return queue_.approxSize();
    }

private:
    void run() {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(3, &cpuset);
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

        std::array<TradeEvent, MAX_BATCH_EVENTS> batch{};
        while (running_.load(std::memory_order_acquire) || queueDepth() > 0) {
            size_t count = 0;
            while (count < batch.size() && queue_.tryPop(batch[count])) {
                ++count;
            }

            if (count == 0) {
                _mm_pause();
                continue;
            }

            publishBatch(batch.data(), count);
            delivered_.fetch_add(count, std::memory_order_relaxed);
        }
    }

    void publishBatch(const TradeEvent* events, size_t count) {
#ifdef ENABLE_KAFKA
        const char* bytes = reinterpret_cast<const char*>(events);
        size_t byte_count = count * sizeof(TradeEvent);
        producer_->produce(
            topic_.get(),
            RdKafka::Topic::PARTITION_UA,
            RdKafka::Producer::RK_MSG_COPY,
            const_cast<char*>(bytes),
            byte_count,
            nullptr,
            nullptr
        );
        producer_->poll(0);
#else
        (void)events;
        (void)count;
#endif
    }
};
