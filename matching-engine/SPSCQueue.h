#pragma once

#include <atomic>
#include <cstddef>
#include <stdexcept>

constexpr std::size_t SPSC_CACHE_LINE_SIZE = 64;

template<typename T>
class SPSCQueue {
private:
    alignas(SPSC_CACHE_LINE_SIZE) const size_t capacity_;
    const size_t mask_;
    T* const buffer_;

    alignas(SPSC_CACHE_LINE_SIZE) std::atomic<size_t> head_{0};
    alignas(SPSC_CACHE_LINE_SIZE) std::atomic<size_t> tail_{0};

public:
    explicit SPSCQueue(size_t capacity)
        : capacity_(capacity)
        , mask_(capacity - 1)
        , buffer_(new T[capacity])
    {
        if ((capacity < 2) || ((capacity & (capacity - 1)) != 0)) {
            throw std::invalid_argument("SPSCQueue capacity must be a power of 2");
        }
    }

    ~SPSCQueue() {
        delete[] buffer_;
    }

    SPSCQueue(const SPSCQueue&) = delete;
    SPSCQueue& operator=(const SPSCQueue&) = delete;
    SPSCQueue(SPSCQueue&&) = delete;
    SPSCQueue& operator=(SPSCQueue&&) = delete;

    bool tryPush(const T& value) {
        size_t head = head_.load(std::memory_order_relaxed);
        size_t next = head + 1;
        if (next - tail_.load(std::memory_order_acquire) > capacity_) {
            return false;
        }

        buffer_[head & mask_] = value;
        head_.store(next, std::memory_order_release);
        return true;
    }

    bool tryPop(T& value) {
        size_t tail = tail_.load(std::memory_order_relaxed);
        if (tail == head_.load(std::memory_order_acquire)) {
            return false;
        }

        value = buffer_[tail & mask_];
        tail_.store(tail + 1, std::memory_order_release);
        return true;
    }

    size_t approxSize() const {
        return head_.load(std::memory_order_acquire) - tail_.load(std::memory_order_acquire);
    }
};
