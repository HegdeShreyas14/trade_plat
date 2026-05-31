#pragma once

#include <atomic>
#include <cstdint>
#include <cstddef>
#include <stdexcept>

// Hardware interference size is often 64 bytes. We use it to pad
// atomics to avoid false sharing between the producer and consumer.
#ifdef __cpp_lib_hardware_interference_size
    constexpr std::size_t CACHE_LINE_SIZE = std::hardware_destructive_interference_size;
#else
    constexpr std::size_t CACHE_LINE_SIZE = 64;
#endif

template<typename T>
class MPSCQueue {
private:
    struct Cell {
        std::atomic<size_t> sequence;
        T data;
    };

    // Pad members to avoid false sharing
    alignas(CACHE_LINE_SIZE) const size_t buffer_mask_;
    Cell* const buffer_;

    alignas(CACHE_LINE_SIZE) std::atomic<size_t> enqueue_pos_;
    alignas(CACHE_LINE_SIZE) std::atomic<size_t> dequeue_pos_;

public:
    explicit MPSCQueue(size_t capacity)
        : buffer_mask_(capacity - 1)
        , buffer_(new Cell[capacity])
    {
        // Capacity must be a power of 2
        if ((capacity < 2) || ((capacity & (capacity - 1)) != 0)) {
            throw std::invalid_argument("Capacity must be a power of 2");
        }

        for (size_t i = 0; i < capacity; ++i) {
            buffer_[i].sequence.store(i, std::memory_order_relaxed);
        }
        enqueue_pos_.store(0, std::memory_order_relaxed);
        dequeue_pos_.store(0, std::memory_order_relaxed);
    }

    ~MPSCQueue() {
        delete[] buffer_;
    }

    // Delete copy/move constructors and assignment operators
    MPSCQueue(const MPSCQueue&) = delete;
    MPSCQueue& operator=(const MPSCQueue&) = delete;
    MPSCQueue(MPSCQueue&&) = delete;
    MPSCQueue& operator=(MPSCQueue&&) = delete;

    bool enqueue(const T& data) {
        Cell* cell;
        size_t pos = enqueue_pos_.load(std::memory_order_relaxed);
        for (;;) {
            cell = &buffer_[pos & buffer_mask_];
            size_t seq = cell->sequence.load(std::memory_order_acquire);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
            
            if (dif == 0) {
                // If it's our turn, try to claim the spot
                if (enqueue_pos_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                return false; // Queue is full
            } else {
                pos = enqueue_pos_.load(std::memory_order_relaxed);
            }
        }
        
        cell->data = data;
        // Release memory order ensures the item writing completes before sequence increment
        cell->sequence.store(pos + 1, std::memory_order_release);
        return true;
    }

    bool dequeue(T& data) {
        Cell* cell;
        size_t pos = dequeue_pos_.load(std::memory_order_relaxed);
        cell = &buffer_[pos & buffer_mask_];
        
        size_t seq = cell->sequence.load(std::memory_order_acquire);
        intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);
        
        if (dif == 0) {
            data = cell->data;
            dequeue_pos_.store(pos + 1, std::memory_order_relaxed); // Single consumer, relax is fine
            cell->sequence.store(pos + buffer_mask_ + 1, std::memory_order_release);
            return true;
        }
        
        return false; // Queue is empty
    }
};
