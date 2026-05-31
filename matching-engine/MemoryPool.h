#pragma once

#include <vector>
#include <mutex>
#include <memory>
#include <stdexcept>

template <typename T>
class MemoryPool {
private:
    struct Slot {
        typename std::aligned_storage<sizeof(T), alignof(T)>::type memory;
        Slot* next;
    };

    std::mutex mutex_;
    Slot* free_head_;
    std::vector<std::unique_ptr<Slot[]>> blocks_;
    
    size_t block_size_;

    void allocate_block() {
        auto new_block = std::make_unique<Slot[]>(block_size_);
        for (size_t i = 0; i < block_size_ - 1; ++i) {
            new_block[i].next = &new_block[i + 1];
        }
        new_block[block_size_ - 1].next = free_head_;
        free_head_ = new_block.get();
        blocks_.push_back(std::move(new_block));
    }

public:
    explicit MemoryPool(size_t initial_capacity = 1024, size_t block_size = 1024)
        : free_head_(nullptr), block_size_(block_size) {
        if (block_size == 0) block_size_ = 1024;
        
        std::lock_guard<std::mutex> lock(mutex_);
        if (initial_capacity > 0) {
            // override temporarily to get initial capacity created in one chunk
            size_t original = block_size_;
            block_size_ = initial_capacity; 
            allocate_block();
            block_size_ = original;
        }
    }

    ~MemoryPool() {
        // Warning: if objects require destruction, manual sweeping might be 
        // needed if pointers were not gracefully returned to the pool and destructed by callers.
    }

    template <typename... Args>
    T* allocate(Args&&... args) {
        Slot* slot = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!free_head_) {
                allocate_block();
            }
            slot = free_head_;
            free_head_ = free_head_->next;
        }
        
        // Construct the object in the reserved aligned memory space using placement new
        return new (&slot->memory) T(std::forward<Args>(args)...);
    }

    void deallocate(T* ptr) {
        if (!ptr) return;

        // Explicitly destroy the object
        ptr->~T();

        // Interpret ptr location as Slot and add back to the free list
        Slot* slot = reinterpret_cast<Slot*>(ptr);
        
        std::lock_guard<std::mutex> lock(mutex_);
        slot->next = free_head_;
        free_head_ = slot;
    }
};
