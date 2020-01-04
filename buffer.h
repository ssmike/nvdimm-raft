#pragma once

#include "bus.h"

#include <stack>
#include <vector>

class BufferPool {
public:
    BufferPool(int start_size, int capacity)
        : start_size_(start_size)
    {
    }

    bus::GenericBuffer& take() {
        if (free_.empty()) {
            buffers_.emplace_back();
            buffers_.back().reserve(start_size_);
        }
    }

    void put(bus::GenericBuffer&) {
    }

private:
    std::vector<bus::GenericBuffer> buffers_;
    std::stack<size_t> free_;
    size_t start_size_;
};

class ScopedBuffer {
public:
    ScopedBuffer(BufferPool& pool)
        : buf_(pool.take())
        , pool_(pool)
    {
    }

    ~ScopedBuffer() {
        pool_.put(buf_);
    }

private:
    bus::GenericBuffer& buf_;
    BufferPool& pool_;
};
