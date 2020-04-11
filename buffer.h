#pragma once

#include "fwd.h"

#include "lock.h"

#include <atomic>
#include <limits>
#include <memory>
#include <stack>
#include <string_view>
#include <vector>

namespace bus {


class BufferPool {
public:
    static constexpr size_t kInvalidBuffer = std::numeric_limits<size_t>::max();

public:
    struct Buffer {
        std::vector<char> data;
        std::atomic<uint64_t> offset = 0;
        std::atomic<int64_t> usage_counter = 0;

        size_t number;
    };

    struct DataPtr {
        Buffer* buffer = nullptr;
        uint64_t offset = 0;

        operator bool () {
            return buffer;
        }
    };

public:
    BufferPool(size_t size)
        : size_(size)
    {
    }

    DataPtr take(size_t size) {
        if (auto result = try_fetch(size, head_.load())) {
            return result;
        } else {
            auto state = state_.get();
            Buffer* new_buffer;
            if (state->free_.empty()) {
                auto buffer = std::make_unique<Buffer>();
                buffer->data.reserve(size_);
                buffer->data.resize(size_);
                buffer->number = state->buffers_.size();
                new_buffer = buffer.get();

                state->buffers_.push_back(std::move(buffer));
            } else {
                new_buffer = state->buffers_[state->free_.top()].get();
                state->free_.pop();
            }
            new_buffer->offset.store(0);

            result = try_fetch(size, new_buffer);
            head_.store(state->buffers_.back().get());

            if (!result) {
                throw std::runtime_error("bad buffer alloc");
            } else {
                return result;
            }
        }
    }

    void put(DataPtr ptr) {
        if (ptr && ptr.buffer->usage_counter.fetch_sub(1) == 1) {
            state_.get()->free_.push(ptr.buffer->number);
        }
    }

private:
    DataPtr try_fetch(size_t size, Buffer* buffer) {
        if (!buffer) {
            return {};
        }
        DataPtr result {
            .buffer = buffer,
            .offset = buffer->offset.fetch_add(size)
        };
        if (result.offset + size > buffer->data.size()) {
            return {};
        } else {
            buffer->usage_counter.fetch_add(1);
            return result;
        }
    }

private:
    struct State {
        std::vector<std::unique_ptr<Buffer>> buffers_;
        std::stack<size_t> free_;
    };

    internal::ExclusiveWrapper<State> state_;
    const size_t size_;
    std::atomic<Buffer*> head_ = nullptr;
};

class SharedView {
public:
    SharedView() = default;

    SharedView(BufferPool& pool, size_t size)
        : pool_(&pool)
        , ptr_(pool.take(size))
        , size_(size)
    {
        data_ = ptr_.buffer->data.data() + ptr_.offset;
    }

    SharedView(const SharedView& oth)
        : SharedView()
    {
        mem_copy(oth);
        ref();
    }

    void operator = (const SharedView& oth) {
        mem_copy(oth);
        ref();
    }

    SharedView(SharedView&& oth) {
        mem_swap(oth);
    }

    ~SharedView() {
        unref();
    }

    bool initialized() {
        return pool_;
    }

    SharedView slice(size_t start, size_t size) const {
        SharedView result = *this;
        result.data_ = data_ + start;
        result.size_ = size;
        return result;
    }

    SharedView skip(size_t start) const {
        return slice(start, size() - start);
    }

    SharedView resize(size_t size) {
        return slice(0, size);
    }

    char* data() const {
        return data_;
    }

    size_t size() const {
        return size_;
    }

    std::string_view view() const {
        return { data_, size_ };
    }

private:
    void mem_reset() {
        pool_ = nullptr;
        ptr_ = {};
        size_ = 0;
    }

    void unref() {
        if (pool_) {
            pool_->put(ptr_);
        }
    }

    void ref() {
        if (pool_) {
            ptr_.buffer->usage_counter.fetch_add(1);
        }
    }

    void mem_copy(const SharedView& oth) {
        std::tie(pool_, ptr_, data_, size_) = std::tie(oth.pool_, oth.ptr_, oth.data_, oth.size_);
    }

    void mem_swap(SharedView& view) {
        std::swap(pool_, view.pool_);
        std::swap(ptr_, view.ptr_);
        std::swap(data_, view.data_);
        std::swap(size_, view.size_);
    }

private:
    BufferPool* pool_ = nullptr;
    BufferPool::DataPtr ptr_;
    char* data_ = nullptr;
    size_t size_ = 0;
};

}
