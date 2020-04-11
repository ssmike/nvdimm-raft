#pragma once

#include "fwd.h"

#include "lock.h"

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
    BufferPool(int start_size)
        : start_size_(start_size)
    {
    }

    std::tuple<size_t, GenericBuffer*> take() {
        auto state = state_.get();
        if (state->free_.empty()) {
            state->buffers_.emplace_back(new GenericBuffer());
            state->buffers_.back()->reserve(start_size_);
            return { state->buffers_.size() - 1, state->buffers_.back().get() };
        } else {
            size_t result = state->free_.top();
            state->free_.pop();
            return { result, state->buffers_[result].get() };
        }
    }

    void put(size_t num) {
        if (num != kInvalidBuffer) {
            state_.get()->free_.push(num);
        }
    }

private:
    struct State {
        std::vector<std::unique_ptr<bus::GenericBuffer>> buffers_;
        std::stack<size_t> free_;
    };

    internal::ExclusiveWrapper<State> state_;
    const size_t start_size_;
};

class ScopedBuffer {
public:
    ScopedBuffer() = default;

    ScopedBuffer(BufferPool& pool)
        : pool_(&pool)
    {
        std::tie(num_, buf_) = pool_->take();
    }

    ScopedBuffer(const ScopedBuffer&) = delete;

    ScopedBuffer(ScopedBuffer&& other)
    {
        std::swap(pool_, other.pool_);
        std::swap(buf_, other.buf_);
        std::swap(num_, other.num_);
    }

    void operator = (ScopedBuffer&& other) {
        std::swap(pool_, other.pool_);
        std::swap(buf_, other.buf_);
        std::swap(num_, other.num_);
    }

    bool initialized() {
        return pool_ != nullptr;
    }

    GenericBuffer& get() {
        return *buf_;
    }

    ~ScopedBuffer() {
        if (pool_) {
            pool_->put(num_);
        }
    }

private:
    size_t num_ = BufferPool::kInvalidBuffer;
    GenericBuffer* buf_ = nullptr;
    BufferPool* pool_ = nullptr;
};

class SharedView {
public:
    SharedView(ScopedBuffer buf) {
        std::unique_ptr<ScopedBuffer> ptr(new ScopedBuffer(std::move(buf)));
        buf_ = std::move(ptr);
        view_ = {buf_->get().data(), buf_->get().size()};
    }

    SharedView(const SharedView&) = default;

    SharedView slice(size_t start, size_t size) const {
        SharedView result = *this;
        result.view_ = { view_.data() + start, size };
        return result;
    }

    SharedView skip(size_t start) const {
        return slice(start, get().size() - start);
    }

    SharedView resize(size_t size) {
        return slice(0, size);
    }

    const char* data() const {
        return view_.data();
    }

    size_t size() const {
        return view_.size();
    }

    std::string_view get() const {
        return view_;
    }

private:
    std::shared_ptr<ScopedBuffer> buf_;
    std::string_view view_;
};

}
