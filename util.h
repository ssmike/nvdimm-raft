#pragma once

#include <condition_variable>
#include <mutex>
#include <atomic>
#include <optional>
#include <functional>

namespace bus::internal {

class Event {
public:
    Event() {
    }

    void reset() {
        notify();
        {
            std::unique_lock<std::mutex> lock(mutex_);
            event_set_.store(true);
        }
    }

    void notify() {
        bool val = true;
        event_set_.exchange(val);
        if (!val) {
          std::unique_lock<std::mutex> lock(mutex_);
          cv_.notify_all();
        }
    }

    bool set() const {
        return event_set_.load();
    }

    void wait() {
        if (!set()) {
          std::unique_lock<std::mutex> lock(mutex_);
          cv_.wait(lock, [&] { return event_set_.load(); });
        }
    }

private:
    std::atomic<bool> event_set_ = false;

    std::mutex mutex_;
    std::condition_variable cv_;
};

template<typename T>
class ExclusiveWrapper {
public:
    class Guard {
    public:
        Guard(T& value, std::unique_lock<std::mutex> lock)
            : value_(value)
            , lock_(std::move(lock))
        {
        }

        T& operator * () {
            return value_;
        }

        const T& operator * () const {
            return value_;
        }

        T* operator -> () {
            return &value_;
        }

        const T* operator -> () const {
            return &value_;
        }

    private:
        T& value_;
        std::unique_lock<std::mutex> lock_;
    };

public:
    template<typename... Args>
    ExclusiveWrapper(Args... args)
        : value_(std::forward<Args>(args)...)
    {
    }

    Guard get() {
        return Guard(value_, std::unique_lock(mutex_));
    }

private:
    T value_;
    std::mutex mutex_;
};

template<typename T>
class ErrorT {
private:
    class FailureTag {};
    ErrorT(FailureTag, std::string what)
        : message_(std::move(what))
    {
    }

public:
    template<typename... Args>
    ErrorT(Args&&... args) {
        value_.emplace(std::forward<Args>(args)...);
    }

    template<typename... Args>
    static ErrorT<T> error(Args&&... args) {
        return ErrorT(ErrorT<T>::FailureTag(), std::forward<Args>(args)...);
    }

    operator bool () const {
        return value_;
    }

    const char* what() {
        return message_.data();
    }

    T& unwrap() {
        if (*this) {
            throw std::runtime_error(message_);
        } else {
            return *value_;
        }
    }

private:
    std::optional<T> value_;
    std::string message_;
};

template<typename T>
class FutureState {
public:
    template<typename... Args>
    void set_value(Args... args) {
        std::vector<std::function<void(T&)>> to_call;
        if (!evt_.set()) {
            std::unique_lock lock(mutex_);
            if (value_) {
                throw std::logic_error("double FutureState::set_value");
            }
            value_.emplace(std::forward<Args>(args)...);
            to_call = std::move(callbacks_);
        } else {
            throw std::logic_error("double FutureState::set_value");
        }
        evt_.notify();
        for (auto& cb : to_call) {
            try {
                cb(get());
            } catch (...) {
            }
        }
    }

    template<typename Func>
    void apply(Func f) {
        if (evt_.set()) {
            f(get());
        } else {
            std::unique_lock<std::mutex> lock(mutex_);
            if (value_) {
                lock.unlock();
                f(get());
            } else {
                callbacks_.push_back(std::move(f));
            }
        }
    }

    T& get() {
        if (!evt_.set()) {
            throw std::logic_error("value not set");
        }
        return *value_;
    }

private:
    Event evt_;
    std::mutex mutex_;
    std::optional<T> value_;
    std::vector<std::function<void(T&)>> callbacks_;
};

template<typename T>
class Promise;

template<typename T>
class Future {
public:
    Future(std::shared_ptr<FutureState<T>> state) : state_(std::move(state)) {}
    Future(const Future<T>&) = default;
    Future(Future<T>&&) = default;

    T& get() {
        return state_->get();
    }

    template<typename Func>
    void subscribe(Func f) {
        state_->apply(std::move(f));
    }

    template<typename Func>
    Future<std::invoke_result_t<Func, T&>> map(Func f) {
        Promise<Future<std::invoke_result_t<Func, T&>>> result;
        state_->apply([f=std::move(f), result] (T& t) {
                result.set_value(f(t));
            });
        state_->apply(std::move(f));
        return result;
    }

    template<typename Func>
    Future<ErrorT<std::invoke_result_t<Func, T&>>> apply(Func f) {
        using return_t = std::invoke_result_t<Func, T&>;
        Promise<Future<std::invoke_result_t<Func, T&>>> result;
        state_->apply([f=std::move(f), result] (T& t) {
                try {
                    result.set_value(f(t));
                } catch(const std::exception& e) {
                    result.set_value(ErrorT<return_t>::error(e.what()));
                }
            });
        state_->apply(std::move(f));
        return result;
    }

private:
    std::shared_ptr<FutureState<T>> state_;
};

template<typename T>
class Promise {
public:
    Promise() : state_(new FutureState<T>()) {}

    Promise(const Promise<T>&) = default;
    Promise(Promise<T>&&) = default;

    template<typename... Args>
    void set_value(Args... args) {
        state_->set_value(std::forward<Args>(args)...);
    }

private:
    std::shared_ptr<FutureState<T>> state_;
};

constexpr size_t header_len = 8;

void write_header(size_t size, char* buf);

size_t read_header(char* buf);

}
