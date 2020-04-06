#pragma once

#include <mutex>
#include <memory>

namespace bus::internal {

template<typename T>
class ExclusiveGuard {
public:
    ExclusiveGuard(T& value, std::unique_lock<std::mutex> lock)
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

template<typename T>
class ExclusiveGuard<std::unique_ptr<T>> {
public:
    ExclusiveGuard(std::unique_ptr<T>& value, std::unique_lock<std::mutex> lock)
        : value_(*value)
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

template<typename T>
class ExclusiveWrapper {
public:

public:
    template<typename... Args>
    ExclusiveWrapper(Args&&... args)
        : value_(std::forward<Args>(args)...)
    {
    }

    ExclusiveGuard<T> get() {
        return ExclusiveGuard(value_, std::unique_lock(mutex_));
    }

private:
    T value_;
    std::mutex mutex_;
};

}
