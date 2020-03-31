#pragma once

#include <condition_variable>
#include <mutex>
#include <atomic>


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

    void wait() {
        if (!event_set_.load()) {
          std::unique_lock<std::mutex> lock(mutex_);
          cv_.wait(lock, [&] { return event_set_.load(); });
        }
    }

private:
    std::atomic<bool> event_set_ = false;

    std::mutex mutex_;
    std::condition_variable cv_;
};
