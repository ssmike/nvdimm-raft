#pragma once

#include "lock.h"
#include "future.h"

#include <condition_variable>
#include <thread>
#include <chrono>
#include <map>

namespace bus::internal {

class DelayedExecutor {
public:
    DelayedExecutor()
        : thread_()
    {
    }

    DelayedExecutor(const DelayedExecutor&) = delete;
    DelayedExecutor(DelayedExecutor&&) = delete;

    void schedule(std::function<void()> what, std::chrono::duration<double> when) {
        auto deadline = std::chrono::time_point_cast<std::chrono::system_clock::time_point::duration>(std::chrono::system_clock::now() + when);
        actions_.get()->insert({ deadline, std::move(what) });
    }

    void schedule(std::function<void()> what, std::chrono::time_point<std::chrono::system_clock> when) {
        actions_.get()->insert({ when, std::move(what) });
    }

    ~DelayedExecutor() {
        shot_down_.store(true);
        ready_.notify();
    }

private:
    void execute() {
        while (!shot_down_.load()) {
            std::function<void()> to_execute;
            std::optional<std::chrono::system_clock::time_point> wait_until;
            {
                auto actions = actions_.get();
                ready_.reset();
                to_execute = std::move(actions->begin()->second);
                actions->erase(actions->begin());

                if (!actions->empty()) {
                    wait_until = actions->begin()->first;
                }
            }
            if (to_execute) {
                try {
                    to_execute();
                } catch (...) {
                }
            }
            if (!shot_down_.load()) {
                if (wait_until) {
                    ready_.wait_until(*wait_until);
                } else {
                    ready_.wait();
                }
            }
        }
    }

private:
    std::thread thread_;
    ExclusiveWrapper<std::multimap<std::chrono::system_clock::time_point, std::function<void()>>> actions_;
    Event ready_;
    std::atomic_bool shot_down_ = false;
};

}
