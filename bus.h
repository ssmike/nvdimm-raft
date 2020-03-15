#pragma once

#include "connect_pool.h"
#include <memory>
#include "fwd.h"

namespace bus {

class BusError: public std::exception {
public:
    BusError(const char*);

    const char* what() const noexcept override {
        return message_;
    }

private:
    const char* message_;
};

class Throttler {
public:
    virtual bool accept_connection() = 0;
};

class TcpBus {
public:
    TcpBus(int port, size_t fixed_pool_size, ConnectPool& pool_);

    void set_throttler(Throttler&);

    void set_handler(std::function<void(int, GenericBuffer&)>);

    int register_endpoint(std::string addr, int port);

    void send(int dest, GenericBuffer);

    void loop();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}
