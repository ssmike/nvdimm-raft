#pragma once

#include "bus.h"

#include <vector>
#include <functional>

namespace bus {

class Service {
public:
    template<typename ResponseProto>
    class ResponseHandle {
    public:
        void send() {
        }

    private:
    };

public:
    Service(TcpBus& bus)
        : bus_(bus)
    {
        bus_.set_handler([=](auto d, auto v) { this->handle(d, v); });
    }

protected:
    template<typename RequestProto, typename ResponseProto>
    void register_handler() {
    }

private:
    void handle(int dest, SharedView view) {
    }

private:
    TcpBus& bus_;
    std::vector<std::function<void(int, SharedView)>> handlers_;
};

}
