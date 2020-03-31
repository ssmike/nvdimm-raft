#pragma once

#include "buffer.h"
#include "endpoint_manager.h"
#include <memory>
#include "fwd.h"

namespace bus {

class TcpBus {
public:
    struct Options {
        int port = 80;
        size_t fixed_pool_size = 6;
        size_t listener_backlog = 60;
        size_t max_message_size = 4098;
    };

public:
    TcpBus(Options, BufferPool&, EndpointManager&);

    void set_handler(std::function<void(int, SharedView)>);

    void send(int dest, SharedView);

    void loop();

    ~TcpBus();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}
