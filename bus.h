#pragma once

#include "buffer.h"
#include "connect_pool.h"
#include "endpoint_manager.h"
#include <memory>
#include "fwd.h"

namespace bus {

class TcpBus {
public:
    TcpBus(int port, size_t fixed_pool_size, ConnectPool&, BufferPool&, EndpointManager&);

    void set_handler(std::function<void(int, SharedView)>);

    void send(int dest, SharedView);

    void loop();

    ~TcpBus();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}
