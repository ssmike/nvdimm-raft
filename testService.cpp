#include "proto_bus.h"
#include "bus.h"
#include "util.h"


using namespace bus;

int main() {
    BufferPool bufferPool{4098};
    EndpointManager manager;

    ProtoBus second(TcpBus::Options{.port = 4002, .fixed_pool_size = 2}, manager, bufferPool);
}
