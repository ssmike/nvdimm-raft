#include "bus.h"
#include "util.h"
#include "messages.pb.h"

#include <thread>

using namespace bus;

int main() {
    BufferPool bufferPool{4098};
    EndpointManager manager;

    TcpBus second(TcpBus::Options{.port = 4002, .fixed_pool_size = 2}, bufferPool, manager);

    std::thread t([&] {
        BufferPool bufferPool{4098};
        EndpointManager manager;
        TcpBus first(TcpBus::Options{.port = 4001, .fixed_pool_size = 2}, bufferPool, manager);

        first.set_handler([&](int endp, SharedView view) {
                Operation op2;
                op2.ParseFromArray(view.data(), view.size());
                assert(op2.key() == "key");
                assert(op2.data() == "data");

                assert(false);
            });

        first.loop();
    });

    Operation op;
    op.set_data("data");
    op.set_key("key");

    ScopedBuffer buffer{bufferPool};
    buffer.get().resize(op.GetCachedSize());
    op.SerializeToArray(buffer.get().data(), buffer.get().size());

    second.send(manager.register_endpoint("::1", 4001), std::move(buffer));

    second.loop();

    t.join();
}
