#pragma once

#include "fwd.h"
#include <stdint.h>
#include <cstddef>

namespace bus {

using GenericBuffer = std::vector<char, std::allocator<char>>;

class SocketHolder {
public:
    SocketHolder(int sock): sock_(sock) {}
    SocketHolder(): SocketHolder(kInvalidSocket) {}

    SocketHolder(const SocketHolder&) = delete;

    SocketHolder(SocketHolder&& oth)
        : SocketHolder(oth.release())
    {
    }

    int get() {
        return sock_;
    }

    int release() {
        int res = sock_;
        sock_ = kInvalidSocket;
        return res;
    }


    ~SocketHolder();

private:
    int sock_ = kInvalidSocket;
    static constexpr int kInvalidSocket = -1;
};

struct ConnData {
    GenericBuffer& in_buf;
    GenericBuffer& out_buf;
    SocketHolder socket;

    int dest;
};

class ConnectPool {
public:
    ConnectPool();

    void add(SocketHolder, uint64_t id, int hint);

    ConnData* select(uint64_t);
    ConnData* select(int hint);

    void set_available(uint64_t);

    size_t count_connections(int hint);
    size_t count_connections();

    void close(uint64_t);
    void close_old_conns();

private:
};

}
