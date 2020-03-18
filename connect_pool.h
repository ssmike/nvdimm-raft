#pragma once

#include "fwd.h"
#include "buffer.h"

#include <optional>
#include <stdint.h>
#include <cstddef>
#include <utility>

namespace bus {

class SocketHolder {
public:
    SocketHolder(int sock): sock_(sock) {}
    SocketHolder(): SocketHolder(kInvalidSocket) {}

    SocketHolder(const SocketHolder&) = delete;

    SocketHolder(SocketHolder&& oth)
        : SocketHolder(oth.release())
    {
    }

    void operator =(SocketHolder&& oth) {
        std::swap(sock_, oth.sock_);
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
    std::optional<SharedView> egr_message;
    uint64_t egr_offset = 0;

    ScopedBuffer ingr_buf;
    size_t ingr_offset = 0;

    SocketHolder socket;

    int dest;
    uint64_t id;
};

class ConnectPool {
public:
    ConnectPool();

    size_t make_id();

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
