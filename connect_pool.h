#pragma once

#include "fwd.h"
#include <stdint.h>
#include <cstddef>

namespace bus {

using GenericBuffer = std::vector<char, std::allocator<char>>;

struct ConnData {
    enum State {
        kReading = 0,
        kWriting = 1,
        kIdle = 2,
    } state;

    GenericBuffer& buf;
};

class SocketHolder {
public:
    explicit SocketHolder(int sock): sock_(sock) {}
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

class ConnectPool {
public:
    ConnectPool(size_t ingr_size, size_t egr_size);

    void add_ingress(int fd, uint64_t desc);
    void add_egress(int fd, uint64_t desc, int hint);

    void get(uint64_t desc);

    void close(int fd);
    void close_old_conns();

private:
};

}
