#include "endpoint_manager.h"

#include "error.h"

#include <sstream>
#include <cstring>
#include <unordered_map>
#include <vector>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <errno.h>

namespace bus {

struct SockaddrHash {
    size_t operator()(const sockaddr_in6& addr) const {
        std::string_view view(reinterpret_cast<const char*>(&addr), sizeof(addr));
        return std::hash<std::string_view>()(view);
    }
};

struct SockaddrCompare {
    bool operator() (const sockaddr_in6& addr1, const sockaddr_in6& addr2) const {
        return memcmp(&addr1, &addr2, sizeof(struct sockaddr_in6)) == 0;
    }
};

void set_nodelay(int socket) {
  int flags = 1;
  CHECK_ERRNO(
      setsockopt(socket, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof(flags)) == 0);
}

class EndpointManager::Impl {
public:
    int resolve(sockaddr_in6* addr) {
        if (resolve_map_.find(*addr) != resolve_map_.end()) {
            return resolve_map_[*addr];
        }
        int result = resolve_map_.size();
        resolve_map_[*addr] = result;
        if (result >= endpoints_.size()) {
            endpoints_.resize(result + 1);
        }
        endpoints_[result] = *addr;
        return result;
    }

    SocketHolder async_connect(int dest) {
        if (dest > endpoints_.size()) {
            throw BusError("invalid endpoint");
        }
        SocketHolder sock = socket(AF_INET6, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        CHECK_ERRNO(sock.get() >= 0);
        sockaddr_in6* addr = &endpoints_[dest];
        int status = connect(sock.get(), reinterpret_cast<sockaddr*>(addr), sizeof(sockaddr_in6));
        CHECK_ERRNO(status == 0 || errno == EINPROGRESS || errno == EINTR);
        set_nodelay(sock.get());
        return sock;
    }

    EndpointManager::IncomingConnection accept(int listensock) {
        sockaddr_in6 addr;
        socklen_t addrlen = sizeof(addr);
        IncomingConnection conn {
            .sock_ = accept4(listensock, reinterpret_cast<struct sockaddr*>(&addr), &addrlen, SOCK_NONBLOCK | SOCK_CLOEXEC),
        };
        conn.errno_ = errno;
        if (conn.sock_.get() >= 0 && addr.sin6_family == AF_INET6 && addrlen == sizeof(addr)) {
            conn.endpoint_ = resolve(&addr);
            return conn;
        } else {
            conn.sock_ = SocketHolder();
        }
        return conn;
    }

public:
    std::unordered_map<sockaddr_in6, int, SockaddrHash, SockaddrCompare> resolve_map_;
    std::vector<sockaddr_in6> endpoints_;
};

EndpointManager::EndpointManager()
    : impl_(new Impl())
{
}

int EndpointManager::register_endpoint(std::string addr, int port) {
    std::string service;
    std::stringstream ss;
    ss << port;
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET6;
    struct addrinfo* info;
    int res = getaddrinfo(addr.c_str(), service.c_str(), &hints, &info);
    if (res != 0) {
        throw BusError(gai_strerror(res));
    }
    std::optional<int> result = std::nullopt;
    for (addrinfo* i = info; i != nullptr; i = i->ai_next) {
        auto addr = reinterpret_cast<sockaddr_in6*>(info->ai_addr);
        if (info->ai_family == AF_INET6) {
            if (!result) {
                result = impl_->resolve(addr);
            }
            impl_->resolve_map_[*addr] = result.value();
        }
    }
    freeaddrinfo(info);
    if (!result.has_value()) {
        throw BusError("no suitable address found");
    }
    return result.value();

}

SocketHolder EndpointManager::async_connect(int dest) {
    return impl_->async_connect(dest);
}

EndpointManager::IncomingConnection EndpointManager::accept(int listen_socket) {
    return impl_->accept(listen_socket);
}

EndpointManager::~EndpointManager() = default;

}
