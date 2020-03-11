#include "bus.h"

#include "string.h"
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/tcp.h>
#include <unistd.h>

#include <string_view>
#include <sstream>
#include <functional>
#include <string>
#include <unordered_map>
#include <map>
#include <vector>

namespace bus {

void throw_errno() {
    throw BusError(strerror(errno));
}

#define CHECK_ERRNO(x) if (!(x)) throw_errno();

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

class TcpBus::Impl {
public:
    Impl(int port, ConnectPool& pool)
        : pool_(pool)
    {
      listensock_ = socket(AF_INET6, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
                           IPPROTO_TCP);
      CHECK_ERRNO(listensock_ >= 0);
      sockaddr_in6 addr;
      addr.sin6_addr = in6addr_any;
      addr.sin6_port = htons(port);
      CHECK_ERRNO(bind(listensock_, reinterpret_cast<struct sockaddr *>(&addr),
                       sizeof(addr)));

      epollfd_ = epoll_create1(EPOLL_CLOEXEC);
      CHECK_ERRNO(epollfd_ >= 0);

      {
          epoll_event evt;
          evt.events = EPOLLIN;
          evt.data.u64 = 0;
          CHECK_ERRNO(epoll_ctl(epollfd_, EPOLL_CTL_ADD, listensock_, &evt));

          event_buf_.emplace_back();
      }
    }

    ~Impl() {
        close(listensock_);
        close(epollfd_);
    }

    int resolve(sockaddr_in6* addr) {
        if (resolve_map_.find(*addr) != resolve_map_.end()) {
            return resolve_map_[*addr];
        }
        int result = resolve_map_.size();
        resolve_map_[*addr] = result;
        return result;
    }

    void accept_conns() {
        for (size_t i = 0; i < 2; ++i) {
            if (throttler_ && !throttler_->accept_connection()) return;
            sockaddr_in6 addr;
            socklen_t addrlen = sizeof(addr);
            SocketHolder sock(accept4(listensock_, reinterpret_cast<struct sockaddr*>(&addr), &addrlen, SOCK_NONBLOCK | SOCK_CLOEXEC));
            if (sock.get() >= 0 && addr.sin6_family == AF_INET6 && addrlen == sizeof(addr)) {
                int flags = 1;
                CHECK_ERRNO(setsockopt(sock.get(), IPPROTO_TCP, TCP_NODELAY, &flags, sizeof(flags)) == 0);
                uint64_t id = add_socket(sock.get(), EPOLLIN | EPOLLOUT | EPOLLET);
                pool_.add(sock.release(), id, resolve(&addr));
            } if (errno == EAGAIN) {
                return;
            } else  if (errno == EMFILE || errno == ENFILE || errno == ENOBUFS || errno == ENOMEM) {
                pool_.close_old_conns();
            } else if (errno != EINTR) {
                throw_errno();
            }
        }
    }

    void loop() {
        while (true) {
            int ready = epoll_wait(epollfd_, event_buf_.data(), event_buf_.size(), -1);
            CHECK_ERRNO(ready >= 0 || errno == EINTR);
            for (size_t i = 0; i < ready; ++i) {
                uint64_t id = event_buf_[i].data.u64;
                if (id == kListenId) {
                    accept_conns();
                } else {
                    if (event_buf_[i].events | EPOLLIN) {
                    }
                    if (event_buf_[i].events | EPOLLOUT) {
                    }
                }
            }
        }
    }

    uint64_t add_socket(int fd, uint32_t flags) {
        epoll_event evt;
        evt.events = flags;
        evt.data.u64 = id_++;
        CHECK_ERRNO(epoll_ctl(epollfd_, EPOLL_CTL_ADD, listensock_, &evt));
        return evt.data.u64;
    }

public:
    Throttler* throttler_ = nullptr;
    std::function<void(int, GenericBuffer&)> handler_;

    int epollfd_;
    int listensock_;

    enum {
        kListenId = 0,
        kStartId = 1,
    };

    uint64_t id_ = kStartId;

    std::vector<epoll_event> event_buf_;
    ConnectPool& pool_;

    std::unordered_map<sockaddr_in6, int, SockaddrHash, SockaddrCompare> resolve_map_;

    std::vector<sockaddr_in6> endpoints_;
};

TcpBus::TcpBus(int port, ConnectPool& pool)
    : impl_(new Impl(port, pool))
{
}

void TcpBus::set_throttler(Throttler& t) {
    impl_->throttler_ = &t;
}

void TcpBus::set_handler(std::function<void(int, GenericBuffer&)> handler) {
    impl_->handler_ = handler;
}

int TcpBus::register_endpoint(std::string addr, int port) {
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
    int result = impl_->resolve_map_.size();
    for (addrinfo* i = info; i != nullptr; i = i->ai_next) {
        if (info->ai_family == AF_INET6) {
            impl_->resolve_map_[*reinterpret_cast<sockaddr_in6*>(info->ai_addr)] = result;
        }
    }
    freeaddrinfo(info);
    return result;
}

void TcpBus::send(int dest, size_t method, GenericBuffer&) {
    //TODO
}

void TcpBus::loop() {
    impl_->loop();
}

};
