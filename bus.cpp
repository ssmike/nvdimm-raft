#include "bus.h"

#include "string.h"
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/tcp.h>
#include <unistd.h>

#include <sstream>
#include <functional>
#include <string>
#include <map>
#include <vector>

namespace bus {

void throw_errno() {
    throw BusError(strerror(errno));
}

#define CHECK_ERRNO(x) if (!(x)) throw_errno();

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
        for (auto endpoint : endpoints_) {
            if (endpoint) {
                freeaddrinfo(endpoint);
            }
        }
        close(listensock_);
    }

    void accept_conns() {
        for (size_t i = 0; i < 2; ++i) {
            if (throttler_ && !throttler_->accept_connection()) return;
            SocketHolder sock(accept4(listensock_, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC));
            if (sock.get() >= 0) {
                int flags = 1;
                CHECK_ERRNO(setsockopt(sock.get(), IPPROTO_TCP, TCP_NODELAY, &flags, sizeof(flags)) == 0);
                uint64_t id = add_socket(sock.get(), EPOLLIN | EPOLLOUT | EPOLLET);
                pool_.add_ingress(sock.release(), id);
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
    std::vector<std::function<void(GenericBuffer&)>> handlers_;
    std::vector<addrinfo*> endpoints_;

    int epollfd_;
    int listensock_;

    enum {
        kListenId = 0,
        kStartId = 1,
    };

    uint64_t id_ = kStartId;

    std::vector<epoll_event> event_buf_;
    ConnectPool& pool_;
};

TcpBus::TcpBus(int port, ConnectPool& pool)
    : impl_(new Impl(port, pool))
{
}

void TcpBus::set_throttler(Throttler& t) {
    impl_->throttler_ = &t;
}

void TcpBus::set_handler(size_t method, std::function<void(GenericBuffer&)> handler) {
    impl_->handlers_.resize(std::max<size_t>(impl_->handlers_.size(), method));
    impl_->handlers_[method] = handler;
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
    impl_->endpoints_.push_back(info);
    return impl_->endpoints_.size();
}

void TcpBus::send(int dest, size_t method, GenericBuffer&) {
    //TODO
}

void TcpBus::loop() {
    impl_->loop();
}

};
