#include "bus.h"

#include "error.h"

#include <string.h>
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
#include <unordered_set>
#include <vector>
#include <queue>

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

class TcpBus::Impl {
public:
    Impl(int port, size_t fixed_pool_size, ConnectPool& pool, BufferPool& buffer_pool, EndpointManager& endpoint_manager)
        : pool_(pool)
        , fixed_pool_size_(fixed_pool_size)
        , buffer_pool_(buffer_pool)
        , endpoint_manager_(endpoint_manager)
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
          evt.data.u64 = listend_id_ = pool_.make_id();
          CHECK_ERRNO(epoll_ctl(epollfd_, EPOLL_CTL_ADD, listensock_, &evt));

          event_buf_.emplace_back();
      }
    }

    ~Impl() {
        close(listensock_);
        close(epollfd_);
    }

    void accept_conns() {
        for (size_t i = 0; i < 2; ++i) {
            if (throttler_ && !throttler_->accept_connection()) return;
            EndpointManager::IncomingConnection conn = endpoint_manager_.accept(listensock_);
            if (conn.sock_.get() >= 0) {
                uint64_t id = epoll_add(conn.sock_.get());
                pool_.add(conn.sock_.release(), id, conn.endpoint_);
                pool_.set_available(id);
            } if (conn.errno_ == EAGAIN) {
                return;
            } else  if (conn.errno_ == EMFILE || conn.errno_ == ENFILE || conn.errno_ == ENOBUFS || conn.errno_ == ENOMEM) {
                pool_.close_old_conns();
            } else if (conn.errno_ != EINTR) {
                throw_errno();
            }
        }
    }

    void fix_pool_size(int dest) {
        size_t pool_size = pool_.count_connections(dest);
        if (pool_size < fixed_pool_size_) {
            for (; pool_size < fixed_pool_size_; ++pool_size) {
                SocketHolder sock = endpoint_manager_.async_connect(dest);
                uint64_t id = epoll_add(sock.get());
                pool_.add(sock.release(), id, dest);
            }
        }
    }

    void loop() {
        while (true) {
            int ready = epoll_wait(epollfd_, event_buf_.data(), event_buf_.size(), -1);
            CHECK_ERRNO(ready >= 0 || errno == EINTR);
            for (size_t i = 0; i < ready; ++i) {
                uint64_t id = event_buf_[i].data.u64;
                if (id == listend_id_) {
                    accept_conns();
                } else if (ConnData* data = pool_.select(id)) {
                    int dest = data->dest;
                    if (event_buf_[i].events & EPOLLERR) {
                        pool_.close(id);
                        fix_pool_size(dest);
                        continue;
                    }
                    if (event_buf_[i].events & EPOLLIN) {
                    }
                    if (event_buf_[i].events & EPOLLOUT) {
                        pool_.set_available(id);
                    }
                }
            }
        }
    }

    void send(int dest, SharedView buffer) {
        fix_pool_size(dest);
        //TODO
    }

    uint64_t epoll_add(int fd) {
        epoll_event evt;
        evt.events = EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLET;
        evt.data.u64 = pool_.make_id();
        CHECK_ERRNO(epoll_ctl(epollfd_, EPOLL_CTL_ADD, listensock_, &evt));
        event_buf_.resize(pool_.count_connections() + 1);
        return evt.data.u64;
    }

public:
    Throttler* throttler_ = nullptr;
    std::function<void(int, SharedView)> handler_;

    int epollfd_;
    int listensock_;
    size_t listend_id_;

    std::vector<epoll_event> event_buf_;
    ConnectPool& pool_;
    size_t fixed_pool_size_;

    EndpointManager& endpoint_manager_;

    std::unordered_map<int, std::queue<ScopedBuffer>> pending_messages_;

    BufferPool& buffer_pool_;
};

TcpBus::TcpBus(int port, size_t fixed_pool_size, ConnectPool& pool, BufferPool& buffer_pool, EndpointManager& endpoint_manager)
    : impl_(new Impl(port, fixed_pool_size, pool, buffer_pool, endpoint_manager))
{
}

void TcpBus::set_throttler(Throttler& t) {
    impl_->throttler_ = &t;
}

void TcpBus::set_handler(std::function<void(int, SharedView)> handler) {
    impl_->handler_ = handler;
}

void TcpBus::send(int dest, SharedView buffer) {
    impl_->send(dest, std::move(buffer));
}

void TcpBus::loop() {
    impl_->loop();
}

};
