#include "bus.h"

#include "error.h"

#include <sys/uio.h>
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

constexpr size_t header_len = 8;

void write_header(size_t size, char* buf) {
    for (size_t i = 0; i < header_len; ++i) {
        buf[i] = size & 255;
        size /= 256;
    }
}

size_t read_header(char* buf) {
    size_t result = 0;
    for (size_t i = 0; i < header_len; ++i) {
        result = result * 256 + size_t(buf[i]);
    }
    return result;
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
            EndpointManager::IncomingConnection conn = endpoint_manager_.accept(listensock_);
            if (conn.sock_.get() >= 0) {
                uint64_t id = epoll_add(conn.sock_.get());
                pool_.add(conn.sock_.release(), id, conn.endpoint_);
                pool_.set_available(id);
            } if (conn.errno_ == EAGAIN) {
                return;
            } else  if (conn.errno_ == EMFILE || conn.errno_ == ENFILE || conn.errno_ == ENOBUFS || conn.errno_ == ENOMEM) {
                pool_.close_old_conns(2);
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
                        ssize_t message_size = std::numeric_limits<ssize_t>::lowest();
                        while (true) {
                            size_t expected = 0;
                            if (data->ingress_offset < header_len) {
                                expected = header_len;
                            } else {
                                expected = read_header(data->ingress_buf.get().data()) + header_len;
                            }
                            data->ingress_buf.get().resize(data->ingress_offset + expected);
                            ssize_t res = read(data->socket.get(), data->ingress_buf.get().data() + data->ingress_offset, expected);
                            if (res >= 0) {
                                data->ingress_offset += res;
                                if (header_len + message_size == data->ingress_offset) {
                                    handler_(dest, SharedView(std::move(data->ingress_buf)).skip(header_len));
                                    data->ingress_buf = ScopedBuffer(buffer_pool_);
                                    data->ingress_offset = 0;
                                    continue;
                                }
                            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                break;
                            } else if (errno == EINTR) {
                                continue;
                            } else {
                                pool_.close(id);
                                data = nullptr;
                                break;
                            }
                        }
                    }
                    if (data && (event_buf_[i].events & EPOLLOUT) != 0) {
                        while (try_write_message(data)) {
                            if (!data->egress_message) {
                                data->egress_message = pending_messages_[dest].front();
                                data->egress_offset = 0;
                                pending_messages_[dest].pop();
                            }
                        }
                    }
                }
            }
        }
    }

    bool try_write_message(ConnData* data) {
        if (!data->egress_message) {
            return true;
        }

        int fd = data->dest;

        char header[header_len];
        write_header(data->egress_message->size(), header);

        while (true) {
            iovec iov_holder[2];
            iov_holder[0] = {.iov_base = header, .iov_len = header_len};
            iov_holder[1] = {.iov_base = (void*)data->egress_message->data(), .iov_len = data->egress_message->size()};

            iovec* iov = iov_holder;
            int iovcnt = 2;
            size_t offset = data->egress_offset;

            while (iovcnt > 0 && offset > iov[0].iov_len) {
                offset -= iov[0].iov_len;
                ++iov;
            }
            if (iovcnt == 0) {
                return 0;
            }
            if (offset > 0) {
                iov[0].iov_base = ((char*)iov[0].iov_base) + offset;
                iov[0].iov_len -= offset;
            }
            ssize_t res = writev(fd, iov, iovcnt);
            if (res >= 0) {
                data->egress_offset += res;
                if (data->egress_offset == header_len + data->egress_message->size()) {
                    data->egress_message.reset();
                    pool_.set_available(data->id);
                }
                return true;
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return false;
            } else if (errno == EINTR) {
                continue;
            } else {
                pending_messages_[data->dest].push(std::move(*data->egress_message));
                pool_.close(data->id);
                return false;
            }
        }
    }

    void send(int dest, SharedView message) {
        fix_pool_size(dest);
        if (auto data = pool_.take_available(dest)) {
            data->egress_message = std::move(message);
            data->egress_offset = 0;
            try_write_message(data);
        }
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
    std::function<void(int, SharedView)> handler_;

    int epollfd_;
    int listensock_;
    size_t listend_id_;

    std::vector<epoll_event> event_buf_;
    ConnectPool& pool_;
    size_t fixed_pool_size_;

    std::unordered_map<int, std::queue<SharedView>> pending_messages_;

    BufferPool& buffer_pool_;
    EndpointManager& endpoint_manager_;
};

TcpBus::TcpBus(int port, size_t fixed_pool_size, ConnectPool& pool, BufferPool& buffer_pool, EndpointManager& endpoint_manager)
    : impl_(new Impl(port, fixed_pool_size, pool, buffer_pool, endpoint_manager))
{
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

TcpBus::~TcpBus() = default;

};
