#include "bus.h"
#include "util.h"
#include "lock.h"

#include "error.h"

#include <sys/uio.h>
#include <string.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/tcp.h>
#include <unistd.h>

#include <functional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <queue>

namespace bus {

class TcpBus::Impl {
public:
    Impl(bus::TcpBus::Options opts, BufferPool& buffer_pool, EndpointManager& endpoint_manager)
        : fixed_pool_size_(opts.fixed_pool_size)
        , port_(opts.port)
        , listener_backlog_(opts.listener_backlog)
        , buffer_pool_(buffer_pool)
        , endpoint_manager_(endpoint_manager)
        , max_message_size_(opts.max_message_size)
    {
        epollfd_ = epoll_create1(EPOLL_CLOEXEC);
        CHECK_ERRNO(epollfd_ >= 0);
    }

    void start(std::function<void(int, SharedView)> handler) {
        handler_ = std::move(handler);

        listensock_ = socket(AF_INET6, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
                             IPPROTO_TCP);
        CHECK_ERRNO(listensock_ >= 0);

        {
            int optval = 1;
            setsockopt(listensock_, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval));
        }

        sockaddr_in6 addr;
        addr.sin6_family = AF_INET6;
        addr.sin6_addr = in6addr_any;
        addr.sin6_port = htons(port_);
        CHECK_ERRNO(bind(listensock_, reinterpret_cast<struct sockaddr *>(&addr),
                         sizeof(addr)) == 0);

        CHECK_ERRNO(listen(listensock_, listener_backlog_) == 0);

        {
            epoll_event evt;
            evt.events = EPOLLIN;
            evt.data.u64 = listend_id_ = pool_.make_id();
            CHECK_ERRNO(epoll_ctl(epollfd_, EPOLL_CTL_ADD, listensock_, &evt) == 0);
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
                pool_.select(id)->ingress_buf = ScopedBuffer(buffer_pool_);
            } else if (conn.errno_ == EAGAIN) {
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

    void handle_read(ConnData* data) {
        ssize_t message_size = std::numeric_limits<ssize_t>::lowest();
        while (true) {
            size_t expected = 0;
            bool has_header = false;
            if (data->ingress_offset < internal::header_len) {
                expected = internal::header_len;
            } else {
                expected = internal::read_header(data->ingress_buf.get().data()) + internal::header_len;
                has_header = true;
            }
            if (expected > max_message_size_) {
                throw BusError("too big message");
            }
            data->ingress_buf.get().resize(expected);
            expected -= data->ingress_offset;
            ssize_t res = read(data->socket.get(), data->ingress_buf.get().data() + data->ingress_offset, expected);
            if (res >= 0) {
                data->ingress_offset += res;
                if (has_header && res == expected) {
                    handler_(data->dest, SharedView(std::move(data->ingress_buf)).skip(internal::header_len));
                    data->ingress_buf = ScopedBuffer(buffer_pool_);
                    data->ingress_offset = 0;
                    continue;
                }
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            } else if (errno == EINTR) {
                continue;
            } else {
                pool_.close(data->id);
                data = nullptr;
                break;
            }
        }
    }

    void handle_write(ConnData* data) {
        do {
            if (!data->egress_message) {
                auto messages = pending_messages_.get();
                auto& queue = (*messages)[data->dest];
                if (queue.empty()) {
                    return;
                }
                data->egress_message = queue.front();
                data->egress_offset = 0;
                (*messages)[data->dest].pop();
            }
        } while (try_write_message(data));
    }

    void loop() {
        std::vector<epoll_event> event_buf;
        while (true) {
            event_buf.resize(pool_.count_connections() + 1);
            int ready = epoll_wait(epollfd_, event_buf.data(), event_buf.size(), -1);
            if (errno == EINTR) {
                continue;
            }
            CHECK_ERRNO(ready >= 0);
            for (size_t i = 0; i < ready; ++i) {
                uint64_t id = event_buf[i].data.u64;
                if (id == listend_id_) {
                    accept_conns();
                } else if (ConnData* data = pool_.select(id)) {
                    int dest = data->dest;
                    if (event_buf[i].events & EPOLLERR) {
                        pool_.close(id);
                        fix_pool_size(dest);
                        continue;
                    }
                    if (event_buf[i].events & EPOLLIN) {
                        handle_read(data);
                    }
                    if (data && (event_buf[i].events & EPOLLOUT) != 0) {
                        handle_write(data);
                    }
                }
            }
        }
    }

    bool try_write_message(ConnData* data) {
        if (!data->egress_message) {
            return true;
        }

        int fd = data->socket.get();

        char header[internal::header_len];
        internal::write_header(data->egress_message->size(), header);

        while (true) {
            iovec iov_holder[2];
            iov_holder[0] = {.iov_base = header, .iov_len = internal::header_len};
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
                if (data->egress_offset == internal::header_len + data->egress_message->size()) {
                    data->egress_message.reset();
                    pool_.set_available(data->id);
                }
                return true;
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return false;
            } else if (errno == EINTR) {
                continue;
            } else {
                (*pending_messages_.get())[data->dest].push(std::move(*data->egress_message));
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
        } else {
            (*pending_messages_.get())[dest].push(std::move(message));
        }
    }

    uint64_t epoll_add(int fd) {
        epoll_event evt;
        evt.events = EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLET;
        evt.data.u64 = pool_.make_id();
        CHECK_ERRNO(epoll_ctl(epollfd_, EPOLL_CTL_ADD, fd, &evt) == 0);
        return evt.data.u64;
    }

public:
    std::function<void(int, SharedView)> handler_;

    int epollfd_;
    int listensock_;
    size_t listend_id_;

    const int port_;
    const size_t listener_backlog_;

    ConnectPool pool_;
    const size_t fixed_pool_size_;

    internal::ExclusiveWrapper<std::unordered_map<int, std::queue<SharedView>>> pending_messages_;

    BufferPool& buffer_pool_;
    EndpointManager& endpoint_manager_;

    const size_t max_message_size_;
};

TcpBus::TcpBus(Options opts, BufferPool& buffer_pool, EndpointManager& endpoint_manager)
    : impl_(new Impl(opts, buffer_pool, endpoint_manager))
{
}

void TcpBus::send(int dest, SharedView buffer) {
    impl_->send(dest, std::move(buffer));
}

void TcpBus::start(std::function<void(int, SharedView)> handler) {
    impl_->start(std::move(handler));
}

void TcpBus::loop() {
    impl_->loop();
}

TcpBus::~TcpBus() = default;

};
