#include "connect_pool.h"

#include "error.h"

#include <netinet/tcp.h>
#include <unistd.h>

#include <unordered_map>
#include <list>


namespace bus {

SocketHolder::~SocketHolder() {
    if (sock_ < 0) {
        return;
    }
    struct linger sl;
    sl.l_onoff = 1;
    sl.l_linger = 0;
    setsockopt(sock_, SOL_SOCKET, SO_LINGER, &sl, sizeof(sl));
    close(sock_);
}

class ConnectPool::Impl {
public:
    struct PoolItem : public ConnData {
        std::list<uint64_t>::iterator usage_list_pos_;
        std::list<uint64_t>::iterator by_dest_pos_;
        bool available_ = false;
    };

public:
    PoolItem* select(uint64_t id) {
        auto it = by_id_.find(id);
        if (it == by_id_.end()) {
            return nullptr;
        } else {
            return &it->second;
        }
    }

public:
    uint64_t id_ = 0;
    uint64_t size_ = 0;

    std::unordered_map<uint64_t, PoolItem> by_id_;
    std::unordered_map<int, std::list<uint64_t>> by_dest_;
    std::list<uint64_t> by_usage_;
};

ConnectPool::ConnectPool()
    : impl_(new Impl())
{
}

size_t ConnectPool::make_id() {
    return ++impl_->id_;
}

void ConnectPool::add(SocketHolder holder, uint64_t id, int dest) {
    if (impl_->by_id_.find(id) != impl_->by_id_.end()) {
        throw BusError("duplicate id");
    }

    auto usage_iterator = impl_->by_usage_.insert(impl_->by_usage_.begin(), id);
    auto hint_iterator =
        impl_->by_dest_[dest].insert(impl_->by_dest_[dest].begin(), id);

    auto& data = impl_->by_id_[id];
    data.id = id;
    data.socket = std::move(holder);
    data.dest = dest;

    data.usage_list_pos_ = usage_iterator;
    data.by_dest_pos_ = hint_iterator;

    ++impl_->size_;
}

ConnData* ConnectPool::select(uint64_t id) {
    return impl_->select(id);
}

ConnData* ConnectPool::take_available(int dest) {
    auto it = impl_->by_dest_.find(dest);
    if (it == impl_->by_dest_.end()) {
        return nullptr;
    }

    while (true) {
        if (it->second.empty()) {
            impl_->by_dest_.erase(it);
            return nullptr;
        }

        auto* data = impl_->select(*it->second.begin());
        if (!data) {
            it->second.erase(it->second.begin());
        } else if (!data->available_) {
            return nullptr;
        } else {
            return data;
        }
    }
}

void ConnectPool::set_available(uint64_t id) {
    if (auto* data = impl_->select(id)) {
        data->available_ = true;
        auto& d_list = impl_->by_dest_[data->dest];
        auto it = d_list.insert(d_list.begin(), id);
        d_list.erase(data->by_dest_pos_);
        data->by_dest_pos_ = it;
    }
}

size_t ConnectPool::count_connections(int dest) {
    return impl_->by_dest_[dest].size();
}

size_t ConnectPool::count_connections() {
    return impl_->size_;
}

void ConnectPool::close(uint64_t id) {
    auto it = impl_->by_id_.find(id);
    if (it != impl_->by_id_.end()) {
        impl_->by_dest_[it->second.dest].erase(it->second.by_dest_pos_);
        impl_->by_usage_.erase(it->second.usage_list_pos_);
        if (impl_->by_dest_[it->second.dest].empty()) {
            impl_->by_dest_.erase(it->second.dest);
        }
        impl_->by_id_.erase(it);
    }
}

void ConnectPool::close_old_conns(size_t cnt) {
    for (size_t i = 0; i < 2 && !impl_->by_usage_.empty(); ++i) {
        auto it = --(impl_->by_usage_.end());
        uint64_t id = *it;
        close(id);

        if (!impl_->by_usage_.empty()) {
            it =  --(impl_->by_usage_.end());
            if (id == *it) {
                impl_->by_usage_.erase(it);
            }
        }
    }
}

ConnectPool::~ConnectPool() = default;

}
