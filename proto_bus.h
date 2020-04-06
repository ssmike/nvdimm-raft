#pragma once

#include "bus.h"
#include "service.pb.h"
#include "error.h"
#include "util.h"


#include <vector>
#include <functional>

namespace bus {

class ProtoBus {
public:
    template<typename ResponseProto>
    class ResponseHandle {
    public:
        using ReturnType = ResponseProto;

    public:
        ResponseHandle(bus::detail::Message header, int rcp, TcpBus* bus, BufferPool* pool)
            : header_(std::move(header))
            , recipient_(rcp)
            , bus_(bus)
            , pool_(pool)
        {
        }

        ResponseHandle(const ResponseHandle&) = delete;
        ResponseHandle(ResponseHandle&&) = default;

        void answer(ResponseProto proto) {
            header_.set_data(proto.SerializeAsString());
            auto buffer = ScopedBuffer(*pool_);
            buffer.get().resize(header_.ByteSizeLong());
            header_.SerializeToArray(buffer.get().data(), buffer.get().size());
            bus_->send(recipient_, std::move(buffer));
        }

    private:
        bus::detail::Message header_;
        int recipient_ = std::numeric_limits<int>::max();
        TcpBus* bus_ = nullptr;
        BufferPool* pool_ = nullptr;
    };

public:
    ProtoBus(TcpBus& bus, BufferPool& pool)
        : pool_(pool)
        , bus_(bus)
    {
        bus_.set_handler([=](auto d, auto v) { this->handle(d, v); });
    }

    template<typename RequestProto, typename ResponseProto>
    internal::Future<ResponseProto> send(RequestProto proto, int dest, uint64_t method) {
        detail::Message header;
        header.set_data(proto.SerializeAsString());
        auto buffer = ScopedBuffer(pool_);
        buffer.get().resize(header.ByteSizeLong());
        header.SerializeToArray(buffer.get().data(), buffer.get().size());
        bus_.send(dest, std::move(buffer));
    }

protected:
    template<typename RequestProto, typename ResponseProto>
    void register_handler(uint32_t method, std::function<void(RequestProto, ResponseHandle<ResponseProto>)> f) {
        handlers_.resize(std::max<uint32_t>(handlers_.size(), method + 1));
        handlers_.push_back(
            [f=std::move(f), this] (int dest, bus::detail::Message header) {
                RequestProto proto;
                proto.ParseFromString(header.data());

                header.clear_data();
                f(proto, ResponseHandle<ResponseProto>(std::move(header), dest, &bus_, &pool_));
            });
    }

private:
    void handle(int dest, SharedView view) {
        bus::detail::Message header;
        header.ParseFromArray(view.data(), view.size());
        if (handlers_.size() <= header.method() || !handlers_[header.method()]) {
            throw BusError("invalid handler number");
        } else {
            handlers_[header.method()](dest, std::move(header));
        }
    }

private:
    BufferPool& pool_;
    TcpBus& bus_;
    std::vector<std::function<void(int, bus::detail::Message)>> handlers_;
};

}
