#pragma once

#include "bus.h"
#include "error.h"
#include "future.h"
#include "executor.h"

#include <chrono>
#include <vector>
#include <functional>

namespace bus {

class ProtoBus {
public:
    ProtoBus(TcpBus::Options opts, EndpointManager& manager, BufferPool& pool)
        : pool_(pool)
        , bus_(opts, pool, manager)
        , loop_([&] { bus_.loop(); }, std::chrono::seconds::zero())
    {
        bus_.start([=](auto d, auto v) { this->handle(d, v); });
        loop_.start();
    }

    template<typename RequestProto, typename ResponseProto>
    Future<ErrorT<ResponseProto>> send(RequestProto proto, int dest, uint64_t method, std::chrono::duration<double> timeout) {
        return send_raw(proto.SerializeAsString(), dest, method, timeout).map(
            [=](ErrorT<std::string>& resp) -> ErrorT<ResponseProto> {
                if (resp) {
                    return ErrorT<ResponseProto>::error(resp.what());
                } else {
                    ResponseProto proto;
                    proto.ParseFromString(resp.unwrap());
                    return ErrorT<ResponseProto>::value(std::move(proto));
                }
            });
    }

protected:
    template<typename RequestProto, typename ResponseProto>
    void register_handler(uint32_t method, std::function<Future<ResponseProto>(RequestProto)> handler) {
        register_raw_handler(method, [handler=std::move(handler)] (std::string str) {
                RequestProto proto;
                proto.ParseFromString(str);
                return handler(std::move(proto)).map([=] (ResponseProto& proto) { return proto.SerializeAsString(); });
            });
    }

private:
    Future<ErrorT<std::string>> send_raw(std::string serialized, int dest, uint64_t method, std::chrono::duration<double> timeout);

    void register_raw_handler(uint32_t method, std::function<Future<std::string>(std::string)> handler);

    void handle(int dest, SharedView view);

private:
    BufferPool& pool_;
    TcpBus bus_;
    std::vector<std::function<void(int, uint32_t, std::string)>> handlers_;
    internal::DelayedExecutor exc_;

    internal::ExclusiveWrapper<std::unordered_map<uint64_t, Promise<ErrorT<std::string>>>> sent_requests_;
    std::atomic<uint64_t> seq_id_;

    internal::PeriodicExecutor loop_;
};

}
