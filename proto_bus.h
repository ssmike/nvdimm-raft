#pragma once

#include "bus.h"
#include "error.h"
#include "future.h"

#include <functional>

namespace bus {

class ProtoBus {
public:
    ProtoBus(TcpBus::Options opts, EndpointManager& manager);
    ~ProtoBus();

    template<typename RequestProto, typename ResponseProto>
    Future<ErrorT<ResponseProto>> send(RequestProto proto, int dest, uint64_t method, std::chrono::duration<double> timeout) {
        return send_raw(proto.SerializeAsString(), dest, method, timeout).map(
            [=](ErrorT<std::string>& resp) -> ErrorT<ResponseProto> {
                if (!resp) {
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

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}
