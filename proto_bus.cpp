#include "proto_bus.h"
#include "service.pb.h"

namespace bus {
    Future<ErrorT<std::string>> ProtoBus::send_raw(std::string serialized, int dest, uint64_t method, std::chrono::duration<double> timeout) {
        detail::Message header;
        uint64_t seq_id = seq_id_.fetch_add(1);
        header.set_seq_id(seq_id);
        header.set_type(detail::Message::REQUEST);
        header.set_data(std::move(serialized));
        header.set_method(method);
        auto buffer = ScopedBuffer(pool_);
        buffer.get().resize(header.ByteSizeLong());
        header.SerializeToArray(buffer.get().data(), buffer.get().size());
        bus_.send(dest, std::move(buffer));
        Promise<ErrorT<std::string>> promise;
        sent_requests_.get()->insert({ seq_id, promise });
        exc_.schedule([=] () mutable {
                auto requests = sent_requests_.get();
                if (requests->find(seq_id) != requests->end()) {
                    promise.set_value(ErrorT<std::string>::error("timeout exceeded"));
                }
            },
            timeout);
        return promise.future();
    }

    void ProtoBus::register_raw_handler(uint32_t method, std::function<Future<std::string>(std::string)> handler) {
        handlers_.resize(std::max<uint32_t>(handlers_.size(), method + 1));
        handlers_[method] =
            [handler=std::move(handler), this] (int dest, uint32_t seq_id, std::string str) {
                handler(std::move(str)).subscribe([=](std::string& str) {
                    bus::detail::Message header;
                    header.set_type(detail::Message::RESPONSE);
                    header.set_data(str);
                    header.set_seq_id(seq_id);
                    auto buffer = ScopedBuffer(pool_);
                    buffer.get().resize(header.ByteSizeLong());
                    header.SerializeToArray(buffer.get().data(), buffer.get().size());
                    bus_.send(dest, std::move(buffer));
                });
            };
    }

    void ProtoBus::handle(int dest, SharedView view) {
        bus::detail::Message header;
        header.ParseFromArray(view.data(), view.size());
        if (header.type() == detail::Message::REQUEST) {
            if (handlers_.size() <= header.method() || !handlers_[header.method()]) {
                throw BusError("invalid handler number");
            } else {
                handlers_[header.method()](dest, header.seq_id(), std::move(*header.mutable_data()));
            }
        }
        if (header.type() == detail::Message::RESPONSE) {
            auto reqs = sent_requests_.get();
            auto it = reqs->find(header.seq_id());
            if (it != reqs->end()) {
                it->second.set_value(ErrorT<std::string>::value(std::move(*header.mutable_data())));
                reqs->erase(it);
            }
        }
    }
}
