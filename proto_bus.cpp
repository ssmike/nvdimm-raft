#include "proto_bus.h"
#include "executor.h"

#include "service.pb.h"

namespace bus {
    class ProtoBus::Impl {
    public:
        Impl(TcpBus::Options opts, EndpointManager& manager)
            : pool_{ opts.max_message_size }
            , bus_(opts, pool_, manager)
            , loop_([&] { bus_.loop(); }, std::chrono::seconds::zero())
        {
            bus_.start([=](auto d, auto v) { this->handle(d, v); });
            loop_.start();
        }

        void handle(int dest, SharedView view) {
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

    public:
        BufferPool pool_;
        TcpBus bus_;
        std::vector<std::function<void(int, uint32_t, std::string)>> handlers_;
        internal::DelayedExecutor exc_;

        internal::ExclusiveWrapper<std::unordered_map<uint64_t, Promise<ErrorT<std::string>>>> sent_requests_;
        std::atomic<uint64_t> seq_id_ = 0;

        internal::PeriodicExecutor loop_;
    };

    Future<ErrorT<std::string>> ProtoBus::send_raw(std::string serialized, int dest, uint64_t method, std::chrono::duration<double> timeout) {
        detail::Message header;
        uint64_t seq_id = impl_->seq_id_.fetch_add(1);
        header.set_seq_id(seq_id);
        header.set_type(detail::Message::REQUEST);
        header.set_data(std::move(serialized));
        header.set_method(method);
        auto buffer = SharedView(impl_->pool_, header.ByteSizeLong());
        header.SerializeToArray(buffer.data(), buffer.size());
        impl_->bus_.send(dest, std::move(buffer));
        Promise<ErrorT<std::string>> promise;
        impl_->sent_requests_.get()->insert({ seq_id, promise });
        impl_->exc_.schedule([=] () mutable {
                auto requests = impl_->sent_requests_.get();
                if (requests->find(seq_id) != requests->end()) {
                    promise.set_value(ErrorT<std::string>::error("timeout exceeded"));
                }
            },
            timeout);
        return promise.future();
    }

    void ProtoBus::register_raw_handler(uint32_t method, std::function<Future<std::string>(std::string)> handler) {
        impl_->handlers_.resize(std::max<uint32_t>(impl_->handlers_.size(), method + 1));
        impl_->handlers_[method] =
            [handler=std::move(handler), this] (int dest, uint32_t seq_id, std::string str) {
                handler(std::move(str)).subscribe([=](std::string& str) {
                    bus::detail::Message header;
                    header.set_type(detail::Message::RESPONSE);
                    header.set_data(str);
                    header.set_seq_id(seq_id);
                    auto buffer = SharedView(impl_->pool_, header.ByteSizeLong());
                    header.SerializeToArray((void*)buffer.data(), buffer.size());
                    impl_->bus_.send(dest, std::move(buffer));
                });
            };
    }

    ProtoBus::ProtoBus(TcpBus::Options opts, EndpointManager& manager)
        : impl_(new Impl(opts, manager))
    {
    }

    ProtoBus::~ProtoBus() = default;

}
