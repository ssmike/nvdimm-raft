#include "executor.h"
#include "error.h"
#include "client.pb.h"

#include "proto_bus.h"

#include <json/reader.h>
#include <fstream>

using duration = std::chrono::system_clock::duration;

duration parse_duration(const Json::Value& val) {
    assert(!val.isNull());
    return std::chrono::duration_cast<duration>(std::chrono::duration<double>(val.asFloat()));
}

class Client : bus::ProtoBus {
private:
    enum {
        kClientReq = 3,
    };
public:
    Client(bus::ProtoBus::Options opts, bus::EndpointManager& manager, size_t members, duration timeout)
        : ProtoBus(opts, manager)
        , timeout_(timeout)
    {
    }

    bus::Future<ClientResponse> execute(ClientRequest req) {
        return bound_execute(req, leader_.load())
            .chain([&] (ClientResponse& resp) {
                    if (!resp.success()) {
                        leader_.store(resp.retry_to());
                        return bound_execute(req, resp.retry_to());
                    } else {
                        return bus::make_future(std::move(resp));
                    }
                });
    }

private:
    bus::Future<ClientResponse> bound_execute(ClientRequest req, size_t member) {
        return send<ClientRequest, ClientResponse>(req, member, kClientReq, timeout_)
            .map([](bus::ErrorT<ClientResponse>& resp) {
                    if (resp) {
                        return resp.unwrap();
                    } else {
                        ClientResponse response;
                        response.set_success(false);
                        return response;
                    }
                });
    }

private:
    duration timeout_;
    std::atomic<size_t> leader_ = 0;
};

int main(int argc, char** argv) {
    assert(argc == 2);
    Json::Value conf;
    std::ifstream(argv[1]) >> conf;

    bus::ProtoBus::Options opts;
    opts.batch_opts.max_batch = conf["max_batch"].asInt();
    opts.batch_opts.max_delay = parse_duration(conf["max_delay"]);
    opts.greeter = std::nullopt;
    opts.tcp_opts.port = conf["port"].asInt();
    opts.tcp_opts.fixed_pool_size = conf["pool_size"].asUInt64();
    opts.tcp_opts.max_message_size = conf["max_message"].asUInt64();

    bus::EndpointManager manager;
    auto members = conf["members"];
    for (size_t i = 0; i < members.size(); ++i) {
        auto member = members[Json::ArrayIndex(i)];
        manager.merge_to_endpoint(member["host"].asString(), member["port"].asInt(), i);
    }

    Client client(opts, manager, members.size(), parse_duration(conf["timeout"]));

    ClientRequest req;
    {
        auto* op = req.add_operations();
        op->set_type(ClientRequest::Operation::WRITE);
        op->set_key("key");
        op->set_value("value");
        assert(client.execute(req).wait().success());
    }

    {
        auto* op = req.add_operations();
        op->set_type(ClientRequest::Operation::READ);
        op->set_key("key");
        ClientResponse response = client.execute(req).wait();
        assert(response.success());
        assert(response.entries_size() == 1);
        assert(response.entries(0).key() == "key");
        assert(response.entries(0).value() == "value");
    }
}
