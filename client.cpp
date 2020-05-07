#include "executor.h"
#include "error.h"
#include "client.pb.h"

#include "proto_bus.h"

//#include <spdlog/spdlog.h>

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
                    if (resp.should_retry()) {
                        leader_.store(resp.retry_to());
                        return bound_execute(req, resp.retry_to());
                    } else {
                        return bus::make_future(std::move(resp));
                    }
                });
    }

    bus::ErrorT<std::string> lookup(std::string key) {
        ClientRequest req;
        auto* op = req.add_operations();
        op->set_type(ClientRequest::Operation::READ);
        op->set_key(key);
        ClientResponse response = execute(req).wait();
        if (response.success() && response.entries_size() == 1) {
            return bus::ErrorT<std::string>::value(response.entries()[0].value());
        } else {
            return bus::ErrorT<std::string>::error("fetch failed");
        }
    }

    bool write(std::string key, std::string value) {
        ClientRequest req;
        auto* op = req.add_operations();
        op->set_type(ClientRequest::Operation::WRITE);
        op->set_key(std::move(key));
        op->set_value(std::move(value));
        return execute(req).wait().success();
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

    assert(client.write("key", "value"));
    assert(client.lookup("key").unwrap() == "value");
}
