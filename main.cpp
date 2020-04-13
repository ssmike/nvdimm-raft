#include "proto_bus.h"
#include "messages.pb.h"
#include "lock.h"

#include <fstream>

#include <json/reader.h>

class RaftNode : public bus::ProtoBus {
public:
    RaftNode(bus::ProtoBus::Options opts, bus::EndpointManager& manager, std::unordered_map<int, int> endpoint_to_id)
        : bus::ProtoBus(opts, manager)
        , endpoint_to_id_(endpoint_to_id)
    {
        register_handler<VoteRpc, Response>(1, [&] (int, VoteRpc rpc) { return bus::make_future(vote(rpc)); });
        //register_handler<>(2, [&](int endp))
    }

    Response vote(VoteRpc rpc) {
    }

    bus::internal::Event& shot_down() {
        return shot_down_;
    }

private:
    bus::internal::ExclusiveWrapper<std::optional<size_t>> voted_for_;

    std::unordered_map<int, int> endpoint_to_id_;
    bus::internal::Event shot_down_;
};


int main(int argc, char** argv) {
    assert(argc == 2);
    Json::Value conf;
    std::ifstream(argv[1]) >> conf;
    bus::ProtoBus::Options opts;
    opts.batch_opts.max_batch = conf["max_batch"].asInt();
    opts.batch_opts.max_delay = std::chrono::duration_cast<std::chrono::system_clock::duration>(std::chrono::duration<double>(conf["max_delay"].asFloat()));
    opts.greeter = conf["id"].asInt();
    opts.tcp_opts.port = conf["port"].asInt();
    opts.tcp_opts.fixed_pool_size = conf["pool_size"].asUInt64();
    opts.tcp_opts.max_message_size = conf["max_message"].asUInt64();

    bus::EndpointManager manager;
    auto members = conf["members"];
    std::unordered_map<int, int> endpoint_to_id;
    for (size_t i = 0; i < members.size(); ++i) {
        auto member = members[Json::ArrayIndex(i)];
        endpoint_to_id[manager.register_endpoint(member["host"].asString(), member["port"].asInt())] = member["id"].asInt();
    }

    RaftNode node(opts, manager, endpoint_to_id);
    node.shot_down().wait();
}
