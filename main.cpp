#include "proto_bus.h"
#include "messages.pb.h"
#include "lock.h"
#include "executor.h"

#include <fstream>

#include <json/reader.h>

using duration = std::chrono::system_clock::duration;

class RaftNode : bus::ProtoBus {
private:
    enum NodeRole {
        kFollower = 0,
        kLeader = 1,
        kCandidate = 2
    };

    enum {
        kVote = 1,
        kAppendRpcs = 2
    };

private:
    struct State {
        // we do all work on bus's thread so no synchronization here
        size_t current_term_ = 0;
        std::optional<int> vote_for_;
        NodeRole role_ = kFollower;
        uint64_t id_;

        size_t buffered_ts_ = 0;
        size_t commited_ts_ = 0;
        size_t applied_ts_ = 0;

        std::unordered_map<uint64_t, uint64_t> next_timestamps_;
        std::unordered_map<uint64_t, uint64_t> commited_timestamps_;

        std::vector<LogRecord> buffered_log_;
        bus::Future<bool> flush_event_ = bus::make_future(true);

        std::unordered_map<std::string, std::string> automata_;

        Response create_response(bool success) {
            Response response;
            response.set_term(current_term_);
            response.set_commited_ts(commited_ts_);
            response.set_success(success);
            return response;
        }

        void advance_to(uint64_t ts) {
            //TODO

            applied_ts_ = ts;
        }
    };

public:
    struct Options {
        duration heartbeat_timeout;
        duration heartbeat_interval;
        duration election_timeout;
        std::unordered_map<int, int> endpoint_to_id;
    };

public:
    RaftNode(bus::ProtoBus::Options opts, bus::EndpointManager& manager, Options options)
        : bus::ProtoBus(opts, manager)
        , options_(options)
        , endpoint_to_id_(options.endpoint_to_id)
    {
        auto state = state_.get();
        assert(opts.greeter.has_value());
        state->id_ = *opts.greeter;
        for (auto [endpoint, id] : endpoint_to_id_) {
            state->next_timestamps_[id] = 0;
            id_to_endpoint_[id] = endpoint;
        }
        register_handler<VoteRpc, Response>(kVote, [&] (int, VoteRpc rpc) { return bus::make_future(vote(rpc)); });
        register_handler<AppendRpcs, Response>(kAppendRpcs, [&](int, AppendRpcs msg) { return handle_append_rpcs(std::move(msg)); });
        timed_send_heartbeat();
    }

    Response vote(VoteRpc rpc) {
        auto state = state_.get();
        if (state->current_term_ > rpc.term()) {
            return state->create_response(false);
        } else if (state->current_term_ < rpc.term()) {
            state->role_ = kFollower;
            state->current_term_ = rpc.term();
            state->vote_for_ = rpc.vote_for();
            return state->create_response(true);
        } else {
            if (state->vote_for_ && rpc.vote_for() != *state->vote_for_) {
                return state->create_response(false);
            } else {
                state->vote_for_ = rpc.vote_for();
                return state->create_response(true);
            }
        }
    }

    void schedule_elections() {
    }

    void initiate_elections() {
    }

    bus::Future<Response> handle_append_rpcs(AppendRpcs msg) {
        bus::Future<bool> flush_event;
        {
            auto state = state_.get();
            if (state->role_ == kLeader) {
                if (msg.term() > state->current_term_) {
                    state->current_term_ = msg.term();
                    state->role_ = kFollower;
                } else {
                    assert(msg.term() != state->current_term_);
                    return bus::make_future(state->create_response(false));
                }
            }
            if (state->role_ == kCandidate) {
                return bus::make_future(state->create_response(false));
            }
            if (state->role_ == kFollower) {
            }
        }
        //I'm follower
        return flush_event.map([this](bool) { return state_.get()->create_response(true); });
    }

    void timed_send_heartbeat() {
        exc_.schedule([this] { timed_send_heartbeat(); }, options_.heartbeat_interval);
        heartbeat_to_followers();
    }

    void heartbeat_to_followers() {
        std::vector<uint64_t> endpoints;
        std::vector<AppendRpcs> messages;
        {
            auto state = state_.get();
            if (state->role_ != kLeader) {
                return;
            }

            for (auto [id, next_ts] : state->next_timestamps_) {
                endpoints.push_back(id_to_endpoint_[id_to_endpoint_[id]]);
                AppendRpcs rpcs;
                rpcs.set_term(state->current_term_);
                rpcs.set_master_id(state->id_);
                if (state->buffered_log_.size() > 0) {
                    const size_t start_ts = state->buffered_log_[0].ts();
                    const size_t start_index = next_ts - start_ts;
                    for (size_t i = start_index; i < state->buffered_log_.size(); ++i) {
                        *rpcs.add_records() = state->buffered_log_[i];
                    }
                }
                messages.push_back(std::move(rpcs));
            }
        }
        for (size_t i = 0; i < endpoints.size(); ++i) {
            send<AppendRpcs, Response>(std::move(messages[i]), endpoints[i], kAppendRpcs, options_.heartbeat_timeout)
                .subscribe([this, id=endpoint_to_id_[endpoints[i]]] (bus::ErrorT<Response>& result) {
                        if (result) {
                            auto& response = result.unwrap();
                            auto state = state_.get();
                            if (response.success()) {
                                state->next_timestamps_[id] = response.next_ts();
                                state->commited_timestamps_[id] = response.commited_ts();
                                uint64_t min_ts = std::numeric_limits<uint64_t>::max();
                                for (auto [id, ts] : state->commited_timestamps_) {
                                    min_ts = std::min(min_ts, ts);
                                }
                                state->advance_to(min_ts);
                            } else {
                                if (response.term() > state->current_term_) {
                                    state->role_ = kFollower;
                                    state->current_term_ = response.term();
                                }
                            }
                        }
                    } );
        }
    }

    bus::internal::Event& shot_down() {
        return shot_down_;
    }

private:
    Options options_;
    bus::internal::ExclusiveWrapper<State> state_;

    bus::internal::DelayedExecutor exc_;

    std::unordered_map<int, int> endpoint_to_id_;
    std::unordered_map<int, int> id_to_endpoint_;
    bus::internal::Event shot_down_;
};

duration parse_duration(const Json::Value& val) {
    assert(!val.isNull());
    return std::chrono::duration_cast<duration>(std::chrono::duration<double>(val.asFloat()));
}

int main(int argc, char** argv) {
    assert(argc == 2);
    Json::Value conf;
    std::ifstream(argv[1]) >> conf;
    bus::ProtoBus::Options opts;
    opts.batch_opts.max_batch = conf["max_batch"].asInt();
    opts.batch_opts.max_delay = parse_duration(conf["max_delay"]);
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

    RaftNode::Options options;
    options.heartbeat_timeout = parse_duration(conf["heartbeat_timeout"]);
    options.heartbeat_interval = parse_duration(conf["heartbeat_interval"]);
    options.election_timeout = parse_duration(conf["election_timeout"]);

    RaftNode node(opts, manager, options);
    node.shot_down().wait();
}
