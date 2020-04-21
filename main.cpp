#include "proto_bus.h"
#include "messages.pb.h"
#include "lock.h"
#include "executor.h"
#include "error.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>

#include <filesystem>
#include <fstream>

#include <json/reader.h>

#define FATAL(cond) if (cond) { std::cerr << strerror(errno) << std::endl; std::terminate();}

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

    static constexpr int kInvalidFd = -1;

private:
    struct State {
        size_t current_term_ = 0;
        std::optional<int> vote_for_;
        NodeRole role_ = kFollower;
        uint64_t id_;

        size_t durable_ts_ = 0;
        size_t applied_ts_ = 0;
        size_t next_ts_ = 0;

        std::unordered_map<uint64_t, uint64_t> next_timestamps_;
        std::unordered_map<uint64_t, uint64_t> commited_timestamps_;

        std::vector<LogRecord> buffered_log_;
        bus::Promise<bool> flush_event_;

        std::unordered_map<std::string, std::string> fsm_;

        size_t current_changelog_ = 0;
        size_t latest_snapshot = 0;

        Response create_response(bool success) {
            Response response;
            response.set_term(current_term_);
            response.set_durable_ts(durable_ts_);
            response.set_success(success);
            response.set_next_ts(next_ts_);
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
        duration rotate_interval;
        duration flush_interval;
        std::unordered_map<int, int> endpoint_to_id;
        std::filesystem::path dir;
    };

    RaftNode(bus::ProtoBus::Options opts, bus::EndpointManager& manager, Options options)
        : bus::ProtoBus(opts, manager)
        , buffer_pool_(opts.tcp_opts.max_message_size)
        , options_(options)
        , endpoint_to_id_(options.endpoint_to_id)
        , rotator_([this] { rotate(); }, options.rotate_interval)
        , flusher_([this] { flush(); }, options.flush_interval)
    {
        {
            auto state = state_.get();
            assert(opts.greeter.has_value());
            state->id_ = *opts.greeter;
            for (auto [endpoint, id] : endpoint_to_id_) {
                state->next_timestamps_[id] = 0;
                id_to_endpoint_[id] = endpoint;
            }
        }
        recover();
        rotator_.start();
        flusher_.start();
        register_handler<VoteRpc, Response>(kVote, [&] (int, VoteRpc rpc) { return bus::make_future(vote(rpc)); });
        register_handler<AppendRpcs, Response>(kAppendRpcs, [&](int, AppendRpcs msg) { return handle_append_rpcs(std::move(msg)); });
        timed_send_heartbeat();
    }

    bus::internal::Event& shot_down() {
        return shot_down_;
    }

private:
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
                for (auto& rpc : msg.records()) {
                    if (rpc.ts() == state->next_ts_) {
                        state->buffered_log_.push_back(rpc);
                        ++state->next_ts_;
                    }
                }
                flush_event = state->flush_event_.future();
            } else {
                assert(false);
            }
        }
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
                                state->commited_timestamps_[id] = response.durable_ts();
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

    static constexpr std::string_view changelog_fname_prefix = "changelog.";
    static constexpr std::string_view snapshot_fname_prefix = "snapshot.";

    std::string changelog_name(size_t number) {
        std::stringstream ss;
        ss << changelog_fname_prefix << number;
        auto path = options_.dir;
        path += ss.str();
        return path.string();
    }

    std::string snapshot_name(size_t number) {
        std::stringstream ss;
        ss << snapshot_fname_prefix << number; auto path = options_.dir; path += ss.str();
        return path.string();
    }

    static std::optional<size_t> parse_name(std::string_view prefix, std::string fname) {
        if (fname.substr(0, prefix.size()) == prefix) {
            auto suffix = fname = fname.substr(prefix.size());
            for (char c : suffix) {
                if (!isdigit(c)) {
                    return std::nullopt;
                }
            }
            return std::stoi(suffix);
        } else {
            return std::nullopt;
        }
    }

    static std::optional<size_t> parse_changelog_name(std::string fname) {
        return parse_name(changelog_fname_prefix, std::move(fname));
    }

    static std::optional<size_t> parse_snapshot_name(std::string fname) {
        return parse_name(snapshot_fname_prefix, std::move(fname));
    }

    std::optional<LogRecord> read_log_record(int fd) {
        uint64_t header;
        if (read(fd, &header, sizeof(header)) != sizeof(header)) { return std::nullopt; }
        LogRecord record;
        bus::SharedView v(buffer_pool_, header);
        if (read(fd, v.data(), v.size()) != v.size()) { return std::nullopt; }
        if (!record.ParseFromArray(v.data(), v.size())) { return std::nullopt; }
        return record;
    }

    bool write_log_record(int fd, const LogRecord& record, char* buf, size_t bufsz) {
        if (record.ByteSizeLong() > bufsz) { return false; }
        bufsz = record.ByteSizeLong();
        if (!record.SerializeToArray(buf, bufsz)) { return false; }
        return (write(fd, buf, bufsz) == bufsz);
    }

    void write_log_record(int fd, const LogRecord& record) {
        uint64_t sz = record.ByteSizeLong();
        bus::SharedView v(buffer_pool_, sz);
        FATAL(!write_log_record(fd, record, v.data(), v.size()));
    }

    void flush() {
        std::vector<LogRecord> to_flush;
        bus::Promise<bool> to_deliver;
        auto log = log_fd_.get();
        {
            auto state = state_.get();
            to_flush.swap(state->buffered_log_);
        }

        for (auto& record : to_flush) {
            write_log_record(*log, record);
        }
        FATAL(fdatasync(*log) != 0);

        to_deliver.set_value(true);
    }

    void recover() {
        auto state = state_.get();
        std::vector<size_t> snapshots;
        std::vector<size_t> changelogs;
        for (auto entry : std::filesystem::directory_iterator(options_.dir)) {
            if (auto number = parse_changelog_name(entry.path())) {
                changelogs.push_back(*number);
            }
            if (auto number = parse_snapshot_name(entry.path())) {
                snapshots.push_back(*number);
            }
        }
        while (!snapshots.empty()) {
            auto fname = snapshot_name(snapshots.back());
            int fd = open(fname.c_str(), O_RDONLY);
            FATAL(fd < 0);
            bool valid = true;
            uint64_t signature;
            std::unordered_map<std::string, std::string> fsm;
            valid = valid || read(fd, &signature, sizeof(signature)) == sizeof(signature);
            if (valid) {
                for (uint64_t i = 0; i < signature; ++i) {
                    if (auto record = read_log_record(fd)) {
                        for (auto& op : record->operations()) {
                            fsm[op.key()] = op.value();
                        }
                    } else {
                        valid = false;
                        break;
                    }
                }
            }
            if (valid) {
                fsm.swap(state->fsm_);
            }
        }
        for (auto changelog : changelogs) {
        }
    }

    void rotate() {
        std::string latest_snapshot;
        std::optional<std::string> to_delete;
        size_t phase;
        // sync calls under lock cos don't want to deal with partial states
        {
            auto state = state_.get();
            auto log_fd = log_fd_.get();
            if (*log_fd != kInvalidFd) {
                close(*log_fd);
                to_delete = changelog_name(state->current_changelog_);
            }
            *log_fd = open(changelog_name(++state->current_changelog_).c_str(), O_CREAT | O_WRONLY | O_APPEND);
            phase = ++state->latest_snapshot;
            FATAL(*log_fd < 0);
        }
        if (to_delete) {
            FATAL(unlink(to_delete->c_str()) != 0);
        }
        // here we go dumpin'
        int fd = open(snapshot_name(phase).c_str(), O_CREAT | O_WRONLY);
        FATAL(fd < 0);
        State& unsafe_state_ptr = *state_.get();
        if (pid_t child = fork()) {
            int wstatus;
            pid_t exited = waitpid(child, &wstatus, 0);
            FATAL(child != exited);
            FATAL(WEXITSTATUS(wstatus) != 0);
            FATAL(close(fd) != 0)
        } else {
            FATAL(child < 0);
            State& state = unsafe_state_ptr;
            uint64_t signature = state.fsm_.size();
            write(fd, &signature, sizeof(signature));
            for (auto [k, v] : state.fsm_) {
                LogRecord record;
                auto* op = record.add_operations();
                op->set_key(k);
                op->set_value(v);
                write_log_record(fd, record);
            }
            FATAL(fsync(fd) != 0);
            _exit(0);
        }
    }

private:
    bus::BufferPool buffer_pool_;
    Options options_;
    bus::internal::ExclusiveWrapper<State> state_;

    bus::internal::DelayedExecutor exc_;
    bus::internal::PeriodicExecutor flusher_;
    bus::internal::PeriodicExecutor rotator_;

    bus::internal::ExclusiveWrapper<int> log_fd_{kInvalidFd};

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
