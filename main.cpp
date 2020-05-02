#include "proto_bus.h"
#include "messages.pb.h"
#include "lock.h"
#include "executor.h"
#include "error.h"
#include "client.pb.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>

#include <thread>
#include <filesystem>
#include <fstream>
#include <map>

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
        kAppendRpcs = 2,
        kClientReq = 3,
    };

    static constexpr int kInvalidFd = -1;

    class DescriptorHolder {
    public:
        DescriptorHolder() = default;
        DescriptorHolder(int fd) : fd_(fd) {
            FATAL(fd < 0);
        }

        DescriptorHolder(const DescriptorHolder&) = delete;

        int operator* () {
            return fd_;
        }

        ~DescriptorHolder() {
            if (fd_ != kInvalidFd) {
                close(fd_);
            }
        }

    private:
        int fd_ = kInvalidFd;
    };

private:
    struct State {
        size_t current_term_ = 0;
        NodeRole role_ = kFollower;

        ssize_t durable_ts_ = 0;
        ssize_t applied_ts_ = 0;
        ssize_t next_ts_ = 0;

        std::set<int> voted_for_me_;

        std::unordered_map<uint64_t, uint64_t> next_timestamps_;
        std::unordered_map<uint64_t, uint64_t> commited_timestamps_;

        std::unordered_map<uint64_t, bus::Promise<ClientResponse>> client_requests_;

        std::vector<LogRecord> buffered_log_;
        bus::Promise<bool> flush_event_;

        std::unordered_map<std::string, std::string> fsm_;

        size_t current_changelog_ = 0;
        size_t latest_snapshot = 0;

        std::chrono::system_clock::time_point latest_heartbeat_;
        std::optional<uint64_t> leader_id_;

        Response create_response(bool success) {
            Response response;
            response.set_term(current_term_);
            response.set_durable_ts(durable_ts_);
            response.set_success(success);
            response.set_next_ts(next_ts_);
            return response;
        }

        void apply(const LogRecord& rec) {
            for (auto op : rec.operations()) {
                fsm_[op.key()] = op.value();
            }
        }

        void advance_to(uint64_t ts) {
            if (!buffered_log_.empty()) {
                ssize_t pos = applied_ts_ - buffered_log_[0].ts() + 1;
                if (pos >= 0) {
                    for (; pos < buffered_log_.size() && ts < buffered_log_[pos].ts(); ++pos) {
                        apply(buffered_log_[pos]);
                        applied_ts_ = buffered_log_[pos].ts();
                    }
                }
            }
        }
    };

public:
    struct Options {
        duration heartbeat_timeout;
        duration heartbeat_interval;
        duration election_timeout;
        duration rotate_interval;
        duration flush_interval;
        std::filesystem::path dir;

        size_t members;
    };

    RaftNode(bus::ProtoBus::Options opts, bus::EndpointManager& manager, Options options)
        : bus::ProtoBus(opts, manager)
        , buffer_pool_(opts.tcp_opts.max_message_size)
        , options_(options)
        , elector_([this] { initiate_elections(); }, options.election_timeout)
        , rotator_([this] { rotate(); }, options.rotate_interval)
        , flusher_([this] { flush(); }, options.flush_interval)
        , sender_([this] { heartbeat_to_followers(); }, options.heartbeat_interval)
    {
        {
            auto state = state_.get();
            assert(opts.greeter.has_value());
            id_ = *opts.greeter;
            for (size_t id = 0; id < options_.members; ++id) {
                if (id != id_) {
                    state->next_timestamps_[id] = 0;
                }
            }
        }
        recover();
        rotator_.delayed_start();
        flusher_.start();
        using namespace std::placeholders;
        register_handler<VoteRpc, Response>(kVote, [&] (int, VoteRpc rpc) { return bus::make_future(vote(rpc)); });
        register_handler<AppendRpcs, Response>(kAppendRpcs, std::bind(&RaftNode::handle_append_rpcs, this, _1, _2));
        register_handler<ClientRequest, ClientResponse>(kClientReq, std::bind(&RaftNode::HandleClientRequest, this, _1, _2));
        sender_.delayed_start();
        elector_.delayed_start();
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
            state->leader_id_ = rpc.vote_for();
            return state->create_response(true);
        } else {
            if (state->applied_ts_ >= rpc.ts() || (state->leader_id_ && rpc.vote_for() != *state->leader_id_)) {
                return state->create_response(false);
            } else {
                state->leader_id_ = rpc.vote_for();
                return state->create_response(true);
            }
        }
    }

    bus::Future<ClientResponse> HandleClientRequest(int id, ClientRequest req) {
        {
            auto state = state_.get();
            if (state->role_ == kFollower) {
                ClientResponse response;
                response.set_success(false);
                assert(state->leader_id_);
                response.set_retry_to(*state->leader_id_);
                return bus::make_future(std::move(response));
            }
            if (state->role_ == kCandidate) {
                ClientResponse response;
                response.set_success(false);
                return bus::make_future(std::move(response));
            }
            if (state->role_ == kLeader) {
                LogRecord rec;
                ClientResponse response;
                for (auto op : req.operations()) {
                    if (op.type() == ClientRequest::Operation::READ) {
                        auto entry = response.add_entries();
                        entry->set_key(op.key());
                        entry->set_value(state->fsm_[op.key()]);
                    }
                    if (op.type() == ClientRequest::Operation::WRITE) {
                        auto applied = rec.add_operations();
                        applied->set_key(op.key());
                        applied->set_value(op.value());
                    }
                }
            }
        }
    }

    void initiate_elections() {
        size_t term;
        {
            auto state = state_.get();
            if (state->role_ != kCandidate) {
                term = ++state->current_term_;
                state->voted_for_me_.clear();
                state->role_ = kCandidate;
                if (state->latest_heartbeat_ + options_.election_timeout > std::chrono::system_clock::now()) {
                    return;
                }
            }
        }
        std::this_thread::sleep_for(options_.election_timeout * (double(rand()) / double(RAND_MAX)));
        std::vector<bus::Future<bus::ErrorT<Response>>> responses;
        std::vector<size_t> ids;
        {
            auto state = state_.get();
            if (term == state->current_term_) {
                VoteRpc rpc;
                for (size_t id = 0; id < options_.members; ++id) {
                    if (id != id_) {
                        responses.push_back(send<VoteRpc, Response>(rpc, id, kVote, options_.heartbeat_timeout));
                        ids.push_back(id);

                    }
                }
            }
        }
        for (size_t i = 0; i < responses.size(); ++i) {
            responses[i]
                .subscribe([&, id=ids[i]] (bus::ErrorT<Response>& r) {
                        if (r && r.unwrap().success()) {
                            auto state = state_.get();
                            state->voted_for_me_.insert(id);
                            if (state->voted_for_me_.size() > options_.members / 2) {
                                state->role_ = kLeader;
                            }
                        }
                    });
        }
    }

    bus::Future<Response> handle_append_rpcs(int id, AppendRpcs msg) {
        bus::Future<bool> flush_event;
        {
            auto state = state_.get();
            state->latest_heartbeat_ = std::chrono::system_clock::now();
            if (state->role_ == kLeader) {
                if (msg.term() > state->current_term_) {
                    state->current_term_ = msg.term();
                    state->role_ = kFollower;
                    state->leader_id_ = id;
                } else {
                    assert(msg.term() != state->current_term_);
                    return bus::make_future(state->create_response(false));
                }
            }
            if (state->role_ == kCandidate) {
                return bus::make_future(state->create_response(false));
            }
            if (state->role_ == kFollower) {
                state->leader_id_ = id;
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

    void heartbeat_to_followers() {
        std::vector<uint64_t> endpoints;
        std::vector<AppendRpcs> messages;
        {
            auto state = state_.get();
            if (state->role_ != kLeader) {
                return;
            }

            for (auto [id, next_ts] : state->next_timestamps_) {
                endpoints.push_back(id);
                AppendRpcs rpcs;
                rpcs.set_term(state->current_term_);
                rpcs.set_master_id(id_);
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
                .subscribe([this, id=endpoints[i]] (bus::ErrorT<Response>& result) {
                        if (result) {
                            auto& response = result.unwrap();
                            auto state = state_.get();
                            if (response.success()) {
                                state->next_timestamps_[id] = response.next_ts();
                                state->commited_timestamps_[id] = response.durable_ts();
                                std::vector<uint64_t> tss;
                                for (auto [id, ts] : state->commited_timestamps_) {
                                    if (id != id_) {
                                        tss.push_back(ts);
                                    }
                                }
                                std::sort(tss.begin(), tss.end());
                                state->advance_to(tss[tss.size() / 2 - 1]);
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

    void write_uint64(int fd, uint64_t val) {
        FATAL(write(fd, &val, sizeof(val)) != sizeof(val));
    }

    std::optional<uint64_t> read_uint64(int fd) {
        uint64_t val;
        if (read(fd, &val, sizeof(val)) != sizeof(val)) {
            return val;
        } else {
            return std::nullopt;
        }
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
            DescriptorHolder fd(open(fname.c_str(), O_RDONLY));
            bool valid = true;
            std::optional<uint64_t> size = read_uint64(*fd);
            std::optional<uint64_t> applied = read_uint64(*fd);
            valid = size.has_value() && applied.has_value();
            std::unordered_map<std::string, std::string> fsm;
            if (valid) {
                for (uint64_t i = 0; i < *size; ++i) {
                    if (auto record = read_log_record(*fd)) {
                        state->apply(*record);
                    } else {
                        valid = false;
                        break;
                    }
                }
            }
            if (valid) {
                fsm.swap(state->fsm_);
                state->durable_ts_ = state->applied_ts_ = *applied;
                state->next_ts_ = *applied + 1;
            }
        }
        size_t first_changelog = snapshots.empty() ? 0 : snapshots.back();
        for (auto changelog : changelogs) {
            if (changelog > first_changelog) {
                auto fname = snapshot_name(changelog);
                DescriptorHolder fd(open(fname.c_str(), O_RDONLY));
                while (auto rec = read_log_record(*fd)) {
                    if (rec->ts() == state->next_ts_) {
                        state->apply(*rec);
                        state->next_ts_ = rec->ts() + 1;
                        state->applied_ts_ = rec->ts();
                        state->durable_ts_ = rec->ts();
                    }
                }
            }
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
            write_uint64(fd, state.fsm_.size());
    std::unordered_map<int, int> endpoint_to_id_;
    std::unordered_map<int, int> id_to_endpoint_;
            write_uint64(fd, state.applied_ts_);
            uint64_t applied_ts = state.applied_ts_;
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

    bus::internal::PeriodicExecutor elector_;
    bus::internal::PeriodicExecutor flusher_;
    bus::internal::PeriodicExecutor rotator_;
    bus::internal::PeriodicExecutor sender_;

    bus::internal::ExclusiveWrapper<int> log_fd_{kInvalidFd};

    uint64_t id_;

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
    for (size_t i = 0; i < members.size(); ++i) {
        auto member = members[Json::ArrayIndex(i)];
        manager.merge_to_endpoint(member["host"].asString(), member["port"].asInt(), i);
    }

    RaftNode::Options options;
    options.heartbeat_timeout = parse_duration(conf["heartbeat_timeout"]);
    options.heartbeat_interval = parse_duration(conf["heartbeat_interval"]);
    options.election_timeout = parse_duration(conf["election_timeout"]);
    options.members = members.size();

    RaftNode node(opts, manager, options);
    node.shot_down().wait();
}
