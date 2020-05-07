#include "proto_bus.h"
#include "messages.pb.h"
#include "lock.h"
#include "executor.h"
#include "error.h"
#include "client.pb.h"

#include <google/protobuf/arena.h>

#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/stat.h>

#include <thread>
#include <filesystem>
#include <fstream>
#include <map>

#include <spdlog/spdlog.h>

#include <json/reader.h>

#define FATAL(cond) if (cond) { std::cerr << strerror(errno) << std::endl; std::terminate();}

using duration = std::chrono::system_clock::duration;

class DescriptorHolder {
public:
    static constexpr int kInvalidFd = -1;

    DescriptorHolder() = default;
    DescriptorHolder(int fd) : fd_(fd) {
        FATAL(fd < 0);
    }

    DescriptorHolder(const DescriptorHolder&) = delete;

    void set(int fd) {
        if (fd_ != kInvalidFd && fd != fd_) {
            close(fd_);
        }
        FATAL(fd < 0);
        fd_ = fd;
    }

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

class BufferedFile {
private:
    static constexpr size_t bufsz_ = 128 << 10;

public:
    BufferedFile() = default;
    BufferedFile(int fd) {
        set_fd(fd);
    }

    void set_fd(int fd) {
        fd_.set(fd);
        data_ptr_ = consumed_ptr_ = 0;
    }

    size_t reserve(size_t sz) {
        assert(sz <= bufsz_);
        if (data_ptr_ + sz > bufsz_) {
            flush();
        }
        auto result = data_ptr_;
        data_ptr_ += sz;
        return result;
    }

    std::optional<size_t> fetch(size_t sz) {
        if (consumed_ptr_ + sz > data_ptr_) {
            memmove(buffer_, buffer_ + consumed_ptr_, data_ptr_ - consumed_ptr_);
            data_ptr_ -= consumed_ptr_;
            consumed_ptr_ = 0;
            auto read_bytes = read(*fd_, buffer_ + data_ptr_, bufsz_ - data_ptr_);
            FATAL(read_bytes < 0);
            data_ptr_ += read_bytes;
        }
        if (consumed_ptr_ + sz > data_ptr_) {
            return std::nullopt;
        } else {
            auto result = consumed_ptr_;
            consumed_ptr_ += sz;
            return result;
        }
    }

    void flush() {
        FATAL(write(*fd_, buffer_, data_ptr_) != data_ptr_);
        consumed_ptr_ = data_ptr_ = 0;
    }

    void write_uint64(uint64_t val) {
        auto ptr = reserve(sizeof(val));
        memcpy(&buffer_[ptr], &val, sizeof(val));
    }

    std::optional<uint64_t> read_uint64() {
        uint64_t val;
        if (auto ptr = fetch(sizeof(val))) {
            memcpy(&val, &buffer_[*ptr], sizeof(val));
            return val;
        } else {
            return std::nullopt;
        }
    }

    std::optional<LogRecord> read_log_record() {
        auto header = read_uint64();
        if (!header) { return std::nullopt; }
        if (auto ptr = fetch(*header)) {
            LogRecord record;
            if (!record.ParseFromArray(&buffer_[*ptr], *header)) { return std::nullopt; }
            return record;
        } else {
            return std::nullopt;
        }
    }

    void write_log_record(const LogRecord& record) {
        auto sz = record.ByteSizeLong();
        write_uint64(sz);
        auto ptr = reserve(sz);
        FATAL(!record.SerializeToArray(&buffer_[ptr], sz));
    }

    void sync() {
        flush();
        FATAL(fdatasync(*fd_) != 0);
    }

private:
    DescriptorHolder fd_;
    char buffer_[bufsz_];
    size_t data_ptr_ = 0;
    size_t consumed_ptr_ = 0;
};

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

    class VoteKeeper {
    public:
        VoteKeeper(std::string fname)
            : fname_(fname)
        {
        }

        void store(VoteRpc vote) {
            auto tmp = fname_ + ".tmp";
            {
                DescriptorHolder fd{open(tmp.c_str(), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR)};
                size_t sz = vote.ByteSizeLong();;
                FATAL(write(*fd, &sz, sizeof(sz)) != sizeof(sz));
                std::vector<char> data(sz);
                vote.SerializeToArray(data.data(), data.size());
                FATAL(write(*fd, data.data(), data.size()) != data.size());
                FATAL(fdatasync(*fd) != 0);
            }
            FATAL(rename(tmp.c_str(), fname_.c_str()) != 0);
        }

        std::optional<VoteRpc> recover() {
            struct stat buf;
            if (stat(fname_.c_str(), &buf) != 0) return std::nullopt;
            DescriptorHolder fd{open(fname_.c_str(), O_RDONLY)};
            uint64_t sz;
            FATAL(read(*fd, &sz, sizeof(sz)) != sizeof(sz));
            std::vector<char> data(sz);
            FATAL(read(*fd, data.data(), data.size()) != data.size());
            VoteRpc result;
            FATAL(!result.ParseFromArray(data.data(), data.size()));
            return result;
        }

    private:
        std::string fname_;
    };

private:
    struct State {
        uint64_t id_;

        size_t current_term_ = 0;
        NodeRole role_ = kCandidate;

        ssize_t durable_ts_ = -1;
        ssize_t applied_ts_ = -1;
        ssize_t next_ts_ = 0;

        std::set<int> voted_for_me_;

        std::vector<int64_t> next_timestamps_;
        std::vector<int64_t> durable_timestamps_;

        std::unordered_map<int64_t, bus::Promise<bool>> commit_subscribers_;

        size_t flushed_index_ = 0;
        std::vector<LogRecord> buffered_log_;
        bus::Promise<bool> flush_event_;

        std::unordered_map<std::string, std::string> fsm_;

        size_t current_changelog_ = 0;

        std::vector<std::chrono::system_clock::time_point> follower_heartbeats_;
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

        void advance_to(int64_t ts) {
            if (!buffered_log_.empty()) {
                auto old_ts = applied_ts_;
                ssize_t pos = applied_ts_ - ssize_t(buffered_log_[0].ts()) + 1;
                if (pos >= 0) {
                    for (; pos < buffered_log_.size() && ts >= buffered_log_[pos].ts(); ++pos) {
                        apply(buffered_log_[pos]);
                        applied_ts_ = buffered_log_[pos].ts();
                    }
                }
                if (old_ts < applied_ts_) {
                    spdlog::debug("advance from {0:d} to {1:d}", old_ts, applied_ts_);
                }
            }
        }

        void advance_applied_timestmap() {
            durable_timestamps_[id_] = durable_ts_;
            std::vector<int64_t> tss;
            for (auto ts : durable_timestamps_) {
                tss.push_back(ts);
            }
            std::sort(tss.begin(), tss.end());
            auto ts = tss[tss.size() / 2];
            advance_to(ts);
        }

    };

public:
    struct Options {
        bus::ProtoBus::Options bus_options;

        duration heartbeat_timeout;
        duration heartbeat_interval;
        duration election_timeout;
        duration rotate_interval;
        duration flush_interval;
        std::filesystem::path dir;

        size_t members;
        ssize_t applied_backlog;
    };

    RaftNode(bus::EndpointManager& manager, Options options)
        : bus::ProtoBus(options.bus_options, manager)
        , vote_keeper_(options.dir / "vote")
        , buffer_pool_(options.bus_options.tcp_opts.max_message_size)
        , options_(options)
        , elector_([this] { initiate_elections(); }, options.election_timeout)
        , rotator_([this] { rotate(); }, options.rotate_interval)
        , flusher_([this] { flush(); }, options.flush_interval)
        , sender_([this] { heartbeat_to_followers(); }, options.heartbeat_interval)
        , stale_nodes_agent_( [this] { recover_stale_nodes(); }, options.heartbeat_interval)
    {
        {
            auto state = state_.get();
            assert(options.bus_options.greeter.has_value());
            id_ = *options.bus_options.greeter;
            state->next_timestamps_.assign(options_.members, 0);
            state->durable_timestamps_.assign(options_.members, -1);
            state->follower_heartbeats_.assign(options_.members, std::chrono::system_clock::time_point::min());
        }
        recover();
        rotator_.delayed_start();
        flusher_.start();
        using namespace std::placeholders;
        register_handler<VoteRpc, Response>(kVote, [&] (int, VoteRpc rpc) { return bus::make_future(vote(rpc)); });
        register_handler<AppendRpcs, Response>(kAppendRpcs, std::bind(&RaftNode::handle_append_rpcs, this, _1, _2));
        register_handler<ClientRequest, ClientResponse>(kClientReq, std::bind(&RaftNode::handle_client_request, this, _1, _2));
        sender_.delayed_start();
        elector_.delayed_start();
    }

    bus::internal::Event& shot_down() {
        return shot_down_;
    }

private:
    Response vote(VoteRpc rpc) {
        spdlog::info("received vote request from {0:d} with ts={1:d} term={2:d}", rpc.vote_for(), rpc.ts(), rpc.term());
        auto state = state_.get();
        if (state->current_term_ > rpc.term()) {
            return state->create_response(false);
        } else if (state->current_term_ < rpc.term()) {
            state->role_ = kCandidate;
            state->current_term_ = rpc.term();
            state->voted_for_me_.clear();
            elector_.trigger();
        }

        if (state->durable_ts_ > rpc.ts() || (state->leader_id_ && rpc.vote_for() != *state->leader_id_)) {
            spdlog::info("denied vote for {0:d} their ts={1:d} my ts={2:d} my vote {3:d}", rpc.vote_for(), rpc.ts(), state->durable_ts_, *state->leader_id_);
            return state->create_response(false);
        } else {
            vote_keeper_.get()->store(rpc);
            state->leader_id_ = rpc.vote_for();
            spdlog::info("granted vote for {0:d}", rpc.vote_for());
            return state->create_response(true);
        }
    }

    bus::Future<ClientResponse> handle_client_request(int id, ClientRequest req) {
        bus::Future<bool> commit_future;
        {
            auto state = state_.get();
            if (state->role_ == kFollower) {
                ClientResponse response;
                response.set_success(false);
                assert(state->leader_id_);
                response.set_retry_to(*state->leader_id_);
                response.set_should_retry(true);
                spdlog::debug("handling client request redirect to {0:d}", *state->leader_id_);
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
                bool has_writes = false;
                bool has_reads = false;
                response.set_success(true);
                for (auto op : req.operations()) {
                    if (op.type() == ClientRequest::Operation::READ) {
                        auto entry = response.add_entries();
                        entry->set_key(op.key());
                        entry->set_value(state->fsm_[op.key()]);
                        has_reads = true;
                    }
                    if (op.type() == ClientRequest::Operation::WRITE) {
                        auto applied = rec.add_operations();
                        applied->set_key(op.key());
                        applied->set_value(op.value());
                        has_writes = true;
                    }
                }
                if (has_reads) {
                    response.set_success(!has_writes);
                    return bus::make_future(std::move(response));
                }
                rec.set_ts(state->next_ts_++);
                spdlog::debug("handling client request ts={0:d}", rec.ts());
                auto promise = bus::Promise<bool>();
                state->commit_subscribers_.insert({ rec.ts(), promise });
                state->buffered_log_.push_back(std::move(rec));
                return promise.future().map([response=std::move(response)](bool) { return response; });
            }
        }
        FATAL(true);
    }

    void initiate_elections() {
        size_t term;
        {
            auto state = state_.get();
            auto now = std::chrono::system_clock::now();
            auto latest_heartbeat = state->latest_heartbeat_;
            if (state->role_ == kLeader) {
                std::vector<std::chrono::system_clock::time_point> times;
                for (size_t id = 0; id < options_.members; ++id) {
                    if (id != id_) {
                        times.push_back(state->follower_heartbeats_[id]);
                    }
                }
                std::sort(times.begin(), times.end());
                latest_heartbeat = times[options_.members / 2];
            }
            if (latest_heartbeat + options_.election_timeout > now) {
                return;
            }
            spdlog::info("starting elections");
            term = ++state->current_term_;
            state->voted_for_me_.clear();
            state->role_ = kCandidate;
            state->leader_id_ = std::nullopt;
            state->latest_heartbeat_ = now;
        }
        std::this_thread::sleep_for((options_.election_timeout * id_) / options_.members);
        std::vector<bus::Future<bus::ErrorT<Response>>> responses;
        std::vector<size_t> ids;
        {
            auto state = state_.get();
            if (term == state->current_term_) {
                if (state->leader_id_ && *state->leader_id_ != id_) {
                    return;
                } else {
                    state->leader_id_ = id_;
                    VoteRpc self_vote;
                    self_vote.set_ts(state->durable_ts_);
                    self_vote.set_term(state->current_term_);
                    self_vote.set_vote_for(id_);
                    vote_keeper_.get()->store(self_vote);
                    state->voted_for_me_.insert(id_);
                }
                VoteRpc rpc;
                rpc.set_term(state->current_term_);
                rpc.set_ts(state->durable_ts_);
                rpc.set_vote_for(id_);
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
                .subscribe([&, id=ids[i], term] (bus::ErrorT<Response>& r) {
                        if (r && r.unwrap().success()) {
                            auto& response = r.unwrap();
                            auto state = state_.get();
                            state->next_timestamps_[id] = response.next_ts();
                            state->durable_timestamps_[id] = response.durable_ts();
                            state->follower_heartbeats_[id] = std::chrono::system_clock::now();
                            if (state->current_term_ == term) {
                                spdlog::info("granted vote from {0:d} with durable_ts={1:d}", id, response.durable_ts());
                                state->voted_for_me_.insert(id);
                                if (state->voted_for_me_.size() > options_.members / 2) {
                                    state->role_ = kLeader;
                                    state->advance_applied_timestmap();
                                    spdlog::info("becoming leader applied up to {0:d}", state->applied_ts_);
                                    state->durable_timestamps_.assign(options_.members, state->applied_ts_);
                                    state->next_timestamps_.assign(options_.members, state->applied_ts_ + 1);
                                }
                            }
                        }
                    });
        }
    }

    bus::Future<Response> handle_append_rpcs(int id, AppendRpcs msg) {
        bus::Future<bool> flush_event;
        {
            auto state = state_.get();
            if (msg.term() < state->current_term_) {
                return bus::make_future(state->create_response(false));
            }
            if (msg.term() > state->current_term_) {
                spdlog::info("stale term becoming follower");
                state->current_term_ = msg.term();
            }
            assert(state->role_ != kLeader);
            state->role_ = kFollower;
            state->latest_heartbeat_ = std::chrono::system_clock::now();
            state->leader_id_ = id;
            for (auto& rpc : msg.records()) {
                if (state->next_ts_ > rpc.ts()) {
                    if (state->buffered_log_.size() > 0) {
                        state->buffered_log_.resize(std::max<ssize_t>(0, rpc.ts() - state->buffered_log_[0].ts() + 1));
                        state->flushed_index_ = std::min(state->flushed_index_, state->buffered_log_.size());
                    }
                    state->next_ts_ = rpc.ts();
                    state->durable_ts_ = std::min<ssize_t>(state->durable_ts_, rpc.ts() - 1);
                    assert(state->applied_ts_ < rpc.ts());
                }
                if (rpc.ts() == state->next_ts_) {
                    state->buffered_log_.push_back(rpc);
                    ++state->next_ts_;
                }
            }
            if (msg.records_size()) {
                spdlog::debug("handling heartbeat next_ts={0:d}", state->next_ts_);
            }
            state->advance_to(std::min(msg.applied_ts(), state->durable_ts_));
            flush_event = state->flush_event_.future();
        }
        return flush_event.map([this](bool) { return state_.get()->create_response(true); });
    }

    void recover_stale_nodes() {
        std::vector<size_t> nodes;
        std::vector<int64_t> nexts;
        if (auto state = state_.get(); state->role_ == kLeader) {
            for (size_t id = 0; id < options_.members; ++id) {
                if (id_ != id) {
                    if (state->next_timestamps_[id] < state->buffered_log_[0].ts()) {
                        nodes.push_back(id);
                        nexts.push_back(id);
                    }
                }
            }
        }
        for (size_t i = 0; i < nodes.size(); ++i) {
            auto node = nodes[i];
            auto next = nexts[i];
        }
    }

    void heartbeat_to_followers() {
        std::vector<uint64_t> endpoints;
        std::vector<AppendRpcs> messages;
        {
            auto state = state_.get();
            if (state->role_ != kLeader) {
                return;
            }

            for (size_t id = 0; id < options_.members; ++id) {
                ssize_t next_ts = state->next_timestamps_[id];
                if (id == id_) {
                    continue;
                }
                endpoints.push_back(id);
                AppendRpcs rpcs;
                rpcs.set_term(state->current_term_);
                if (state->buffered_log_.size() > 0) {
                    const size_t start_ts = state->buffered_log_[0].ts();
                    const size_t start_index = next_ts - start_ts;
                    for (size_t i = start_index; i < state->buffered_log_.size(); ++i) {
                        *rpcs.add_records() = state->buffered_log_[i];
                    }
                }
                if (rpcs.records_size()) {
                    spdlog::debug("sending to {0:d} {1:d} records", id, rpcs.records_size());
                }
                messages.push_back(std::move(rpcs));
            }
        }
        for (size_t i = 0; i < endpoints.size(); ++i) {
            bool to_log = messages[i].records_size() > 0;
            send<AppendRpcs, Response>(std::move(messages[i]), endpoints[i], kAppendRpcs, options_.heartbeat_timeout)
                .subscribe([=, id=endpoints[i]] (bus::ErrorT<Response>& result) {
                        std::vector<bus::Promise<bool>> subscribers;
                        if (result) {
                            auto& response = result.unwrap();
                            auto state = state_.get();
                            if (response.success()) {
                                state->next_timestamps_[id] = response.next_ts();
                                state->durable_timestamps_[id] = response.durable_ts();
                                state->follower_heartbeats_[id] = std::chrono::system_clock::now();
                                if (to_log) {
                                    spdlog::debug("node {2:d} responded with next_ts={0:d} durable_ts={1:d}", response.next_ts(), response.durable_ts(), id);
                                }
                                state->advance_applied_timestmap();
                                while (!state->commit_subscribers_.empty() && state->commit_subscribers_.begin()->first <= state->applied_ts_) {
                                    spdlog::debug("fire commit subscriber for ts={0:d}", state->commit_subscribers_.begin()->first);
                                    subscribers.push_back(state->commit_subscribers_.begin()->second);
                                    state->commit_subscribers_.erase(state->commit_subscribers_.begin());
                                }
                            } else {
                                spdlog::debug("node {0:d} failed heartbeat", id);
                            }
                        }
                        for (auto& f : subscribers) {
                            f.set_value(true);
                        }
                    } );
        }
    }

    static constexpr std::string_view changelog_fname_prefix = "changelog.";
    static constexpr std::string_view snapshot_fname_prefix = "snapshot.";

    std::string changelog_name(size_t number) {
        std::stringstream ss;
        ss << changelog_fname_prefix << number;
        std::filesystem::path path = options_.dir;
        path /= ss.str();
        return path.string();
    }

    std::string snapshot_name(size_t number) {
        std::stringstream ss;
        ss << snapshot_fname_prefix << number;
        std::filesystem::path path = options_.dir;
        path /= ss.str();
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

    void flush() {
        std::vector<LogRecord> to_flush;
        bus::Promise<bool> to_deliver;
        // we want log records to be consecutive
        auto log = log_.get();
        size_t durable_ts;

        {
            auto state = state_.get();
            auto& log = state->buffered_log_;
            size_t i = 0;
            while (i < log.size() && log[i].ts() + options_.applied_backlog < state->applied_ts_) {
                spdlog::debug("deleting old record with ts={0:d} applied_ts={1:d}", log[i].ts(), state->applied_ts_);
                ++i;
            }
            to_flush.insert(to_flush.begin(), log.begin() + state->flushed_index_, log.end());
            log.erase(log.begin(), log.begin() + i);
            if (i > 0) {
                spdlog::debug("erased up to {0:d}'th record", i);
            }
            state->flushed_index_ = log.size();
            to_deliver.swap(state->flush_event_);
            durable_ts = !state->buffered_log_.empty() ? state->buffered_log_.back().ts() : state->durable_ts_;
        }

        for (auto& record : to_flush) {
            spdlog::debug("write ts={0:d} to changelog", record.ts());
            log->write_log_record(record);
        }
        log->sync();

        state_.get()->durable_ts_ = durable_ts;

        to_deliver.set_value_once(true);
    }

    void recover() {
        auto state = state_.get();
        std::vector<size_t> snapshots;
        std::vector<size_t> changelogs;
        for (auto entry : std::filesystem::directory_iterator(options_.dir)) {
            if (auto number = parse_changelog_name(entry.path().filename())) {
                changelogs.push_back(*number);
                state->current_changelog_ = std::max<size_t>(state->current_changelog_, *number + 1);
            }
            if (auto number = parse_snapshot_name(entry.path().filename())) {
                snapshots.push_back(*number);
                state->current_changelog_ = std::max<size_t>(state->current_changelog_, *number + 1);
            }
        }
        std::sort(snapshots.begin(), snapshots.end());
        std::sort(changelogs.begin(), changelogs.end());
        BufferedFile io;
        while (!snapshots.empty()) {
            auto fname = snapshot_name(snapshots.back());
            io.set_fd(open(fname.c_str(), O_RDONLY));
            bool valid = true;
            std::optional<uint64_t> size = io.read_uint64();
            std::optional<uint64_t> applied = io.read_uint64();
            valid = size.has_value() && applied.has_value();
            std::unordered_map<std::string, std::string> fsm;
            if (valid) {
                for (uint64_t i = 0; i < *size; ++i) {
                    if (auto record = io.read_log_record()) {
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
                break;
            } else {
                snapshots.pop_back();
            }
        }

        size_t first_changelog = 0;
        for (size_t i = 0; i < changelogs.size(); ++i) {
            auto fname = changelog_name(changelogs[i]);
            io.set_fd(open(fname.c_str(), O_RDONLY));
            auto durable_ts = io.read_uint64();
            if (durable_ts && *durable_ts < state->applied_ts_) {
                first_changelog = i;
            }
        }
        for (size_t i = first_changelog; i < changelogs.size(); ++i) {
            auto changelog = changelogs[i];
            auto fname = changelog_name(changelog);
            io.set_fd(open(fname.c_str(), O_RDONLY));
            if (!io.read_uint64()) continue;
            while (auto rec = io.read_log_record()) {
                if (rec->ts() > state->applied_ts_) {
                    state->buffered_log_.resize(std::max<size_t>(state->buffered_log_.size(), rec->ts() - state->applied_ts_));
                    state->buffered_log_[rec->ts() - state->applied_ts_ - 1] = *rec;
                    state->next_ts_ = std::max<size_t>(state->next_ts_, rec->ts() + 1);
                    state->durable_ts_ = std::max<ssize_t>(state->durable_ts_, rec->ts());
                }
            }
        }
        {
            auto log = log_.get();
            log->set_fd(open(changelog_name(state->current_changelog_).c_str(), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR));
            io.write_uint64(state->durable_ts_);
        }
        if (auto vote = vote_keeper_.get()->recover()) {
            state->current_term_ = vote->term();
            state->leader_id_ = vote->vote_for();
        }
        spdlog::info("recovered term={0:d} durable_ts={1:d} applied_ts={2:d}", state->current_term_, state->durable_ts_, state->applied_ts_);
    }

    void rotate() {
        size_t changelog_number;
        // sync calls under lock cos don't want to deal with partial states
        {
            auto log = log_.get();
            auto state = state_.get();
            changelog_number = ++state->current_changelog_;
            log->set_fd(open(changelog_name(changelog_number).c_str(), O_CREAT | O_WRONLY | O_APPEND, S_IRUSR | S_IWUSR));
            log->write_uint64(state->durable_ts_);
        }
        // here we go dumpin'
        BufferedFile snapshot{open(snapshot_name(changelog_number).c_str(), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR)};
        State& unsafe_state_ptr = *state_.get();
        bus::SharedView preallocated_arena_buf(buffer_pool_, options_.bus_options.tcp_opts.max_message_size);
        if (pid_t child = fork()) {
            FATAL(child < 0);
            int wstatus;
            pid_t exited = waitpid(child, &wstatus, 0);
            FATAL(child != exited);
            FATAL(WEXITSTATUS(wstatus) != 0);
        } else {
            State& state = unsafe_state_ptr;
            snapshot.write_uint64(state.fsm_.size());
            snapshot.write_uint64(state.applied_ts_);
            uint64_t applied_ts = state.applied_ts_;
            for (auto [k, v] : state.fsm_) {
                google::protobuf::ArenaOptions options;
                options.initial_block = preallocated_arena_buf.data();
                options.initial_block_size = preallocated_arena_buf.size();
                google::protobuf::Arena arena(options);
                LogRecord* record = google::protobuf::Arena::CreateMessage<LogRecord>(&arena);
                auto* op = record->add_operations();
                op->set_key(k);
                op->set_value(v);
                snapshot.write_log_record(*record);
            }
            snapshot.sync();
            _exit(0);
        }
    }

private:
    bus::internal::ExclusiveWrapper<VoteKeeper> vote_keeper_;
    bus::BufferPool buffer_pool_;
    Options options_;
    bus::internal::ExclusiveWrapper<State> state_;

    bus::internal::PeriodicExecutor elector_;
    bus::internal::PeriodicExecutor flusher_;
    bus::internal::PeriodicExecutor rotator_;
    bus::internal::PeriodicExecutor sender_;
    bus::internal::PeriodicExecutor stale_nodes_agent_;

    bus::internal::ExclusiveWrapper<BufferedFile> log_;

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
    RaftNode::Options options;
    options.bus_options.batch_opts.max_batch = conf["max_batch"].asInt();
    options.bus_options.batch_opts.max_delay = parse_duration(conf["max_delay"]);
    size_t id = conf["id"].asInt();
    options.bus_options.greeter = id;
    options.bus_options.tcp_opts.port = conf["port"].asInt();
    options.bus_options.tcp_opts.fixed_pool_size = conf["pool_size"].asUInt64();
    options.bus_options.tcp_opts.max_message_size = conf["max_message"].asUInt64();
    options.dir = conf["log"].asString();

    bus::EndpointManager manager;
    auto members = conf["members"];
    for (size_t i = 0; i < members.size(); ++i) {
        auto member = members[Json::ArrayIndex(i)];
        manager.merge_to_endpoint(member["host"].asString(), member["port"].asInt(), i);
    }

    options.heartbeat_timeout = parse_duration(conf["heartbeat_timeout"]);
    options.heartbeat_interval = parse_duration(conf["heartbeat_interval"]);
    options.election_timeout = parse_duration(conf["election_timeout"]);
    options.applied_backlog = conf["applied_backlog"].asUInt64();
    options.rotate_interval = parse_duration(conf["rotate_interval"]);
    options.flush_interval = parse_duration(conf["flush_interval"]);
    options.members = members.size();

    spdlog::set_pattern("[%H:%M:%S.%e] [" + std::to_string(id) + "] [%^%l%$] %v");

#ifndef NDEBUG
    spdlog::set_level(spdlog::level::debug);
#endif

    spdlog::info("starting node");

    RaftNode node(manager, options);
    node.shot_down().wait();
}
