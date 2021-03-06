#include "proto_bus.h"
#include "messages.pb.h"
#include "lock.h"
#include "delayed_executor.h"
#include "error.h"
#include "client.pb.h"
#include "engine2.h"

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

template<typename T>
void should_be_set(bus::Future<T>& f) {
    struct guard {
        std::atomic_bool flag = true;

        ~guard() {
            if (flag.load()) {
                std::cerr << "forgotten future" << std::endl;
                std::terminate();
            }
        }
    };

    auto g = std::make_shared<guard>();
    f.subscribe([=] (auto&) {
            g->flag.store(false);
        });
}

using duration = std::chrono::system_clock::duration;

struct PersistentStrArray {
    Engine::PersistentStr& operator [](size_t i) {
        return (reinterpret_cast<Engine::PersistentStr*>(data_))[i];
    }

    size_t size() const {
        return size_ / sizeof(Engine::PersistentStr);
    }

    char* data_;
    size_t size_;
};

template<typename T>
class DelayedSetter {
public:
    DelayedSetter() = default;
    DelayedSetter(bus::Promise<T> p) {
        promise_.emplace(std::move(p));
    }

    DelayedSetter(const DelayedSetter<T>&) = delete;
    DelayedSetter(DelayedSetter<T>&& oth) {
        promise_.swap(oth.promise_);
    }

    DelayedSetter<T>& operator = (const DelayedSetter<T>&) = delete;
    DelayedSetter<T>& operator = (DelayedSetter<T>&& oth) {
        promise_.swap(oth.promise_);
        return *this;
    }

    void set() {
        if (promise_) {
            promise_->set_value_once();
        }
        promise_.reset();
    }

    ~DelayedSetter() {
        set();
    }

private:
    std::optional<bus::Promise<T>> promise_ = std::nullopt;
};

namespace {
    std::string ts_to_str(uint64_t id) {
        std::string str;
        str.resize(sizeof(id));
        for (ssize_t i = sizeof(id) - 1; i >= 0; --i) {
            str[i] = id & 255;
            id /= 256;
        }
        return str;
    }

    uint64_t str_to_ts(std::string_view v) {
        uint64_t res = 0;
        for (size_t i = 0; i < sizeof(res); ++i) {
            res *= 256;
            res += (unsigned char)v[i];
        }
        return res;
    }

    std::string rollback_key(uint64_t id) {
        return "_" + ts_to_str(id);
    }

    std::string durable_ts_key() {
        return "_durable";
    }
    std::string applied_ts_key() {
        return "_applied";
    }

    std::string base_key(std::string key, int64_t ts) {
        key += '_';
        key += ts_to_str(ts);
        return key;
    }

    std::string_view from_base_key(std::string_view key) {
        size_t i = 0;
        while (i < key.size() && key[i] != '_') {
            ++i;
        }
        return key.substr(0, i);
    }

    uint64_t ts_from_base_key(std::string_view key) {
        size_t i = 0;
        while (i < key.size() && key[i] != '_') {
            ++i;
        }
        return str_to_ts(key.substr(i + 1, sizeof(uint64_t)));
    }

    bool reserved_key(std::string_view key) {
        return (key.size() > 0 && key[0] == '_');
    }

    // allowed client keys
    bool allowed_key(std::string_view key) {
        if (key.size() == 0) {
            return false;
        }
        for (size_t i = 0; i < key.size(); ++i) {
            if (key[i] == '_') {
                return false;
            }
        }
        return true;
    }
}

class VoteKeeper {
private:
    static constexpr std::string_view key_name = "_vote";

public:
    VoteKeeper() = default;

    VoteKeeper(Engine& engine) {
        reset(engine);
    }

    void reset(Engine& engine) {
        engine_ = &engine;
        key_ = engine_->copy_str(key_name);
    }

    void store(VoteRpc vote) {
        auto str = engine_->allocate_str(vote.ByteSizeLong());
        vote.SerializeToArray(str.data(), str.size());
        engine_->insert(key_, str);
        engine_->commit();
    }

    std::optional<VoteRpc> recover() {
        if (auto vote = engine_->lookup(key_name)) {
            VoteRpc result;
            FATAL(!result.ParseFromArray(vote->data(), vote->size()));
            return result;
        } else {
            return std::nullopt;
        }
    }

private:
    Engine* engine_ = nullptr;
    Engine::PersistentStr key_;
};

//template<typename F>
//void timeit(F f, char* msg) {
//    auto pt = std::chrono::steady_clock::now();
//    f();
//    spdlog::debug("tracepoint \"{0}\" taken {1:d}us", msg, std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - pt).count());
//}
//
//template<typename F>
//auto timeit_(F f, char* msg) {
//    auto pt = std::chrono::steady_clock::now();
//    auto result =  f();
//    spdlog::debug("tracepoint \"{0}\" taken {1:d}us", msg, std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - pt).count());
//    return result;
//}

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
        kRecover = 4
    };

private:
    struct State {
        uint64_t id_;

        size_t current_term_ = 0;
        NodeRole role_ = kCandidate;

        ssize_t durable_ts_ = -1;
        ssize_t applied_ts_ = -1;
        ssize_t next_ts_ = 0;
        ssize_t read_barrier_ts_ = -1;

        std::set<int> voted_for_me_;

        std::vector<int64_t> next_timestamps_;
        std::vector<int64_t> durable_timestamps_;

        std::unordered_map<int64_t, bus::Promise<bool>> commit_subscribers_;

        ssize_t applied_backlog;
        size_t flushed_index_ = 0;
        std::vector<LogRecord> buffered_log_;
        bus::Promise<bool> flush_event_;

        VoteKeeper vote_keeper_;
        Engine engine_;

        size_t current_changelog_ = 0;

        std::vector<std::chrono::system_clock::time_point> follower_heartbeats_;
        std::chrono::system_clock::time_point latest_heartbeat_;
        std::optional<uint64_t> leader_id_;

        bool match_message(const LogRecord& rec) {
            if (buffered_log_.empty() || rec.ts() < buffered_log_[0].ts() || rec.ts() > buffered_log_.back().ts()) {
                return true;
            }
            return buffered_log_[rec.ts() - buffered_log_[0].ts()].SerializeAsString() != rec.SerializeAsString();
        }

        Response create_response(bool success) {
            Response response;
            response.set_term(current_term_);
            response.set_durable_ts(durable_ts_);
            response.set_success(success);
            response.set_next_ts(next_ts_);
            return response;
        }

        size_t write_num = 0;
        size_t flush_frequency = 0;
        DelayedSetter<bool> flush() {
            bus::Promise<bool> new_;
            if (durable_ts_ + 1 < next_ts_) {
                std::chrono::steady_clock::time_point pt = std::chrono::steady_clock::now();
                durable_ts_ = next_ts_ - 1;
                if (durable_ts_ >= 0) {
                    engine_.insert(engine_.copy_str(durable_ts_key()), engine_.copy_str(ts_to_str(durable_ts_)));
                }
                engine_.commit();
                if (role_ == kLeader) {
                    advance_applied_timestamp();
                }

                new_.future().subscribe([subs=pick_subscribers()] (auto&) mutable {
                        for (auto& sub : subs) {
                            sub.set_value_once(true);
                        }
                    });
                spdlog::debug("flush taken {0:d}us", std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - pt).count());
            }
            new_.swap(flush_event_);
            write_num = 0;
            return new_;
        }

        DelayedSetter<bool> account_write() {
            if (write_num % flush_frequency == 0) {
                return flush();
            } else {
                return {};
            }
        }

        void rollback(const LogRecord& rec) {
            spdlog::debug("rolling back record with ts={0:d}", rec.ts());
            for (auto& op : rec.operations()) {
                engine_.unsafe_erase(base_key(op.key(), rec.ts()));
            }
            //engine_.unsafe_erase(rollback_key(rec.ts()));
        }

        void write(const LogRecord& rec) {
            ++write_num;
            Engine::PersistentStr rollback_record = engine_.allocate_str(rec.operations_size() * sizeof(Engine::PersistentStr));
            assert(rec.ts() >= 0);
            uint64_t ts = rec.ts();
            memcpy(rollback_record.data(), &ts, sizeof(ts));
            size_t i = 0;
            for (auto& op : rec.operations()) {
                auto _key = engine_.copy_str(::base_key(op.key(), rec.ts()));
                engine_.insert(_key, engine_.copy_str(op.value()));
                size_t offset = sizeof(Engine::PersistentStr) * (i++);
                assert(offset + sizeof(Engine::PersistentStr) <= rollback_record.size());
                memcpy(rollback_record.data() + offset, &_key, sizeof(Engine::PersistentStr));
            }
            engine_.insert(engine_.copy_str(rollback_key(rec.ts())), rollback_record);
            spdlog::debug("write ts={0:d}", rec.ts());
        }

        void advance_to(int64_t ts) {
            if (!buffered_log_.empty()) {
                auto old_ts = applied_ts_;
                ssize_t pos = applied_ts_ - ssize_t(buffered_log_[0].ts()) + 1;
                if (pos >= 0) {
                    for (; pos < buffered_log_.size() && ts >= buffered_log_[pos].ts(); ++pos) {
                        applied_ts_ = buffered_log_[pos].ts();
                        for (auto& op : buffered_log_[pos].operations()) {
                            std::optional<std::string_view> prevkey;
                            engine_.iterate(op.key(), base_key(op.key(), buffered_log_[pos].ts()),
                                [&] (std::string_view key, auto) {
                                    if (prevkey) {
                                        engine_.unsafe_erase(*prevkey);
                                    }
                                    prevkey = key;
                                });
                        }
                        engine_.unsafe_erase(rollback_key(buffered_log_[pos].ts()));
                    }
                }
                if (old_ts < applied_ts_) {
                    spdlog::debug("advance from {0:d} to {1:d}", old_ts, applied_ts_);
                    engine_.insert(engine_.copy_str(applied_ts_key()), engine_.copy_str(ts_to_str(applied_ts_)));
                }

                // delete records
                size_t i = 0;
                while (i < buffered_log_.size() && buffered_log_[i].ts() + applied_backlog <= applied_ts_) {
                    ++i;
                }
                if (i > 0) {
                    spdlog::debug("erased up to ts={0:d} record", buffered_log_[i - 1].ts());
                }
                buffered_log_.erase(buffered_log_.begin(), buffered_log_.begin() + i);
            }
        }

        std::vector<bus::Promise<bool>> pick_subscribers() {
            if (role_ != kLeader) return {};
            std::vector<bus::Promise<bool>> subscribers;
            while (!commit_subscribers_.empty() && commit_subscribers_.begin()->first <= applied_ts_) {
                spdlog::debug("fire commit subscriber for ts={0:d}", commit_subscribers_.begin()->first);
                subscribers.push_back(commit_subscribers_.begin()->second);
                commit_subscribers_.erase(commit_subscribers_.begin());
            }
            return subscribers;
        }

        void advance_applied_timestamp() {
            durable_timestamps_[id_] = durable_ts_;
            std::vector<int64_t> tss;
            for (auto ts : durable_timestamps_) {
                tss.push_back(ts);
            }
            std::sort(tss.begin(), tss.end());
            auto ts = tss[tss.size() / 2];
            advance_to(std::min(durable_ts_, ts));
        }

    };

public:
    struct Options {
        bus::ProtoBus::Options bus_options;

        duration heartbeat_timeout;
        duration heartbeat_interval;
        duration election_timeout;
        duration flush_interval;
        duration gc_frequency;
        uint64_t flush_requests;
        std::filesystem::path dir;

        size_t pool_size;

        size_t rpc_max_batch;
        size_t members;
        ssize_t applied_backlog;
    };

    RaftNode(bus::EndpointManager& manager, Options options)
        : bus::ProtoBus(options.bus_options, manager)
        , buffer_pool_(options.bus_options.tcp_opts.max_message_size)
        , options_(options)
        , elector_([this] { initiate_elections(); }, options.election_timeout)
        , flusher_([this] { timed_flush(); }, options.flush_interval, ProtoBus::executor())
        , gc_([engine=&state_.get()->engine_] { engine->gc(); }, options.gc_frequency)
        , sender_([this] { heartbeat_to_followers(); }, options.heartbeat_interval, ProtoBus::executor())
        , stale_nodes_agent_( [this] { recover_stale_nodes(); }, options.heartbeat_interval)
    {
        {
            auto state = state_.get();
            state->engine_.reset(options.dir / "db", options_.pool_size);
            state->vote_keeper_.reset(state->engine_);
            state->applied_backlog = options.applied_backlog;

            state->flush_frequency = options_.flush_requests;

            assert(options.bus_options.greeter.has_value());
            state->id_ = id_ = *options.bus_options.greeter;
            state->next_timestamps_.assign(options_.members, 0);
            state->durable_timestamps_.assign(options_.members, -1);
            state->follower_heartbeats_.assign(options_.members, std::chrono::system_clock::time_point::min());
        }
        gc_.delayed_start();
        recover();
        flusher_.start();
        using namespace std::placeholders;
        register_handler<VoteRpc, Response>(kVote, [&] (int, VoteRpc rpc) { return bus::make_future(vote(rpc)); });
        register_handler<AppendRpcs, Response>(kAppendRpcs,
            [=](int node, AppendRpcs rpc, bus::Promise<Response> promise) {
                handle_append_rpcs(node, std::move(rpc), std::move(promise));
            });
        register_handler<ClientRequest, ClientResponse>(kClientReq, [=](int node, ClientRequest req) { return handle_client_request(node, std::move(req)); } );
        register_handler<RecoverySnapshot, Response>(kRecover, [&](int, RecoverySnapshot s) {
            Response r;
            r.set_success(handle_recovery_snapshot(std::move(s)));
            return bus::make_future(std::move(r));
        });
        sender_.delayed_start();
        elector_.delayed_start();
        stale_nodes_agent_.start();

        ProtoBus::start();
    }

    bus::internal::Event& shot_down() {
        return shot_down_;
    }

private:
    bool handle_recovery_snapshot(RecoverySnapshot s) {
        auto state = state_.get();

        if (state->role_ != kFollower || s.applied_ts() <= state->applied_ts_) {
            spdlog::debug("denied recover request with ts={0:d}", s.applied_ts());
            return false;
        }

        for (auto& op : s.operations()) {
            state->engine_.insert(state->engine_.copy_str(op.key()), state->engine_.copy_str(op.value()));
        }

        if (s.should_set_applied_ts()) {
            state->applied_ts_ = s.applied_ts();
            state->durable_ts_ = std::max(state->durable_ts_, state->applied_ts_);
            state->next_ts_ = state->durable_ts_ + 1;
            state->engine_.insert(
                state->engine_.copy_str(durable_ts_key()),
                state->engine_.copy_str(ts_to_str(state->durable_ts_)));
            state->engine_.insert(state->engine_.copy_str(applied_ts_key()), state->engine_.copy_str(ts_to_str(s.applied_ts())));
            state->engine_.iterate(rollback_key(0), rollback_key(state->applied_ts_),
                [&] (auto key, auto value) {
                    state->engine_.unsafe_erase(key);
                });
            state->engine_.commit();
            spdlog::debug("set applied_ts={0:d}", s.applied_ts());
        }
        return true;
    }

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
            state->vote_keeper_.store(rpc);
            state->leader_id_ = rpc.vote_for();
            spdlog::info("granted vote for {0:d}", rpc.vote_for());
            return state->create_response(true);
        }
    }

    bus::Future<ClientResponse> handle_client_request(int id, ClientRequest req) {
        DelayedSetter<bool> flush_event;
        bus::Future<ClientResponse> commit_future;
        LogRecord rec;
        auto handle_req_start = std::chrono::steady_clock::now();
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
                ClientResponse response;
                if (state->applied_ts_ < state->read_barrier_ts_) {
                    response.set_success(false);
                    return bus::make_future(std::move(response));
                }
                bool has_writes = false;
                bool has_reads = false;
                response.set_success(true);
                for (auto op : req.operations()) {
                    if (!allowed_key(op.key())) {
                        response.set_success(false);
                        return bus::make_future(std::move(response));
                    }
                    if (op.type() == ClientRequest::Operation::READ) {
                        auto entry = response.add_entries();
                        auto lim = base_key(op.key(), state->applied_ts_);
                        state->engine_.iterate(op.key(), lim,
                            [&] (auto key, auto value) {
                                entry->set_value(std::string(value));
                            });
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
                state->write(rec);
                state->buffered_log_.push_back(std::move(rec));
                //flush_event = state->account_write();
                commit_future = promise.future().map([response=std::move(response)](bool) { return response; });
            }
        }
        heartbeat_to_followers();
        {
            auto state = state_.get();
            flush_event = state->account_write();
        }
        commit_future.subscribe([=] (auto&) {
                spdlog::debug("commit taken {0:d}us", std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - handle_req_start).count());
            });
        return commit_future;
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
            term = ++state->current_term_;
            spdlog::info("starting elections term={0:d}", term);
            state->voted_for_me_.clear();
            state->role_ = kCandidate;
            state->leader_id_ = std::nullopt;
            state->latest_heartbeat_ = now;
        }
        std::this_thread::sleep_for((options_.election_timeout * (rand()%options_.members)) / (options_.members * 2));
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
                    state->vote_keeper_.store(self_vote);
                    state->voted_for_me_.insert(id_);
                }
                VoteRpc rpc;
                rpc.set_term(state->current_term_);
                rpc.set_ts(state->durable_ts_);
                rpc.set_vote_for(id_);
                for (size_t id = 0; id < options_.members; ++id) {
                    if (id != id_) {
                        responses.push_back(send<VoteRpc, Response>(rpc, id, kVote, options_.heartbeat_interval));
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
                                    state->commit_subscribers_.clear();
                                    state->advance_applied_timestamp();
                                    state->read_barrier_ts_ = state->durable_ts_;
                                    spdlog::info("becoming leader applied up to {0:d}, barrier ts is {1:d}", state->applied_ts_, state->read_barrier_ts_);
                                    for (auto & ts : state->durable_timestamps_) {
                                        ts = std::min(ts, state->applied_ts_);
                                    }
                                    state->next_timestamps_.assign(options_.members, state->applied_ts_ + 1);
                                }
                            }
                        }
                    });
        }
    }

    void handle_append_rpcs(int id, AppendRpcs msg, bus::Promise<Response> response) {
        DelayedSetter<bool> flush_notifier;
        //bus::Future<bool> flush_event;
        bool has_new_records = false;
        {
            auto state = state_.get();
            if (msg.term() < state->current_term_) {
                response.set_value(state->create_response(false));
                return;
            }
            if (msg.term() > state->current_term_) {
                spdlog::info("stale term becoming follower");
                state->current_term_ = msg.term();
                state->role_ = kFollower;
            }
            assert(state->role_ != kLeader);
            state->role_ = kFollower;
            state->latest_heartbeat_ = std::chrono::system_clock::now();
            state->leader_id_ = id;
            if (msg.records_size()) {
                spdlog::debug("handling heartbeat next_ts={0:d}", state->next_ts_);
            }
            for (auto& rpc : msg.records()) {
                if (rpc.ts() <= state->applied_ts_) {
                    continue;
                }
                if (rpc.ts() < state->next_ts_ && state->match_message(rpc)) {
                    continue;
                }
                while (rpc.ts() < state->next_ts_) {
                    assert(!state->buffered_log_.empty());
                    state->rollback(state->buffered_log_.back());
                    state->next_ts_ = state->buffered_log_.back().ts();
                    state->durable_ts_ = std::min(state->durable_ts_, rpc.ts() - 1);
                    state->buffered_log_.pop_back();
                }
                if (rpc.ts() == state->next_ts_) {
                    state->write(rpc);
                    state->buffered_log_.push_back(rpc);
                    ++state->next_ts_;
                    has_new_records = true;
                }
            }
            state->flush_event_.future().subscribe([this, response] (bool) mutable { response.set_value(state_.get()->create_response(true)); } );
            //flush_event = state->flush_event_.future();

            if (msg.records_size()) {
                flush_notifier = state->account_write();
            }

            state->advance_to(std::min(msg.applied_ts(), state->durable_ts_));
            if (msg.records_size()) {
                spdlog::debug("heartbeat handled next_ts={0:d}", state->next_ts_);
            }
        }

        flush_notifier.set();

        {
            auto state = state_.get();
            if (state->role_ != kLeader) {
                state->advance_to(std::min(msg.applied_ts(), state->durable_ts_));
            }
        }
        //should_be_set(flush_event);
        //flush_event.subscribe([pt=std::chrono::steady_clock::now()] (auto&) {
        //        spdlog::debug("respond to heartbeat after {0:d}us", std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - pt).count());
        //    });
        //return flush_event.map([this](bool) { return state_.get()->create_response(true); });
    }

    void recover_stale_nodes() {
        std::vector<size_t> nodes;
        std::vector<int64_t> nexts;
        if (auto state = state_.get(); state->role_ == kLeader) {
            for (size_t id = 0; id < options_.members; ++id) {
                int64_t ts = !state->buffered_log_.empty() ? state->buffered_log_[0].ts() : state->applied_ts_;
                if (id_ != id) {
                    if (state->next_timestamps_[id] < ts) {
                        nodes.push_back(id);
                        nexts.push_back(state->next_timestamps_[id]);
                        spdlog::info("should recover {0:d} with next_ts={1:d}. my applied_ts={2:d}", id, state->next_timestamps_[id], state->applied_ts_);
                    }
                }
            }
        }

        auto recover_node = [&](size_t node, int64_t next) {
            spdlog::info("starting recovery for {0:d} ts={1:d}", node, next);
            std::unordered_map<std::string, std::string> fsm;
            int64_t applied_ts = -1;
            auto root = state_.get()->engine_.root();
            if (auto serialized = root.lookup(applied_ts_key())) {
                applied_ts = str_to_ts(*serialized);
            }

            auto send_snapshot = [=] (RecoverySnapshot rec) {
                rec.set_applied_ts(applied_ts);
                spdlog::debug("snapshot ts = {0:1}, messages={1:d}", rec.applied_ts(), rec.operations_size());
                bus::ErrorT<Response> resp = send<RecoverySnapshot, Response>(std::move(rec), node, kRecover, options_.heartbeat_timeout).wait();
                bool result = resp && resp.unwrap().success();
                if (!result) {
                    spdlog::info("failed to recover {0:d}", node);
                }
                return result;
            };

            spdlog::info("sending snapshot ts={0:d} to node={1:d}", applied_ts, node);
            RecoverySnapshot rec;
            root.iterate([&] (std::string_view key, std::string_view value) mutable {
                    if (reserved_key(key) || ts_from_base_key(key) > applied_ts) {
                        return;
                    }
                    auto op = rec.add_operations();
                    op->set_key(std::string(key));
                    op->set_value(std::string(value));
                    if (rec.operations_size() >= options_.rpc_max_batch) {
                        if (!send_snapshot(std::move(rec))) {
                            return;
                        }
                        rec.Clear();
                    }
                });
            rec.set_should_set_applied_ts(true);
            if (!send_snapshot(std::move(rec))) {
                return;
            }
            if (auto serialized = root.lookup(durable_ts_key())) {
                next = std::max<int64_t>(next, applied_ts + 1);
            }
            spdlog::info("successful recovery node={0:d} next_ts={1:d}", node, next);
            {
                auto state = state_.get();
                state->next_timestamps_[node] = std::max(state->next_timestamps_[node], next);
            }
        };

        for (size_t i = 0; i < nodes.size(); ++i) {
            recover_node(nodes[i], nexts[i]);
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
                rpcs.set_applied_ts(state->applied_ts_);
                if (state->buffered_log_.size() > 0 && next_ts >= state->buffered_log_[0].ts()) {
                    const size_t start_ts = state->buffered_log_[0].ts();
                    const size_t start_index = next_ts - start_ts;
                    for (size_t i = start_index; i < state->buffered_log_.size() && rpcs.records_size() < options_.rpc_max_batch; ++i) {
                        *rpcs.add_records() = state->buffered_log_[i];
                        // avoiding rollbacks
                        state->next_timestamps_[id] = state->buffered_log_[i].ts() + 1;
                    }
                }
                if (rpcs.records_size()) {
                    spdlog::debug("sending to {0:d} {1:d} records upper ts={2:d}", id, rpcs.records_size(), state->buffered_log_.back().ts());
                }
                messages.push_back(std::move(rpcs));
            }
        }
        for (size_t i = 0; i < endpoints.size(); ++i) {
            bool to_log = messages[i].records_size() > 0;
            send<AppendRpcs, Response>(std::move(messages[i]), endpoints[i], kAppendRpcs, options_.heartbeat_interval)
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
                                state->advance_applied_timestamp();
                                subscribers = state->pick_subscribers();
                                for (auto& f : subscribers) {
                                    f.set_value(true);
                                }
                            } else {
                                spdlog::debug("node {0:d} failed heartbeat", id);
                            }
                        }

                    } );
        }
    }

    void timed_flush() {
        DelayedSetter<bool> to_deliver;
        {
            auto state = state_.get();
            to_deliver = state->flush();
        }
        to_deliver.set();
    }

    void recover() {
        auto state = state_.get();

        if (auto serialized = state->engine_.lookup(durable_ts_key())) {
            state->durable_ts_ = str_to_ts(*serialized);
        }
        if (auto serialized = state->engine_.lookup(applied_ts_key())) {
            state->applied_ts_ = str_to_ts(*serialized);
        }
        state->next_ts_ = state->durable_ts_ + 1;
        assert(state->durable_ts_ >= state->applied_ts_);
        for (ssize_t i = state->applied_ts_ + 1; i <= state->durable_ts_; ++i) {
            auto record = state->engine_.lookup(rollback_key(i));
            assert(record);
            PersistentStrArray array{const_cast<char*>(record->data()), record->size()};
            state->buffered_log_.emplace_back();
            state->buffered_log_.back().set_ts(i);
            for (size_t j = 0; j < array.size(); ++j) {
                auto op = state->buffered_log_.back().add_operations();
                auto key = ::from_base_key({ array[j].data(), array[j].size() });
                op->set_key(std::string(key));
                auto value = state->engine_.lookup({ array[j].data(), array[j].size() });
                assert(value);
                op->set_value(std::string(*value));
            }
        }
        if (auto vote = state->vote_keeper_.recover()) {
            state->current_term_ = vote->term();
            state->leader_id_ = vote->vote_for();
        }
        spdlog::info("recovered term={0:d} durable_ts={1:d} applied_ts={2:d} next_ts={3:d}", state->current_term_, state->durable_ts_, state->applied_ts_, state->next_ts_);
    }

private:
    bus::BufferPool buffer_pool_;
    Options options_;
    bus::internal::ExclusiveWrapper<State, bus::internal::SpinLock> state_;

    bus::internal::PeriodicExecutor elector_;
    bus::internal::PeriodicExecutor flusher_;
    bus::internal::PeriodicExecutor gc_;
    bus::internal::PeriodicExecutor sender_;
    bus::internal::PeriodicExecutor stale_nodes_agent_;

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
    srand(id);
    if (auto max_messages = conf["max_pending_messages"]; !max_messages.isNull()) {
        options.bus_options.tcp_opts.max_pending_messages = max_messages.asInt();
    }
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
    options.flush_interval = parse_duration(conf["flush_interval"]);
    options.rpc_max_batch = conf["rpc_max_batch"].asUInt64();
    options.flush_requests = conf["flush_req_interval"].asUInt64();
    options.gc_frequency = parse_duration(conf["gc"]);
    options.pool_size = conf["db_pool_size"].asUInt64() * 1024 * 1024;
    options.members = members.size();

    spdlog::set_pattern("[%H:%M:%S.%F] [" + std::to_string(id) + "] [%^%l%$] %v");

    if (auto level = conf["log_level"]; !level.isNull() && level.asString() == "debug") {
        spdlog::set_level(spdlog::level::debug);
    }

    spdlog::info("starting node");

    RaftNode node(manager, options);
    node.shot_down().wait();
}
