#pragma once

#include <libpmemobj++/mutex.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/container/array.hpp>
#include <libpmemobj++/container/string.hpp>
#include <libpmemobj++/container/vector.hpp>

#include <filesystem>
#include <mutex>
#include <set>
#include <string>
#include <list>
#include <map>

class Engine {
public:
    struct PersistentStr {
        pmem::obj::persistent_ptr<char> ptr_ = nullptr;

        char* data() const {
            return ptr_.get() + sizeof(uint64_t);
        }

        size_t size() const {
            if (ptr_)
                return *reinterpret_cast<uint64_t*>(ptr_.get());
            else
                return 0;
        }

        std::string_view view() const {
            std::string_view result{ data(), size() };
            assert(data() == result.data());
            assert(size() == result.size());
            return { data(), size() };
        }

        char operator[](size_t i) const {
            return data()[i];
        }

        void set_size(uint64_t sz) {
            *reinterpret_cast<uint64_t*>(ptr_.get()) = sz;
        }

        static size_t size(size_t chars) {
            return sizeof(uint64_t) + chars;
        }

        static size_t align() {
            return alignof(uint64_t);
        }

        operator bool () const {
            return ptr_ != nullptr;
        }

        bool operator < (const PersistentStr& oth) const {
            return std::string_view{data(), size()} < std::string_view{oth.data(), oth.size()};
        }

        bool operator > (const PersistentStr& oth) const {
            return std::string_view{data(), size()} > std::string_view{oth.data(), oth.size()};
        }

        bool operator <= (const PersistentStr& oth) const {
            return std::string_view{data(), size()} <= std::string_view{oth.data(), oth.size()};
        }

        bool operator == (const PersistentStr& oth) const {
            return std::string_view{data(), size()} == std::string_view{oth.data(), oth.size()};
        }

        bool operator < (const std::string_view& oth) const {
            return std::string_view{data(), size()} < oth;
        }

        bool operator > (const std::string_view& oth) const {
            return std::string_view{data(), size()} > oth;
        }

        bool operator <= (const std::string_view& oth) const {
            return std::string_view{data(), size()} <= oth;
        }

        bool operator >= (const std::string_view& oth) const {
            return std::string_view{data(), size()} >= oth;
        }

        bool operator == (const std::string_view& oth) const {
            return std::string_view{data(), size()} == oth;
        }
    };


private:
    static constexpr size_t page_sz_ = 8192 * 16;
    //static constexpr size_t page_sz_ = 120;

    struct KVNode {
        PersistentStr key;
        PersistentStr value;
        pmem::obj::persistent_ptr<KVNode> next = nullptr;
        bool tombstone = false;
    };

    struct Page {
        pmem::obj::persistent_ptr<Page> next = nullptr;
        pmem::obj::p<uint64_t> used = 0;
        char data[page_sz_];
    };

    struct Root {
        pmem::obj::persistent_ptr<Page> free_pages = nullptr;
        pmem::obj::persistent_ptr<Page> used_pages = nullptr;

        pmem::obj::persistent_ptr<KVNode> durable_root_;
        pmem::obj::persistent_ptr<Page> stale_gc_root_;
    };

    struct DirtyPage {
        Page* page = nullptr;
        uint64_t start = 0;
    };

private:
    void allocpage() {
        std::unique_lock lock(root_lock);
        pmem::obj::transaction::run(
            pool_,
            [&] {
                auto old = root_->free_pages;
                root_->free_pages = pmem::obj::make_persistent<Page>();
                root_->free_pages->next = old;
            });
    }

    char* allocate(size_t sz, uint64_t align) {
        assert(sz + align <= page_sz_);
        while (true) {
            if (root_->free_pages == nullptr) {
                allocpage();
            }
            auto page = root_->free_pages;
            if (page->used & (align - 1)) {
                page->used += align - (page->used & (align - 1));
            }
            if (page->used + sz <= page_sz_) {
                auto ptr = page->data + page->used;
                if (dirty_pages_.empty() || dirty_pages_.back().page != page.get()) {
                    dirty_pages_.push_back({ page.get(), page->used });
                }
                page->used += sz;
                return ptr;
            } else {
                std::unique_lock lock(root_lock);
                pmem::obj::transaction::run(
                    pool_,
                    [&] {
                        root_->free_pages = page->next;
                        page->next = root_->used_pages;
                        root_->used_pages = page;
                    });
            }
        }
    }

    template<typename T>
    T* allocate() {
        return (T*)allocate(sizeof(T), alignof(T));
    }

    void visit_str(const PersistentStr& str, std::vector<pmem::obj::persistent_ptr<void>>& collector) {
        collector.push_back(str.ptr_.raw());
    }

    void visit_node(pmem::obj::persistent_ptr<KVNode> node, std::vector<pmem::obj::persistent_ptr<void>>& collector) {
        if (!node) {
            return;
        }
        collector.push_back(node.raw());
        visit_str(node->key, collector);
        if (!node->tombstone) {
            visit_str(node->value, collector);
        }
    }

public:
    class RootHolder {
    public:
        RootHolder(KVNode* root, std::unique_lock<std::mutex> lock, Engine& parent)
            : root_(root)
            , lock_(std::move(lock))
        {
            for (auto node = root; node; node = node->next.get()) {
                if (index.find(node->key.view()) == index.end()) {
                    if (node->tombstone) {
                        index[node->key.view()] = std::nullopt;
                    } else {
                        index[node->key.view()] = node->value;
                    }
                }
            }
        }

        RootHolder(const RootHolder&) = delete;

        template<typename F>
        void iterate(F&& f) {
            for (auto it = index.begin(); it != index.end(); ++it) {
                if (it->second) {
                    f(it->first, it->second.value().view());
                }
            }
        }

        std::optional<std::string_view> lookup(std::string_view key) {
            auto str = index[key];
            if (str) {
                return str->view();
            } else {
                return std::nullopt;
            }
        }

    private:
        KVNode* root_ = nullptr;
        std::unique_lock<std::mutex> lock_;
        std::map<std::string_view, std::optional<PersistentStr>> index;
    };

    RootHolder root() {
        std::unique_lock gc_lock{gc_lock_};
        std::unique_lock lock{root_lock};
        return RootHolder{
            root_->durable_root_.get(),
            std::move(gc_lock),
            *this
        };
    }

public:
    Engine() = default;

    template<typename... Args>
    Engine(Args&&... args) {
        reset(std::forward<Args>(args)...);
    }

    void reset(std::string fname, size_t pool_size=PMEMOBJ_MIN_POOL, mode_t mode=S_IWUSR | S_IRUSR) {
        if (std::filesystem::exists(fname)) {
            pool_ = pmem::obj::pool<Root>::open(fname, layout_);
            root_ = pool_.root().get();
            std::set<std::string_view> visited_keys;
            for (auto node = root_->durable_root_; node; node = node->next) {
                if (visited_keys.find(node->key.view()) == visited_keys.end() && !node->tombstone) {
                    index_[node->key.view()] = node.get();
                }
                visited_keys.insert(node->key.view());
            }
            volatile_root_.store(root_->durable_root_.get());
        } else {
            pool_ = pmem::obj::pool<Root>::create(fname, layout_, pool_size, mode);
            root_ = pool_.root().get();
            for (size_t i = 0; i + 1 < pool_size / sizeof(Page); ++i) {
                try {
                    allocpage();
                } catch(pmem::transaction_out_of_memory) {
                    // we shouldnt allocate any more
                    break;
                }
            }
        }
        dirty_pages_.clear();
    }

    PersistentStr allocate_str(size_t sz) {
        PersistentStr result{  allocate(PersistentStr::size(sz), PersistentStr::align()) };
        result.set_size(sz);
        return result;
    }

    PersistentStr copy_str(std::string_view v) {
        PersistentStr result = allocate_str(v.size());
        memcpy(result.data(), v.data(), v.size());
        return result;
    }

    void insert(PersistentStr key, PersistentStr value) {
        auto kvnode = allocate<KVNode>();
        kvnode->key = key;
        kvnode->value = value;
        kvnode->next = volatile_root_.load();
        kvnode->tombstone = false;
        volatile_root_.store(kvnode);

        index_[key.view()] = kvnode;
    }

    template<typename F>
    void iterate(std::string_view start, std::string_view end, F&& f) {
        for (auto it = index_.lower_bound(start); it != index_.end() && it->first <= end; ++it) {
            f(it->second->key.view(), it->second->value.view());
        }
    }

    std::optional<std::string_view> lookup(std::string_view key) {
        auto it = index_.find(key);
        if (it == index_.end()) {
            return std::nullopt;
        } else {
            return it->second->value.view();
        }
    }

    void unsafe_erase(std::string_view key) {
        auto kvnode = allocate<KVNode>();
        kvnode->key = copy_str(key);
        kvnode->tombstone = true;
        kvnode->next = volatile_root_.load();
        volatile_root_.store(kvnode);

        index_.erase(key);
    }

    void erase(std::string_view key) {
        if (lookup(key)) {
            unsafe_erase(key);
        }
    }

    void sync() {
        pool_.drain();
    }

    void flush() {
        for (auto [page, start] : dirty_pages_) {
            pool_.flush(page->used);
            pool_.flush(&page->data[start], page->used - start);
        }
        dirty_pages_.clear();
    }

    void store_root() {
        std::unique_lock lock{root_lock};
        pmem::obj::transaction::run(
            pool_,
            [&] {
                root_->stale_gc_root_ = root_->used_pages;
                root_->durable_root_ = volatile_root_.load();
            });
    }

    void commit() {
        flush();
        sync();
        store_root();
    }

    void gc() {
        std::unique_lock<std::mutex> lock{gc_lock_};

        std::vector<pmem::obj::persistent_ptr<void>> visited;
        std::set<std::string_view> keys;
        pmem::obj::persistent_ptr<Page> stale_gc_root;
        KVNode* durable_root;
        {
            std::unique_lock lock{root_lock};
            stale_gc_root = root_->stale_gc_root_;
            durable_root = root_->durable_root_.get();
        }

        KVNode* prev = nullptr;
        for (auto node = durable_root; node; node = node->next.get()) {
            if ((keys.find(node->key.view()) != keys.end() || node->tombstone) && prev) {
                pmem::obj::transaction::run(
                    pool_,
                    [&] {
                        prev->next = node->next;
                    });
            } else {
                visit_node(node, visited);
                prev = node;
            }
            keys.insert(node->key.view());
        }

        std::vector<pmem::obj::persistent_ptr<Page>> pages;
        for (auto page = stale_gc_root; page; page = page->next) {
            pages.push_back(page);
        }

        std::sort(visited.begin(), visited.end());
        std::sort(pages.begin(), pages.end());
        std::set<pmem::obj::persistent_ptr<Page>> page_set;
        size_t visited_pos = 0;
        for (auto page : pages) {
            while (visited_pos < visited.size() && visited[visited_pos].raw().off < page.raw().off) {
                ++visited_pos;
            }
            if (visited_pos < visited.size() && visited[visited_pos].raw().off <= page.raw().off + sizeof(Page)) {
                page_set.insert(page);
            }
        }
        pmem::obj::transaction::run(
            pool_,
            [&] {
                for (auto page = stale_gc_root; page && page->next; page = page->next) {
                    if (page_set.find(page->next) == page_set.end()) {
                        auto free = page->next;
                        free->used = 0;
#ifndef NDEBUG
                        std::vector<pmem::obj::persistent_ptr<void>> visited;
                        for (auto node = volatile_root_.load(); node; node = node->next.get()) {
                            visit_node(node, visited);
                        }
                        for (auto ptr : visited) {
                            assert(ptr.raw().off < free.raw().off || ptr.raw().off > free.raw().off + sizeof(Page));
                        }
#endif
                        page->next = page->next->next;
                        free->next = root_->free_pages;
                        root_->free_pages = free;
                    }
                }
            });
    }

private:
    std::map<std::string_view, KVNode*> index_;

    pmem::obj::pool<Root> pool_;
    Root* root_;
    std::mutex gc_lock_;

    const std::string layout_ = "kv_engine";
    std::vector<DirtyPage> dirty_pages_;

    std::atomic<KVNode*> volatile_root_ = nullptr;

    std::mutex root_lock;
};

inline bool operator < (std::string_view view, Engine::PersistentStr str) {
    return view < std::string_view{ str.data(), str.size() };
}
