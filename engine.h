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

class Engine {
public:
    struct PersistentStr {
        pmem::obj::persistent_ptr<char> ptr_;

        char* data() const {
            return ptr_.get() + sizeof(uint64_t);
        }

        size_t size() const {
            if (ptr_)
                return *reinterpret_cast<uint64_t*>(ptr_.get());
            else
                return 0;
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

    static constexpr size_t max_children = 7;
    static constexpr size_t min_children = 4;
    //static constexpr size_t max_children = 3;
    //static constexpr size_t min_children = 2;
    struct KeyNode {
        std::array<pmem::obj::persistent_ptr<char>, max_children> children;
        std::array<PersistentStr, max_children> keys;
        uint64_t key_count = 0;

        bool is_leaf;

        pmem::obj::persistent_ptr<KeyNode> child(size_t i) {
            return children[i].raw();
        }
    };

    struct Page {
        pmem::obj::persistent_ptr<Page> next = nullptr;
        pmem::obj::p<uint64_t> used = 0;
        char data[page_sz_];
    };

    struct Root {
        pmem::obj::persistent_ptr<Page> free_pages = nullptr;
        pmem::obj::persistent_ptr<Page> used_pages = nullptr;

        pmem::obj::mutex lock;

        pmem::obj::persistent_ptr<Page> stale_gc_root_;
        pmem::obj::persistent_ptr<KeyNode> durable_root_ = nullptr;
    };

    struct DirtyPage {
        Page* page = nullptr;
        uint64_t start = 0;
    };

private:
    void allocpage() {
        pmem::obj::transaction::run(
            pool_,
            [&] {
                auto old = root_->free_pages;
                root_->free_pages = pmem::obj::make_persistent<Page>();
                root_->free_pages->next = old;
            },
            root_->lock);
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
                pmem::obj::transaction::run(
                    pool_,
                    [&] {
                        root_->free_pages = page->next;
                        page->next = root_->used_pages;
                        root_->used_pages = page;
                    },
                    root_->lock);
            }
        }
    }

    template<typename T>
    T* allocate() {
        return (T*)allocate(sizeof(T), alignof(T));
    }

    struct InsertResult {
        KeyNode* left = nullptr;
        KeyNode* right = nullptr;
    };

    PersistentStr prevkey;
    void traverse(KeyNode* node) {
        if (!node) return;
        if (node->is_leaf) {
            for (size_t i = 0; i < node->key_count; ++i) {
                assert(prevkey < node->keys[i]);
                prevkey = node->keys[i];
            }
        } else {
            for (size_t i = 0; i < node->key_count; ++i) {
                traverse(node->child(i).get());
            }
        }
    }

    void assert_node(KeyNode* node) {
#ifndef NDEBUG
        for (size_t i = 0; i + 1 < node->key_count; ++i) {
            assert(node->keys[i] < node->keys[i + 1]);
        }
        prevkey = PersistentStr{};
        if (!node->is_leaf) {
            for (size_t i = 0; i < node->key_count; ++i) {
                assert(node->keys[i] == node->child(i)->keys[0]);
                assert(prevkey < node->keys[i]);
                traverse(node->child(i).get());
            }
        }
        prevkey = PersistentStr{};
        traverse(node);
#endif
    }

    InsertResult insert(KeyNode* root, PersistentStr key, PersistentStr value) {
        if (root == nullptr) {
            root = allocate<KeyNode>();
            root->key_count = 1;
            root->keys[0] = key;
            root->children[0] = value.ptr_;
            root->is_leaf = true;
            assert_node(root);
            return { root };
        } else {
            ssize_t pos = 0;

            PersistentStr insert_key;
            pmem::obj::persistent_ptr<char> insert_value;

            KeyNode* new_root = allocate<KeyNode>();
            memcpy(new_root, root, sizeof(KeyNode));
            //*new_root = *root;

            if (root->is_leaf) {
                //while (pos < root->key_count && root->keys[pos] < key) {
                //    ++pos;
                //}
                pos = std::lower_bound(&root->keys[0], &root->keys[root->key_count], key) - &root->keys[0];

                // insert() == replace()
                if (pos < root->key_count && root->keys[pos] == key) {
                    new_root->children[pos] = value.ptr_;
                    assert_node(new_root);
                    return InsertResult{ .left = new_root, .right = nullptr };
                }
                //

                insert_key = key;
                insert_value = value.ptr_;
            } else {
                //while (pos + 1 < root->key_count && root->keys[pos + 1] <= key) {
                //    ++pos;
                //}
                pos = std::max<ssize_t>(std::upper_bound(&root->keys[0], &root->keys[root->key_count], key) - &root->keys[0] - 1, 0);
                auto result = insert(root->child(pos).get(), key, value);

                if (result.right) {
                    new_root->keys[pos] = result.left->keys[0];
                    new_root->children[pos] = (char*)result.left;

                    insert_key = result.right->keys[0];
                    insert_value = (char*)result.right;
                    ++pos;
                } else {
                    new_root->keys[pos] = result.left->keys[0];
                    new_root->children[pos] = (char*)result.left;
                    assert_node(new_root);
                    return { new_root };
                }
            }

            if (new_root->key_count == max_children) {
                InsertResult result;
                result.left = new_root;
                result.right = allocate<KeyNode>();
                result.right->key_count = (max_children+1)/2;
                //result.left->key_count = result.right->key_count = 0;
                result.left->is_leaf = result.right->is_leaf = root->is_leaf;

                assert(new_root->key_count > result.right->key_count);
                ssize_t rpos = ssize_t(result.right->key_count) - 1;
                ssize_t lpos = new_root->key_count - result.right->key_count;
                size_t lsize = lpos + 1;
                assert(lpos + 1 + rpos + 1 == new_root->key_count + 1);

                auto add_key = [&] (PersistentStr key, pmem::obj::persistent_ptr<char> value) mutable {
                    if (rpos >= 0) {
                        result.right->keys[rpos] = key;
                        result.right->children[rpos] = value;
                        --rpos;
                    } else {
                        result.left->keys[lpos] = key;
                        result.left->children[lpos] = value;
                        --lpos;
                    }
                };

                if (pos == new_root->key_count) {
                    add_key(insert_key, insert_value);
                }
                for (ssize_t i = new_root->key_count - 1; i >= 0; --i) {
                    add_key(new_root->keys[i], new_root->children[i]);
                    if (i == pos) {
                        add_key(insert_key, insert_value);
                    }
                }
                result.left->key_count = lsize;

                assert_node(result.left);
                assert_node(result.right);
                return result;
            } else {
                ++new_root->key_count;
                for (ssize_t i = new_root->key_count - 1; i > pos; --i) {
                    new_root->keys[i] = new_root->keys[i - 1];
                    new_root->children[i] = new_root->children[i - 1];
                }
                new_root->keys[pos] = insert_key;
                new_root->children[pos] = insert_value;
                return { new_root };
            }
        }
    }

    KeyNode* erase(KeyNode* root, std::string_view key) {
        if (root == nullptr) {
            return nullptr;
        }
        size_t pos = 0;
        if (root->is_leaf) {
            //while (pos < root->key_count && root->keys[pos] < key) {
            //    ++pos;
            //}
            pos = std::lower_bound(&root->keys[0], &root->keys[root->key_count], key) - &root->keys[0];
            if (!(root->keys[pos] == key)) {
                throw std::logic_error("can't delete nonexistent keys");
            }
            //assert(root->keys[pos] == key);

            if (root->key_count == 1) {
                return nullptr;
            }

            KeyNode* result = allocate<KeyNode>();
            memcpy(result, root, sizeof(KeyNode));
            for (size_t i = pos; i < root->key_count; ++i) {
                result->children[i] = result->children[i + 1];
                result->keys[i] = result->keys[i + 1];
            }
            --result->key_count;
            assert_node(result);
            return result;
        } else {
            //while (pos + 1 < root->key_count && root->keys[pos + 1] <= key) {
            //    ++pos;
            //}
            pos = std::max<ssize_t>(std::upper_bound(&root->keys[0], &root->keys[root->key_count], key) - &root->keys[0] - 1, 0);
            auto subnode = erase((KeyNode*)root->children[pos].get(), key);
            if (root->key_count == 1) {
                return subnode;
            }
            assert(subnode);
            KeyNode* node = allocate<KeyNode>();
            memcpy(node, root, sizeof(KeyNode));
            node->children[pos] = (char*)subnode;

            if (subnode->key_count < min_children) {
                int di = pos == 0 && pos + 1 < subnode->key_count ? 1 : -1;

                const size_t secondary_count = node->child(pos + di)->key_count + subnode->key_count;
                if (secondary_count > max_children) {
                    const size_t cut = secondary_count / 2;
                    KeyNode* neighbor = allocate<KeyNode>();
                    memcpy(neighbor, node->child(pos + di).get(), sizeof(KeyNode));
                    node->children[pos + di] = (char*)neighbor;

                    KeyNode* left = node->child(pos + std::min(0, di)).get();
                    KeyNode* right = node->child(pos + std::max(0, di)).get();

                    assert(cut >= min_children);
                    assert(secondary_count - cut >= min_children);
                    assert(secondary_count - cut < max_children);

                    if (left->key_count > cut) {
                        size_t delta = left->key_count - cut;
                        for (ssize_t i = right->key_count - 1; i >= 0; --i) {
                            right->children[i + delta] = right->children[i];
                            right->keys[i + delta] = right->keys[i];
                        }
                        for (size_t i = 0; i < delta; ++i) {
                            right->children[i] = left->children[cut + i];
                            right->keys[i] = left->keys[cut + i];
                        }
                    } else {
                        size_t delta = cut - left->key_count;
                        for (size_t i = 0; i < delta; ++i) {
                            left->keys[left->key_count + i] = right->keys[i];
                            left->children[left->key_count + i] = right->children[i];
                        }
                        size_t rsize = secondary_count - cut;
                        for (size_t i = 0; i < rsize; ++i) {
                            right->children[i] = right->children[i + delta];
                            right->keys[i] = right->keys[i + delta];
                        }
                    }

                    left->key_count = cut;
                    right->key_count = secondary_count - cut;

                    node->keys[pos] = node->child(pos)->keys[0];
                    node->keys[pos + di] = node->child(pos + di)->keys[0];
                    assert_node(node);
                } else {
                    KeyNode* neighbor = node->child(pos + di).get();
                    node->children[pos + di] = nullptr;

                    if (di < 0) {
                        ssize_t delta = neighbor->key_count;
                        for (ssize_t i = subnode->key_count - 1; i >= 0; --i) {
                            subnode->keys[i + delta] = subnode->keys[i];
                            subnode->children[i + delta] = subnode->children[i];
                        }
                        for (size_t i = 0; i < delta; ++i) {
                            subnode->keys[i] = neighbor->keys[i];
                            subnode->children[i] = neighbor->children[i];
                        }
                        subnode->key_count += delta;
                        assert_node(subnode);
                    } else {
                        for (size_t i = 0; i < neighbor->key_count; ++i) {
                            subnode->keys[subnode->key_count] = neighbor->keys[i];
                            subnode->children[subnode->key_count] = neighbor->children[i];
                            ++subnode->key_count;
                        }
                        assert_node(subnode);
                    }
                    node->keys[pos] = node->child(pos)->keys[0];
                }

                for (ssize_t i = pos + di; i + 1 < node->key_count; ++i) {
                    if (node->children[i] == nullptr) {
                        node->keys[i] = node->keys[i + 1];
                        node->children[i] = node->children[i + 1];
                        node->children[i + 1] = nullptr;
                    }
                }
                if (node->children[node->key_count - 1] == nullptr) {
                    --node->key_count;
                }

                assert_node(node);
                return node;
            } else {
                node->children[pos] = (char*)subnode;
                node->keys[pos] = subnode->keys[0];
                assert_node(node);
                return node;
            }
        }
    }

    template<typename F, typename Comp>
    bool iterate(KeyNode* root, Comp&& cmp, F&& f) {
        if (!root) return true;
        if (root->is_leaf) {
            for (size_t i = 0; i < root->key_count; ++i) {
                if (cmp(root->keys[i]) == 0) {
                    if (!f(root->keys[i], PersistentStr{root->children[i]})) {
                        return false;
                    }
                }
            }
        } else {
            for (size_t i = 0; i < root->key_count; ++i) {
                if (i + 1 < root->key_count && cmp(root->keys[i]) < 0 && cmp(root->keys[i + 1]) <= 0) {
                    continue;
                }
                if (cmp(root->keys[i]) > 0) {
                    break;
                }
                if (!iterate((KeyNode*)root->children[i].get(), cmp, std::forward<F>(f))) {
                    return false;
                }
            }
        }
        return true;
    }

    void visit_str(const PersistentStr& str, std::vector<pmem::obj::persistent_ptr<void>>& collector) {
        collector.push_back(str.ptr_.raw());
    }

    void visit_node(pmem::obj::persistent_ptr<KeyNode> node, std::vector<pmem::obj::persistent_ptr<void>>& collector) {
        if (!node) {
            return;
        }
        collector.push_back(node.raw());
        for (size_t i = 0; i < node->key_count; ++i) {
            visit_str(node->keys[i], collector);
            if (node->is_leaf) {
                visit_str({ node->children[i].raw() }, collector);
            } else {
                visit_node(node->children[i].raw(), collector);
            }
        }
    }

public:
    class RootHolder {
    public:
        RootHolder(KeyNode* root, std::function<void()> deleter, Engine& parent)
            : root_(root)
            , deleter_(deleter)
            , parent_(parent)
        {
        }

        RootHolder(const RootHolder&) = delete;

        template<typename F>
        void iterate(F&& f) {
            parent_.iterate(root_,
                [](auto) { return 0; },
                [&] (const PersistentStr& key_, const PersistentStr& value_) {
                    f({key_.data(), key_.size()}, {value_.data(), value_.size()});
                    return true;
                });
        }

        std::optional<std::string_view> lookup(std::string_view key) {
            return parent_.lookup(root_, key);
        }

        ~RootHolder() {
            if (deleter_) {
                deleter_();
            }
        }

    private:
        KeyNode* root_ = nullptr;
        std::function<void()> deleter_;
        Engine& parent_;
    };

    RootHolder root() {
        std::unique_lock lock(root_->lock);
        auto iterator = gc_pinned_nodes_.insert(gc_pinned_nodes_.end(), volatile_root_);
        return RootHolder{
            root_->durable_root_.get(),
            [=] {
                std::unique_lock lock(root_->lock);
                gc_pinned_nodes_.erase(iterator);
            },
            *this
        };
    }

    std::optional<std::string_view> lookup(KeyNode* root, std::string_view key) {
        std::optional<std::string_view> result;
        iterate(root, key, key,
            [&] (std::string_view key_, std::string_view value_) {
                if (key == key_) {
                    result = value_;
                    return false;
                } else {
                    return true;
                }
            });
        return result;
    }

    template<typename F>
    void iterate(KeyNode* root, std::string_view start, std::string_view end, F&& f) {
        iterate(root,
            [&] (PersistentStr str) {
                if (str < start) return -1;
                if (str > end) return 1;
                return 0;
            },
            [&] (const PersistentStr& key_, const PersistentStr& value_) {
                f(std::string_view{key_.data(), key_.size()}, std::string_view{value_.data(), value_.size()});
                return true;
            });
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
        root_ = pool_.root().get();
        volatile_root_ = root_->durable_root_.get();
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
        auto result = insert(volatile_root_, key, value);
        if (result.right) {
            volatile_root_ = allocate<KeyNode>();
            volatile_root_->is_leaf = false;
            volatile_root_->key_count = 2;
            volatile_root_->keys[0] = result.left->keys[0];
            volatile_root_->keys[1] = result.right->keys[0];
            volatile_root_->children[0] = (char*)result.left;
            volatile_root_->children[1] = (char*)result.right;
        } else {
            volatile_root_ = result.left;
        }
    }

    template<typename F>
    void iterate(std::string_view start, std::string_view end, F&& f) {
        iterate(volatile_root_, start, end, std::forward<F>(f));
    }

    std::optional<std::string_view> lookup(std::string_view key) {
        return lookup(volatile_root_, key);
    }

    void unsafe_erase(std::string_view key) {
        auto root = erase(volatile_root_, key);
        if (root && !root->is_leaf && root->key_count == 1) {
            root = root->child(0).get();
        }
        volatile_root_ = root;
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
        pmem::obj::transaction::run(
            pool_,
            [&] {
                root_->stale_gc_root_ = root_->used_pages;
                root_->durable_root_ = volatile_root_;
            },
            root_->lock);
    }

    void commit() {
        flush();
        sync();
        store_root();
    }

    void gc() {
        std::vector<pmem::obj::persistent_ptr<void>> visited;
        pmem::obj::persistent_ptr<Page> stale_gc_root;
        std::vector<pmem::obj::persistent_ptr<KeyNode>> pinned;
        pinned.reserve(10);
        {
            std::unique_lock lock(root_->lock);
            pinned.push_back(root_->durable_root_.get());
            for (auto node : gc_pinned_nodes_) {
                pinned.push_back(node);
            }
            stale_gc_root = root_->stale_gc_root_;
        }
        for (auto node : pinned) {
            visit_node(node, visited);
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
                        for (auto node : gc_pinned_nodes_) {
                            visit_node(volatile_root_, visited);
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
    pmem::obj::pool<Root> pool_;
    Root* root_;
    std::list<pmem::obj::persistent_ptr<KeyNode>> gc_pinned_nodes_;

    const std::string layout_ = "kv_engine";
    std::vector<DirtyPage> dirty_pages_;

    KeyNode* volatile_root_ = nullptr;
};

inline bool operator < (std::string_view view, Engine::PersistentStr str) {
    return view < std::string_view{ str.data(), str.size() };
}
