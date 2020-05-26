#pragma once

#include <libpmemobj++/mutex.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/container/array.hpp>
#include <libpmemobj++/container/string.hpp>
#include <libpmemobj++/container/vector.hpp>

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

        bool operator < (const PersistentStr& oth) {
            return std::string_view{data(), size()} < std::string_view{oth.data(), oth.size()};
        }

        bool operator <= (const PersistentStr& oth) {
            return std::string_view{data(), size()} <= std::string_view{oth.data(), oth.size()};
        }

        bool operator == (const PersistentStr& oth) {
            return std::string_view{data(), size()} == std::string_view{oth.data(), oth.size()};
        }

        bool operator < (const std::string_view& oth) {
            return std::string_view{data(), size()} < oth;
        }

        bool operator > (const std::string_view& oth) {
            return std::string_view{data(), size()} > oth;
        }

        bool operator <= (const std::string_view& oth) {
            return std::string_view{data(), size()} <= oth;
        }

        bool operator >= (const std::string_view& oth) {
            return std::string_view{data(), size()} >= oth;
        }

        bool operator == (const std::string_view& oth) {
            return std::string_view{data(), size()} == oth;
        }
    };

private:
    static constexpr size_t page_sz_ = 8192;

    static constexpr size_t max_children = 4;
    static constexpr size_t min_children = 2;
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
        std::array<char, page_sz_> data;
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
    char* allocate(size_t sz, uint64_t align) {
        assert(sz + align <= page_sz_);
        while (true) {
            if (root_->free_pages == nullptr) {
                pmem::obj::transaction::run(
                    pool_,
                    [&] {
                        root_->free_pages = pmem::obj::make_persistent<Page>();
                    },
                    root_->lock);
            }
            auto page = root_->free_pages;
            if (page->used & (align - 1)) {
                page->used += align - (page->used & (align - 1));
            }
            if (page->used + sz <= page_sz_) {
                auto ptr = page->data.data() + page->used;
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
            size_t pos = 0;

            PersistentStr insert_key;
            pmem::obj::persistent_ptr<char> insert_value;

            KeyNode new_root;

            if (root->is_leaf) {
                while (pos < root->key_count && root->keys[pos] < key) {
                    ++pos;
                }

                // insert() == replace()
                if (pos < root->key_count && root->keys[pos] == key) {
                    auto result = allocate<KeyNode>();
                    *result = *root;
                    result->children[pos] = value.ptr_;
                    return InsertResult{ .left = result, .right = nullptr };
                }
                //

                insert_key = key;
                insert_value = value.ptr_;
                new_root = *root;
            } else {
                while (pos + 1 < root->key_count && root->keys[pos + 1] <= key) {
                    ++pos;
                }
                auto result = insert(root->child(pos).get(), key, value);

                if (result.right) {
                    new_root = *root;
                    new_root.keys[pos] = result.left->keys[0];
                    new_root.children[pos] = (char*)result.left;

                    insert_key = result.right->keys[0];
                    insert_value = (char*)result.right;
                    ++pos;
                } else {
                    auto ret = allocate<KeyNode>();
                    *ret = *root;
                    ret->keys[pos] = result.left->keys[0];
                    ret->children[pos] = (char*)result.left;
                    assert_node(ret);
                    return { ret };
                }
            }

            if (new_root.key_count == max_children) {
                InsertResult result;
                result.left = allocate<KeyNode>();
                result.right = allocate<KeyNode>();
                result.left->key_count = result.right->key_count = 0;
                result.left->is_leaf = result.right->is_leaf = root->is_leaf;

                auto add_key = [&, index=0] (PersistentStr key, pmem::obj::persistent_ptr<char> value) mutable {
                    size_t mid = (max_children+1)/2;
                    auto ptr = result.left;
                    if (index >= mid) {
                        ptr = result.right;
                    }
                    ptr->keys[ptr->key_count] = key;
                    ptr->children[ptr->key_count] = value;
                    ++ptr->key_count;
                    assert_node(ptr);
                    ++index;
                };

                for (size_t i = 0; i < pos; ++i)
                    add_key(new_root.keys[i], new_root.children[i]);
                add_key(insert_key, insert_value);
                for (size_t i = pos; i < max_children; ++i)
                    add_key(new_root.keys[i], new_root.children[i]);
                assert_node(result.left);
                assert_node(result.right);
                return result;
            } else {
                ++new_root.key_count;
                for (ssize_t i = new_root.key_count - 1; i > pos; --i) {
                    new_root.keys[i] = new_root.keys[i - 1];
                    new_root.children[i] = new_root.children[i - 1];
                }
                new_root.keys[pos] = insert_key;
                new_root.children[pos] = insert_value;
                auto result = allocate<KeyNode>();
                *result = new_root;
                assert_node(result);
                return { result };
            }
        }
    }

    std::optional<KeyNode> erase(KeyNode* root, std::string_view key) {
        if (root == nullptr) {
            return std::nullopt;
        }
        size_t pos = 0;
        if (root->is_leaf) {
            while (pos < root->key_count && root->keys[pos] < key) {
                ++pos;
            }
            assert(root->keys[pos] == key);
            KeyNode result = *root;
            for (size_t i = pos; i < root->key_count; ++i) {
                result.children[i] = result.children[i + 1];
                result.keys[i] = result.keys[i + 1];
            }
            --result.key_count;
            if (result.key_count == 0) {
                return std::nullopt;
            } else {
                assert_node(&result);
                return result;
            }
        } else {
            while (pos + 1 < root->key_count && root->keys[pos + 1] <= key) {
                ++pos;
            }
            auto subnode = erase((KeyNode*)root->children[pos].get(), key);
            //if (root->key_count == 1) {
            //    return subnode;
            //}
            assert(subnode);
            KeyNode node = *root;
            if (subnode->key_count < min_children) {
                int di = pos == 0 && pos + 1 < subnode->key_count ? 1 : -1;
                KeyNode* nodes[2];
                if (pos == 0 || pos + 1 < subnode->key_count) {
                    nodes[0] = &*subnode;
                    nodes[1] = node.child(pos + 1).get();
                } else {
                    nodes[0] = node.child(pos - 1).get();
                    nodes[1] = &*subnode;
                    --pos;
                }
                assert_node(nodes[0]);
                assert_node(nodes[1]);

                size_t secondary_count = nodes[0]->key_count + nodes[1]->key_count;
                size_t cut = secondary_count;

                node.children[pos] = (char*)allocate<KeyNode>();
                node.child(pos)->key_count = 0;
                node.child(pos)->is_leaf = subnode->is_leaf;

                if (secondary_count >= 2 * min_children) {
                    node.children[pos + 1] = (char*)allocate<KeyNode>();
                    node.child(pos + 1)->key_count = 0;
                    node.child(pos + 1)->is_leaf = subnode->is_leaf;
                    cut = min_children;
                } else {
                    node.children[pos + 1] = nullptr;
                    cut = secondary_count;
                }

                auto add_kv = [&](PersistentStr key, pmem::obj::persistent_ptr<char> value) {
                    auto ptr = node.child(pos).get();
                    if (ptr->key_count >= cut) {
                        ptr = node.child(pos + 1).get();
                    }
                    ptr->keys[ptr->key_count] = key;
                    ptr->children[ptr->key_count] = value;
                    ++ptr->key_count;
                    assert(ptr->key_count <= max_children);
                    assert_node(ptr);
                };

                for (size_t i = 0; i < 2; ++i) {
                    auto ptr = nodes[i];
                    for (size_t j = 0; j < ptr->key_count; ++j) {
                        add_kv(ptr->keys[j], ptr->child(j).raw());
                    }
                    if (node.child(pos + i)) {
                        node.keys[pos + i] = node.child(pos + i)->keys[0];
                    }
                }

                if (!node.child(pos + 1)) {
                    for (ssize_t i = pos + 1; i < node.key_count; ++i) {
                        node.children[i] = node.children[i + 1];
                        node.keys[i] = node.keys[i + 1];
                    }
                    --node.key_count;
                }

                assert_node(&node);
                return node;
            } else {
                auto child = allocate<KeyNode>();
                *child = *subnode;
                node.children[pos] = (char*)child;
                node.keys[pos] = subnode->keys[0];
                assert_node(&node);
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
            volatile_root_,
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
        if (pmem::obj::pool<Root>::check(fname, layout_) == 1) {
            pool_ = pmem::obj::pool<Root>::open(fname, layout_);
        } else {
            pool_ = pmem::obj::pool<Root>::create(fname, layout_, pool_size, mode);
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

    void erase(std::string_view key) {
        if (lookup(key)) {
            auto root = erase(volatile_root_, key);
            if (root) {
                if (root->is_leaf || root->key_count > 1) {
                    volatile_root_ = allocate<KeyNode>();
                    *volatile_root_ = *root;
                } else {
                    volatile_root_ = root->child(0).get();
                }
            } else {
                volatile_root_ = nullptr;
            }
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
            while (visited_pos < visited.size() && visited[visited_pos] < page) {
                ++visited_pos;
            }
            if (visited_pos < visited.size() && visited[visited_pos].raw().off - page.raw().off < sizeof(Page)) {
                page_set.insert(page);
            }
        }
        pmem::obj::transaction::run(
            pool_,
            [&] {
                for (auto page = stale_gc_root; page && page->next; page = page->next) {
                    if (page_set.find(page->next) == page_set.end()) {
                        auto free = page->next;
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
