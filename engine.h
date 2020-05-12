#pragma once

#include <libpmemobj.h>

#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/container/array.hpp>
#include <libpmemobj++/container/string.hpp>
#include <libpmemobj++/container/vector.hpp>

#include <string>

class Engine {
public:
    struct PersistentStr {
        pmem::obj::persistent_ptr<char> ptr_;

        char* data() const {
            return ptr_.get() + sizeof(uint64_t);
        }

        size_t size() const {
            return *reinterpret_cast<uint64_t*>(ptr_.get());
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

    static constexpr size_t max_children = 3;
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

        pmem::obj::vector<pmem::obj::persistent_ptr<KeyNode>> stale_roots_;
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
                        root_->free_pages = pmem::obj::make_persistent<Page>(pool_);
                    });
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
                    });
            }
        }
    }

    template<typename T>
    T* allocate() {
        return (T*)allocate(sizeof(T), alignof(T));
    }

    void flush() {
        for (auto [page, start] : dirty_pages_) {
            pool_.flush(page->used);
            pool_.flush(&page->data[start], page->used - start);
        }
        dirty_pages_.clear();
        pool_.drain();
    }

    struct InsertResult {
        KeyNode* left = nullptr;
        KeyNode* right = nullptr;
    };

    InsertResult insert(KeyNode* root, PersistentStr key, PersistentStr value) {
        if (root == nullptr) {
            root = allocate<KeyNode>();
            root->key_count = 1;
            root->keys[0] = key;
            root->children[0] = value.ptr_;
            root->is_leaf = true;
            return { root };
        } else {
            size_t pos = 0;
            while (pos < root->key_count && key < root->keys[pos]) {
                ++pos;
            }

            PersistentStr insert_key;
            pmem::obj::persistent_ptr<char> insert_value;

            KeyNode new_root = *root;

            if (root->is_leaf) {
                insert_key = key;
                insert_value = value.ptr_;
            } else {
                auto result = insert(root->child(pos).get(), key, value);

                new_root.keys[pos] = result.left->keys[0];
                new_root.children[pos] = (char*)result.left;

                insert_key = result.right->keys[0];
                insert_value = (char*)result.right;
            }

            if (new_root.key_count == max_children) {
                InsertResult result;
                result.left = allocate<KeyNode>();
                result.right = allocate<KeyNode>();
                result.left->key_count = result.right->key_count = 0;
                result.left->is_leaf = result.right->is_leaf = true;

                auto add_key = [&, index=0] (PersistentStr key, pmem::obj::persistent_ptr<char> value) mutable {
                    size_t mid = (max_children+1)/2;
                    if (index < mid) {
                        result.left->keys[result.left->key_count] = key;
                        result.left->children[result.left->key_count] = value;
                        ++result.left->key_count;
                    } else {
                        result.right->keys[result.right->key_count] = key;
                        result.right->children[result.right->key_count] = value;
                        ++result.right->key_count;
                    }
                    ++index;
                };

                for (size_t i = 0; i < pos; ++i)
                    add_key(new_root.keys[i], root->children[i]);
                add_key(insert_key, insert_value);
                for (size_t i = pos; i < max_children; ++i)
                    add_key(new_root.keys[i], root->children[i]);
                return result;
            } else {
                ++new_root.key_count;
                for (ssize_t i = root->key_count - 1; i > pos; --i) {
                    new_root.keys[i] = new_root.keys[i - 1];
                    new_root.children[i] = new_root.children[i - 1];
                }
                new_root.keys[pos] = key;
                new_root.children[pos] = value.ptr_;
                auto result = allocate<KeyNode>();
                *result = new_root;
                return { result };
            }
        }
    }

    std::optional<KeyNode> erase(KeyNode* root, std::string_view key) {
        if (root == nullptr) {
            return std::nullopt;
        }
        size_t pos = 0;
        while (pos < root->key_count && root->keys[pos] > key) {
            ++pos;
        }
        if (root->is_leaf) {
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
                return result;
            }
        } else {
            auto subnode = erase((KeyNode*)root->children[pos].get(), key);
            assert(subnode);
            KeyNode node = *root;
            if (subnode->key_count < min_children) {
                int di = pos == 0 && pos + 1 < subnode->key_count ? 1 : -1;
                KeyNode* nodes[2];
                if (di < 0) {
                    nodes[0] = node.child(pos + di).get();
                    nodes[1] = &*subnode;
                    --pos;
                } else {
                    nodes[1] = node.child(pos + di).get();
                    nodes[0] = &*subnode;
                }
                node.children[pos] = (char*)allocate<KeyNode>();
                node.children[pos + 1] = (char*)allocate<KeyNode>();
                node.child(pos)->key_count = node.child(pos + 1)->key_count = 0;
                node.child(pos)->is_leaf = node.child(pos + 1)->is_leaf = subnode->is_leaf;
                auto add_kv = [&](PersistentStr key, pmem::obj::persistent_ptr<char> value) {
                    auto ptr = node.child(pos);
                    if (ptr->key_count >= min_children) {
                        ptr = node.child(pos + 1);
                    }
                    ptr->keys[ptr->key_count] = key;
                    ptr->children[ptr->key_count] = value;
                    ++ptr->key_count;
                };

                for (size_t i = 0; i < 2; ++i) {
                    auto ptr = node.child(pos + i);
                    for (size_t j = 0; j < ptr->key_count; ++j) {
                        add_kv(ptr->keys[j], ptr->child(j).raw());
                    }
                }

                return node;
            } else {
                node.children[pos] = (char*)allocate<KeyNode>();
                node.keys[pos] = subnode->keys[0];
                return node;
            }
        }
    }

    template<typename F>
    bool iterate(KeyNode* root, std::string_view start, std::string_view end, F&& f) {
        if (!root) return true;
        if (root->is_leaf) {
            for (size_t i = 0; i < root->key_count; ++i) {
                if (root->keys[i] >= start && root->keys[i] <= end) {
                    if (!f(root->keys[i], PersistentStr{root->children[i]})) {
                        return false;
                    }
                }
            }
        } else {
            for (size_t i = 0; i < root->key_count; ++i) {
                if (i + 1 < root->key_count && root->keys[i + 1] <= start) {
                    continue;
                }
                if (!(root->keys[i] <= end)) {
                    break;
                }
                if (!iterate((KeyNode*)root->children[i].get(), start, end, std::forward<F>(f))) {
                    return false;
                }
            }
        }
        return true;
    }

public:
    Engine(std::string fname) {
        if (pmem::obj::pool<Root>::check(fname, layout_) == 1) {
            pool_ = pmem::obj::pool<Root>::open(fname, layout_);
        } else {
            pool_ = pmem::obj::pool<Root>::create(fname, layout_);
        }
        root_ = pool_.root().get();
        volatile_root_ = root_->durable_root_.get();
    }

    PersistentStr allocate_str(size_t sz) {
        return { allocate(PersistentStr::size(sz), PersistentStr::align()) };
    }

    void insert(PersistentStr key, PersistentStr value) {
        auto result = insert(volatile_root_, key, value);
        if (result.right) {
            volatile_root_ = allocate<KeyNode>();
            volatile_root_->is_leaf = false;
            volatile_root_->key_count = 1;
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
        iterate(volatile_root_, start, end,
            [&] (const PersistentStr& key_, const PersistentStr& value_) {
                return f({key_.data(), key_.size()}, {value_.data(), value_.size()});
            });
    }

    std::optional<std::string_view> lookup(std::string_view key) {
        std::optional<std::string_view> result;
        iterate(key, key,
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

    void erase(std::string_view key) {
        if (lookup(key)) {
        }
    }

    void commit();

    void gc();

private:
    pmem::obj::pool<Root> pool_;
    Root* root_;

    const std::string layout_ = "kv_engine";
    std::vector<DirtyPage> dirty_pages_;

    KeyNode* volatile_root_ = nullptr;
};
