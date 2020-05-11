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
private:
    static constexpr size_t page_sz_ = 8192;

    struct PersistentStr {
        pmem::obj::persistent_ptr<char> ptr_;

        char* data() {
            return ptr_.get() + sizeof(uint64_t);
        }

        size_t size() {
            return *reinterpret_cast<uint64_t*>(ptr_.get());
        }

        static size_t size(size_t chars) {
            return sizeof(uint64_t) + chars;
        }

        static size_t align() {
            return alignof(uint64_t);
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
    };

    struct DirtyPage {
        Page* page = nullptr;
        uint64_t start = 0;
    };

private:
    pmem::obj::persistent_ptr<char> allocate(size_t sz, uint64_t align) {
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
    pmem::obj::persistent_ptr<T> allocate() {
        return allocate(sizeof(T), alignof(T));
    }

    PersistentStr allocate_str(size_t sz) {
        return { allocate(PersistentStr::size(sz), PersistentStr::align()) };
    }

    void flush() {
        for (auto [page, start] : dirty_pages_) {
            pool_.flush(page->used);
            pool_.flush(&page->data[start], page->used - start);
        }
        dirty_pages_.clear();
    }

public:
    Engine(std::string fname) {
        if (pmem::obj::pool<Root>::check(fname, layout_) == 1) {
            pool_ = pmem::obj::pool<Root>::open(fname, layout_);
        } else {
            pool_ = pmem::obj::pool<Root>::create(fname, layout_);
        }
        root_ = pool_.root();
    }


private:
    pmem::obj::pool<Root> pool_;
    pmem::obj::persistent_ptr<Root> root_;

    const std::string layout_ = "kv_engine";
    std::vector<DirtyPage> dirty_pages_;
};
