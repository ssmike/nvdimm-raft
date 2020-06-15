#include "engine2.h"
#include <libpmemkv.hpp>

#include <chrono>

#include <unistd.h>

#define ensureok(condition) if (auto status = (condition); status != pmem::kv::status::OK) { throw std::logic_error("condition not met " #condition); }

int main(int argc, char** argv) {
    constexpr size_t size = 1000 * 1024 * 1024;
    assert(argc == 2);
    unlink(argv[1]);
    Engine engine(argv[1], size);

    srand(2);
    std::vector<std::string> keys;
    std::vector<std::string> values;
    constexpr size_t inserts = 2000;
    for (size_t i = 0; i < inserts; ++i) {
        keys.push_back(std::to_string(rand()));
        values.push_back(std::to_string(i));
    }

    auto pt = std::chrono::steady_clock::now();
    auto mcs = [&] {
        return std::chrono::duration_cast<std::chrono::nanoseconds>((std::chrono::steady_clock::now() - pt)/inserts).count();
    };

   pt = std::chrono::steady_clock::now();
   for (size_t i = 0; i < inserts; ++i) {
       engine.insert(engine.copy_str(keys[i]), engine.copy_str(values[i]));
   }
   std::cout << "engine inserts " << mcs() << std::endl;

   pt = std::chrono::steady_clock::now();
   for (size_t i = 0; i < inserts; ++i) {
       if (auto res = engine.lookup(keys[i]); !res || *res != values[i]) {
           return 3;
       }
   }
   std::cout << "engine lookups " << mcs() << std::endl;

   pt = std::chrono::steady_clock::now();
   for (size_t i = 0; i < keys.size(); ++i) {
       engine.unsafe_erase(keys[i]);
   }
   std::cout << "engine erases " << mcs() << std::endl;

    for (auto name : {"cmap", "stree", "csmap", "tree3"}) {
        pmem::kv::config cfg;
        cfg.put_int64("size", size);
        auto path = std::string(argv[1]) + name;
        ensureok(cfg.put_string("path", path));
        ensureok(cfg.put_uint64("SIZE", 2 * 1024UL * 1024UL * 1024UL));
        ensureok(cfg.put_uint64("force_create", 1));
        system(("rm -r " + path).c_str());

        pmem::kv::db db;
        ensureok(db.open(name, std::move(cfg)));

        pt = std::chrono::steady_clock::now();
        for (size_t i = 0; i < inserts; ++i) {
            db.put(keys[i], values[i]);
        }
        std::cout << name << " inserts " << mcs() << std::endl;

        pt = std::chrono::steady_clock::now();
        for (size_t i = 0; i < inserts; ++i) {
            db.remove(keys[i]);
        }
        std::cout << name << " erases " << mcs() << std::endl;
    }
}
