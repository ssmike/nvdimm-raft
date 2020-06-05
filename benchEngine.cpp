#include "engine.h"
#include <libpmemkv.hpp>

#include <chrono>

#include <unistd.h>

int main(int argc, char** argv) {
    constexpr size_t size = 1000 * 1024 * 1024;
    assert(argc == 2);
    unlink(argv[1]);
    Engine engine(argv[1], size);

    srand(2);
    std::vector<std::string> keys;
    std::vector<std::string> values;
    auto pt = std::chrono::steady_clock::now();
    constexpr size_t inserts = 50000;
    auto mcs = [&] {
        return std::chrono::duration_cast<std::chrono::nanoseconds>((std::chrono::steady_clock::now() - pt)/inserts).count();
    };

    for (size_t i = 0; i < inserts; ++i) {
        keys.push_back(std::to_string(rand()));
        values.push_back(std::to_string(i));
        engine.insert(engine.copy_str(keys.back()), engine.copy_str(values.back()));
    }
    std::cout << "engine inserts " << mcs() << std::endl;

    pt = std::chrono::steady_clock::now();
    while (!keys.empty()) {
        engine.unsafe_erase(keys.back());
        keys.pop_back();
    }
    std::cout << "engine erases " << mcs() << std::endl;

    for (auto name : {"cmap", "stree", "csmap", "tree3"}) {
        pmem::kv::config cfg;
        cfg.put_int64("size", size);
        cfg.put_string("path", std::string(argv[1]) + name);

        pmem::kv::db engine;
        engine.open(name, std::move(cfg));

        for (size_t i = 0; i < inserts; ++i) {
            keys.push_back(std::to_string(rand()));
            values.push_back(std::to_string(i));
            engine.put(keys.back(), values.back());
        }
        std::cout << name << " inserts " << mcs() << std::endl;

        pt = std::chrono::steady_clock::now();
        while (!keys.empty()) {
            engine.remove(keys.back());
            keys.pop_back();
        }
        std::cout << name << " erases " << mcs() << std::endl;
    }
}
