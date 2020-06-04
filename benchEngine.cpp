#include "engine.h"
#include <chrono>

#include <unistd.h>

int main(int argc, char** argv) {
    assert(argc == 2);
    unlink(argv[1]);
    Engine engine(argv[1], 200 * 1024 * 1024);

    srand(2);
    std::vector<std::string> keys;
    std::vector<std::string> values;
    auto pt = std::chrono::steady_clock::now();
    constexpr size_t inserts = 50000;
    for (size_t i = 0; i < inserts; ++i) {
        keys.push_back(std::to_string(rand()));
        values.push_back(std::to_string(i));
        engine.insert(engine.copy_str(keys.back()), engine.copy_str(values.back()));
    }
    std::cerr << std::chrono::duration_cast<std::chrono::nanoseconds>((std::chrono::steady_clock::now() - pt)/inserts).count() << std::endl;

    pt = std::chrono::steady_clock::now();
    while (!keys.empty()) {
        engine.unsafe_erase(keys.back());
        keys.pop_back();
    }
    std::cerr << std::chrono::duration_cast<std::chrono::nanoseconds>((std::chrono::steady_clock::now() - pt)/inserts).count() << std::endl;
}
