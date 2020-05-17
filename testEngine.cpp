#include "engine.h"

#include <unistd.h>

int main(int argc, char** argv) {
    assert(argc == 2);
    unlink(argv[1]);
    Engine engine(argv[1], 200 * 1024 * 1024);

    srand(2);
    std::vector<std::string> keys;
    std::vector<std::string> values;
    for (size_t i = 0; i < 2000; ++i) {
        keys.push_back(std::to_string(rand()));
        values.push_back(std::to_string(i));
        engine.insert(engine.copy_str(keys.back()), engine.copy_str(values.back()));
        engine.commit();
        engine.gc();

        for (size_t j = 0; j < keys.size(); ++j) {
            assert(engine.lookup(keys[j]));
            assert(*engine.lookup(keys[j]) == values[j]);
        }
    }
    
    while (!keys.empty()) {
        engine.erase(keys.back());
        engine.commit();
        engine.gc();
        assert(!engine.lookup(keys.back()));
        keys.pop_back();
        for (size_t j = 0; j < keys.size(); ++j) {
            assert(engine.lookup(keys[j]));
            assert(*engine.lookup(keys[j]) == values[j]);
        }
    }
}
