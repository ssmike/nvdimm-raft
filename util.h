#pragma once

#include <condition_variable>
#include <mutex>
#include <atomic>
#include <optional>
#include <functional>

namespace bus::internal {

constexpr size_t header_len = 8;

void write_header(size_t size, char* buf);

size_t read_header(char* buf);

}
