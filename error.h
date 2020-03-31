#pragma once

#include <string>
#include <string.h>

namespace bus {

class BusError: public std::exception {
public:
    BusError(std::string s) : message_(std::move(s)) {}

    const char* what() const noexcept override {
        return message_.c_str();
    }

private:
    std::string message_;
};

void throw_errno();

#define CHECK_ERRNO(x) if (!(x)) throw_errno();

}
