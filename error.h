#pragma once

#include <string>
#include <string.h>

namespace bus {

class BusError: public std::exception {
public:
    BusError(const char*);

    const char* what() const noexcept override {
        return message_;
    }

private:
    const char* message_;
};

void throw_errno() {
    throw BusError(strerror(errno));
}

#define CHECK_ERRNO(x) if (!(x)) throw_errno();

}
