#include "error.h"

namespace bus {

void throw_errno() {
    throw BusError(strerror(errno));
}

}
