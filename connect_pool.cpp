#include "connect_pool.h"

#include <netinet/tcp.h>
#include <unistd.h>

namespace bus {

SocketHolder::~SocketHolder() {
    if (sock_ < 0) {
        return;
    }
    struct linger sl;
    sl.l_onoff = 1;
    sl.l_linger = 0;
    setsockopt(sock_, SOL_SOCKET, SO_LINGER, &sl, sizeof(sl));
    close(sock_);
}

}
