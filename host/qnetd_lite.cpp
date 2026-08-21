// POSIX wrapper around the qnetd core: a select()-driven single-threaded
// server. Exists to interop-test the core against real corosync-qdevice
// clients (see integration/) without flashing an ESP.
#include <arpa/inet.h>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>
#include "server.h"

using namespace qnetd_core;

static uint64_t now_ms() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return uint64_t(ts.tv_sec) * 1000 + ts.tv_nsec / 1000000;
}

struct PosixTransport : Transport {
  int fds[MAX_CLIENTS];
  PosixTransport() {
    for (int &f : fds)
      f = -1;
  }
  void send_frame(int slot, const uint8_t *data, size_t len) override {
    if (fds[slot] < 0)
      return;
    size_t off = 0;
    while (off < len) {  // blocking-ish write; frames are tiny
      ssize_t n = ::send(fds[slot], data + off, len - off, 0);
      if (n <= 0) {
        if (errno == EINTR)
          continue;
        return;  // read path will detect the dead socket
      }
      off += size_t(n);
    }
  }
  void close_connection(int slot) override {
    if (fds[slot] >= 0)
      ::close(fds[slot]);
    fds[slot] = -1;
  }
};

int main(int argc, char **argv) {
  setvbuf(stdout, nullptr, _IOLBF, 0);  // container logs are pipes
  uint16_t port = argc > 1 ? uint16_t(atoi(argv[1])) : 5403;
  PosixTransport tp;
  Server server(&tp, [](LogLevel lvl, const char *msg) {
    const char *l[] = {"debug", "info", "warn", "error"};
    printf("[%s] %s\n", l[int(lvl)], msg);
    fflush(stdout);
  });

  int ls = socket(AF_INET6, SOCK_STREAM, 0);
  int off = 0, on = 1;
  setsockopt(ls, IPPROTO_IPV6, IPV6_V6ONLY, &off, sizeof(off));
  setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
  struct sockaddr_in6 addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin6_family = AF_INET6;
  addr.sin6_addr = in6addr_any;
  addr.sin6_port = htons(port);
  if (bind(ls, (struct sockaddr *)&addr, sizeof(addr)) != 0 || listen(ls, 8) != 0) {
    perror("bind/listen");
    return 1;
  }
  printf("[info] qnetd_lite listening on :%u\n", port);

  while (true) {
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(ls, &rfds);
    int maxfd = ls;
    for (int i = 0; i < MAX_CLIENTS; i++) {
      if (tp.fds[i] >= 0) {
        FD_SET(tp.fds[i], &rfds);
        if (tp.fds[i] > maxfd)
          maxfd = tp.fds[i];
      }
    }
    struct timeval tv = {0, 200000};  // 200 ms tick for DPD
    int r = select(maxfd + 1, &rfds, nullptr, nullptr, &tv);
    if (r < 0 && errno != EINTR) {
      perror("select");
      return 1;
    }
    uint64_t now = now_ms();
    if (r > 0 && FD_ISSET(ls, &rfds)) {
      struct sockaddr_storage sa;
      socklen_t sl = sizeof(sa);
      int fd = accept(ls, (struct sockaddr *)&sa, &sl);
      if (fd >= 0) {
        char host[64] = "?";
        if (sa.ss_family == AF_INET6)
          inet_ntop(AF_INET6, &((struct sockaddr_in6 *)&sa)->sin6_addr, host, sizeof(host));
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
        int slot = server.on_connect(now, host);
        if (slot < 0)
          ::close(fd);
        else
          tp.fds[slot] = fd;
      }
    }
    for (int i = 0; i < MAX_CLIENTS; i++) {
      if (tp.fds[i] < 0 || !FD_ISSET(tp.fds[i], &rfds))
        continue;
      uint8_t buf[2048];
      ssize_t n = recv(tp.fds[i], buf, sizeof(buf), 0);
      if (n > 0) {
        server.on_data(i, buf, size_t(n), now);
      } else if (n == 0 || (errno != EWOULDBLOCK && errno != EAGAIN)) {
        tp.close_connection(i);
        server.on_disconnected(i, now);
      }
    }
    server.tick(now);
  }
}
