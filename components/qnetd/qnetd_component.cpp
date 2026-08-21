#include "qnetd_component.h"
#include "esphome/core/log.h"
#include "esphome/core/hal.h"
#include <cerrno>

namespace esphome {
namespace qnetd {

static const char *const TAG = "qnetd";

uint64_t QnetdComponent::now_ms_() {
  // extend esphome's 32-bit millis() to 64 bits (DPD deadlines must survive
  // the 49.7-day wrap)
  uint32_t now = millis();
  if (now < last_millis_)
    millis_high_ += 0x100000000ULL;
  last_millis_ = now;
  return millis_high_ | now;
}

void QnetdComponent::setup() {
  server_ = std::unique_ptr<qnetd_core::Server>(new qnetd_core::Server(
      this,
      [](qnetd_core::LogLevel lvl, const char *msg) {
        switch (lvl) {
          case qnetd_core::LogLevel::DEBUG:
            ESP_LOGD(TAG, "%s", msg);
            break;
          case qnetd_core::LogLevel::INFO:
            ESP_LOGI(TAG, "%s", msg);
            break;
          case qnetd_core::LogLevel::WARN:
            ESP_LOGW(TAG, "%s", msg);
            break;
          case qnetd_core::LogLevel::ERROR:
            ESP_LOGE(TAG, "%s", msg);
            break;
        }
      }));
  server_->on_state_change = [this]() { this->dirty_ = true; };

  listen_ = socket::socket_ip_loop_monitored(SOCK_STREAM, 0);
  if (listen_ == nullptr) {
    ESP_LOGE(TAG, "could not create listening socket");
    this->mark_failed();
    return;
  }
  int enable = 1;
  listen_->setsockopt(SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int));
  listen_->setblocking(false);
  struct sockaddr_storage server_addr;
  socklen_t sl = socket::set_sockaddr_any(reinterpret_cast<struct sockaddr *>(&server_addr),
                                          sizeof(server_addr), port_);
  if (sl == 0 || listen_->bind(reinterpret_cast<struct sockaddr *>(&server_addr), sl) != 0 ||
      listen_->listen(4) != 0) {
    ESP_LOGE(TAG, "bind/listen on port %u failed: errno %d", port_, errno);
    this->mark_failed();
    return;
  }
  ESP_LOGCONFIG(TAG, "listening on TCP %u", port_);
}

void QnetdComponent::send_frame(int slot, const uint8_t *data, size_t len) {
  Conn &c = conns_[slot];
  if (!c.used || c.sock == nullptr)
    return;
  if (c.tx.empty()) {
    ssize_t n = c.sock->write(data, len);
    if (n < 0) {
      if (errno != EWOULDBLOCK && errno != EAGAIN)
        return;  // read path will notice the broken socket
      n = 0;
    }
    if (size_t(n) < len)
      c.tx.insert(c.tx.end(), data + n, data + len);
  } else {
    c.tx.insert(c.tx.end(), data, data + len);
  }
}

void QnetdComponent::close_connection(int slot) {
  Conn &c = conns_[slot];
  if (c.sock != nullptr)
    c.sock->close();
  c.sock = nullptr;
  c.tx.clear();
  c.used = false;
}

void QnetdComponent::loop() {
  if (this->is_failed())
    return;
  uint64_t now = now_ms_();

  // accept
  while (true) {
    struct sockaddr_storage source_addr;
    socklen_t addr_len = sizeof(source_addr);
    auto sock = listen_->accept_loop_monitored(
        reinterpret_cast<struct sockaddr *>(&source_addr), &addr_len);
    if (sock == nullptr)
      break;
    sock->setblocking(false);
    int nodelay = 1;
    sock->setsockopt(IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(int));
    // format the peer from the sockaddr accept() already gave us, instead of
    // the Socket peername helpers whose API has changed across esphome
    // releases (std::string getpeername() -> getpeername_to() -> removed)
    char peername[46] = "?";  // INET6_ADDRSTRLEN
    if (source_addr.ss_family == AF_INET) {
      inet_ntop(AF_INET, &reinterpret_cast<struct sockaddr_in *>(&source_addr)->sin_addr,
                peername, sizeof(peername));
    }
#if USE_NETWORK_IPV6
    else if (source_addr.ss_family == AF_INET6) {
      inet_ntop(AF_INET6, &reinterpret_cast<struct sockaddr_in6 *>(&source_addr)->sin6_addr,
                peername, sizeof(peername));
    }
#endif
    int slot = server_->on_connect(now, peername);
    if (slot < 0) {
      sock->close();
      continue;
    }
    conns_[slot].sock = std::move(sock);
    conns_[slot].tx.clear();
    conns_[slot].used = true;
  }

  // per-connection I/O
  for (int i = 0; i < qnetd_core::MAX_CLIENTS; i++) {
    Conn &c = conns_[i];
    if (!c.used || c.sock == nullptr)
      continue;
    // flush queued tx
    if (!c.tx.empty()) {
      ssize_t n = c.sock->write(c.tx.data(), c.tx.size());
      if (n > 0)
        c.tx.erase(c.tx.begin(), c.tx.begin() + n);
    }
    // read
    uint8_t buf[512];
    while (c.used && c.sock != nullptr) {
      ssize_t n = c.sock->read(buf, sizeof(buf));
      if (n > 0) {
        server_->on_data(i, buf, size_t(n), now);
      } else if (n == 0) {
        // orderly shutdown by peer
        close_connection(i);
        server_->on_disconnected(i, now);
        break;
      } else {
        if (errno == EWOULDBLOCK || errno == EAGAIN)
          break;
        close_connection(i);
        server_->on_disconnected(i, now);
        break;
      }
    }
  }

  server_->tick(now);

  if (dirty_) {
    dirty_ = false;
    publish_();
  }
}

void QnetdComponent::publish_() {
#ifdef USE_SENSOR
  if (connected_sensor_ != nullptr)
    connected_sensor_->publish_state(server_->connected_clients());
  if (decisions_sensor_ != nullptr)
    decisions_sensor_->publish_state(server_->decisions());
#endif
#ifdef USE_BINARY_SENSOR
  if (vote_granted_sensor_ != nullptr)
    vote_granted_sensor_->publish_state(server_->any_ack());
#endif
#ifdef USE_TEXT_SENSOR
  if (status_sensor_ != nullptr) {
    std::string st = server_->status_string();
    if (!status_sensor_->has_state() || status_sensor_->state != st)
      status_sensor_->publish_state(st);
  }
#endif
}

void QnetdComponent::dump_config() {
  ESP_LOGCONFIG(TAG, "qnetd arbiter:");
  ESP_LOGCONFIG(TAG, "  port: %u", port_);
  ESP_LOGCONFIG(TAG, "  algorithm: ffsplit; TLS: unsupported (plaintext)");
  ESP_LOGCONFIG(TAG, "  max clients: %d", qnetd_core::MAX_CLIENTS);
}

}  // namespace qnetd
}  // namespace esphome
