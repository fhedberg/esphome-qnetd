// ESPHome glue for the qnetd core: TCP listener on the esphome socket
// abstraction (same pattern as the native API server), loop() pump,
// entity publishing. All protocol logic lives in server.{h,cpp}.
#pragma once
#include "esphome/core/component.h"
#include "esphome/core/defines.h"
#include "esphome/components/socket/socket.h"
#ifdef USE_SENSOR
#include "esphome/components/sensor/sensor.h"
#endif
#ifdef USE_BINARY_SENSOR
#include "esphome/components/binary_sensor/binary_sensor.h"
#endif
#ifdef USE_TEXT_SENSOR
#include "esphome/components/text_sensor/text_sensor.h"
#endif
#include "server.h"

namespace esphome {
namespace qnetd {

class QnetdComponent : public Component, public qnetd_core::Transport {
 public:
  void setup() override;
  void loop() override;
  void dump_config() override;
  float get_setup_priority() const override { return setup_priority::AFTER_WIFI; }

  void set_port(uint16_t port) { port_ = port; }
#ifdef USE_SENSOR
  void set_connected_sensor(sensor::Sensor *s) { connected_sensor_ = s; }
  void set_decisions_sensor(sensor::Sensor *s) { decisions_sensor_ = s; }
#endif
#ifdef USE_BINARY_SENSOR
  void set_vote_granted_sensor(binary_sensor::BinarySensor *s) { vote_granted_sensor_ = s; }
#endif
#ifdef USE_TEXT_SENSOR
  void set_status_sensor(text_sensor::TextSensor *s) { status_sensor_ = s; }
#endif

  // qnetd_core::Transport
  void send_frame(int slot, const uint8_t *data, size_t len) override;
  void close_connection(int slot) override;

 protected:
  struct Conn {
    std::unique_ptr<socket::Socket> sock;
    std::vector<uint8_t> tx;  // unsent remainder after a partial write
    bool used = false;
  };
  uint64_t now_ms_();
  void publish_();

  uint16_t port_{5403};
  std::unique_ptr<socket::Socket> listen_;
  Conn conns_[qnetd_core::MAX_CLIENTS];
  std::unique_ptr<qnetd_core::Server> server_;
  bool dirty_{false};
  uint32_t last_millis_{0};
  uint64_t millis_high_{0};
#ifdef USE_SENSOR
  sensor::Sensor *connected_sensor_{nullptr};
  sensor::Sensor *decisions_sensor_{nullptr};
#endif
#ifdef USE_BINARY_SENSOR
  binary_sensor::BinarySensor *vote_granted_sensor_{nullptr};
#endif
#ifdef USE_TEXT_SENSOR
  text_sensor::TextSensor *status_sensor_{nullptr};
#endif
};

}  // namespace qnetd
}  // namespace esphome
