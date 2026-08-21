// qnetd server core: client sessions, message dispatch, dead-peer detection
// and the ffsplit decision algorithm. Portable: no sockets, no clock; the
// embedder drives on_connect/on_data/on_disconnected/tick and provides a
// Transport. Ported from corosync-qdevice qdevices/qnetd-*.c (BSD).
#pragma once
#include <array>
#include <functional>
#include <string>
#include "msg.h"

namespace qnetd_core {

constexpr int MAX_CLIENTS = 8;
constexpr int MAX_CLUSTERS = 2;
constexpr uint32_t HEARTBEAT_MIN_MS = 1000;       // upstream QNETD defaults
constexpr uint32_t HEARTBEAT_MAX_MS = 2 * 60 * 1000;
constexpr double DPD_COEFFICIENT = 1.5;           // upstream default
constexpr uint32_t PREACTIVE_TIMEOUT_MS = 30000;  // ours: cap a stuck handshake

struct Transport {
  virtual ~Transport() = default;
  virtual void send_frame(int slot, const uint8_t *data, size_t len) = 0;
  // Close the connection. Transport must NOT call back into the server from
  // inside this call; the server has already cleaned the slot up.
  virtual void close_connection(int slot) = 0;
};

enum class LogLevel : uint8_t { DEBUG = 0, INFO = 1, WARN = 2, ERROR = 3 };
using LogFn = std::function<void(LogLevel, const char *msg)>;

class Server {
 public:
  Server(Transport *transport, LogFn log, uint32_t max_rx_frame = 32768)
      : transport_(transport), log_(std::move(log)), max_rx_frame_(max_rx_frame) {}

  // Returns a slot id >= 0, or -1 when full (caller should close the socket).
  int on_connect(uint64_t now_ms, const std::string &peer);
  // Feed received bytes. May emit frames and request closes via Transport.
  void on_data(int slot, const uint8_t *data, size_t len, uint64_t now_ms);
  // Transport noticed the connection is gone (EOF/reset).
  void on_disconnected(int slot, uint64_t now_ms);
  // Drive timers (DPD). Call at least a few times per second.
  void tick(uint64_t now_ms);

  // --- introspection (for sensors / status) ---
  int connected_clients() const;
  bool any_ack() const;
  std::string status_string() const;
  uint32_t decisions() const { return decisions_; }
  // Invoked after anything observable changed (connections, votes).
  std::function<void()> on_state_change;

 private:
  enum class Phase : uint8_t { FREE, WAIT_PREINIT, WAIT_INIT, ACTIVE };
  enum class FfClientState : uint8_t { WAITING_FOR_CHANGE, SENDING_NACK, SENDING_ACK };
  enum class FfClusterState : uint8_t {
    WAITING_FOR_CHANGE,
    WAITING_FOR_STABLE_MEMBERSHIP,
    SENDING_NACKS,
    SENDING_ACKS,
  };

  struct Cluster {
    bool used = false;
    std::string name;
    int members = 0;
    FfClusterState state = FfClusterState::WAITING_FOR_CHANGE;
    NodeList quorate_partition;
  };

  struct Session {
    Phase phase = Phase::FREE;
    std::string peer;
    std::string cluster_name;
    int cluster = -1;  // index into clusters_, valid in ACTIVE
    uint32_t node_id = 0;
    RingId last_ring_id;
    uint32_t heartbeat_ms = 0;
    TieBreaker tie_breaker;
    Algorithm algorithm = Algorithm::FFSPLIT;
    bool keep_active_partition_tb = true;  // upstream default enabled
    NodeList config_list;
    bool config_version_set = false;
    uint64_t config_version = 0;
    NodeList membership_list;
    NodeList quorum_list;
    Heuristics last_heuristics = Heuristics::UNDEFINED;
    Vote last_sent_vote = Vote::UNDEFINED;
    Vote last_ack_nack = Vote::UNDEFINED;
    FfClientState ff_state = FfClientState::WAITING_FOR_CHANGE;
    uint32_t vote_info_seq = 0;
    uint64_t deadline_ms = 0;
    bool schedule_disconnect = false;
    std::vector<uint8_t> rx;

    void reset() { *this = Session(); }
  };

  // --- plumbing ---
  void send(int slot, const Frame &f) {
    transport_->send_frame(slot, f.data(), f.size());
  }
  void send_err(int slot, const MsgDecoded &m, ReplyErrorCode code) {
    send(slot, build_server_error(m.seq_number_set, m.seq_number, code));
  }
  void logf(LogLevel lvl, const char *fmt, ...);
  void drain_closes(uint64_t now_ms);
  void disconnect_client(int slot, uint64_t now_ms, const char *why);
  void reschedule_dpd(Session &s, uint64_t now_ms);
  void notify();

  // --- message handlers ---
  void handle_frame(int slot, MsgType type, const uint8_t *frame, size_t frame_len,
                    const uint8_t *payload, size_t payload_len, uint64_t now_ms);
  void handle_preinit(int slot, const MsgDecoded &m);
  void handle_init(int slot, const MsgDecoded &m, uint64_t now_ms);
  void handle_set_option(int slot, const MsgDecoded &m, uint64_t now_ms);
  void handle_node_list(int slot, const MsgDecoded &m);
  void handle_vote_info_reply(int slot, const MsgDecoded &m);
  void handle_heuristics_change(int slot, const MsgDecoded &m);

  // --- cluster helpers ---
  int find_or_create_cluster(const std::string &name);
  void leave_cluster(int slot, uint64_t now_ms);
  Session *cluster_client_by_node_id(int cluster, uint32_t node_id, int except_slot = -1);

  // --- ffsplit (port of qnetd-algo-ffsplit.c) ---
  // View of the triggering client's *incoming* lists; peers use stored state.
  struct TriggerView {
    int slot;
    bool leaving;
    const RingId *ring_id;
    const NodeList *config;
    const NodeList *membership;
    Heuristics heuristics;
  };
  ReplyErrorCode ffsplit_do(const TriggerView &tv, Vote &result);
  bool ffsplit_is_stable(const TriggerView &tv) const;
  const NodeList *ffsplit_select_partition(const TriggerView &tv) const;
  bool ffsplit_partition_better(const Session *c1, const NodeList *cfg1, const NodeList *mem1,
                                Heuristics h1, const Session *c2, const NodeList *cfg2,
                                const NodeList *mem2, Heuristics h2,
                                const NodeList &prev_quorate, bool keep_active_tb) const;
  bool ffsplit_is_preferred_partition(const Session *c, const NodeList *cfg,
                                      const NodeList *mem) const;
  void ffsplit_partition_stats(const Session *c, const NodeList *mem, Heuristics h,
                               size_t &clients, size_t &pass, size_t &fail) const;
  void ffsplit_update_states(const TriggerView &tv, const NodeList *winner);
  size_t ffsplit_send_votes(const TriggerView &tv, bool send_acks);
  size_t ffsplit_count_state(int cluster, FfClientState st) const;
  // effective per-client view (trigger override)
  const NodeList *eff_config(const Session &s, const TriggerView &tv) const;
  const NodeList *eff_membership(const Session &s, const TriggerView &tv) const;
  const RingId *eff_ring(const Session &s, const TriggerView &tv) const;
  Heuristics eff_heuristics(const Session &s, const TriggerView &tv) const;
  bool skip_in_cluster_walk(const Session &s, const TriggerView &tv) const {
    return tv.leaving && s.node_id == sessions_[tv.slot].node_id;
  }

  Transport *transport_;
  LogFn log_;
  uint32_t max_rx_frame_;
  std::array<Session, MAX_CLIENTS> sessions_;
  std::array<Cluster, MAX_CLUSTERS> clusters_;
  std::vector<int> pending_close_;
  bool processing_ = false;
  uint32_t decisions_ = 0;
};

}  // namespace qnetd_core
