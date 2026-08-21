#include "server.h"
#include <cinttypes>
#include <cstdarg>
#include <cstdio>

namespace qnetd_core {

void Server::logf(LogLevel lvl, const char *fmt, ...) {
  if (!log_)
    return;
  char buf[192];
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(buf, sizeof(buf), fmt, ap);
  va_end(ap);
  log_(lvl, buf);
}

void Server::notify() {
  if (on_state_change)
    on_state_change();
}

// ---------------------------------------------------------------- lifecycle

int Server::on_connect(uint64_t now_ms, const std::string &peer) {
  for (int i = 0; i < MAX_CLIENTS; i++) {
    if (sessions_[i].phase == Phase::FREE) {
      sessions_[i].reset();
      sessions_[i].phase = Phase::WAIT_PREINIT;
      sessions_[i].peer = peer;
      sessions_[i].deadline_ms = now_ms + PREACTIVE_TIMEOUT_MS;
      logf(LogLevel::INFO, "client %s connected (slot %d)", peer.c_str(), i);
      notify();
      return i;
    }
  }
  logf(LogLevel::WARN, "client %s rejected: no free slots", peer.c_str());
  return -1;
}

void Server::on_disconnected(int slot, uint64_t now_ms) {
  if (slot < 0 || slot >= MAX_CLIENTS || sessions_[slot].phase == Phase::FREE)
    return;
  bool was_processing = processing_;
  processing_ = true;
  logf(LogLevel::INFO, "client %s (slot %d) disconnected", sessions_[slot].peer.c_str(), slot);
  leave_cluster(slot, now_ms);
  sessions_[slot].reset();
  processing_ = was_processing;
  if (!was_processing)
    drain_closes(now_ms);
  notify();
}

void Server::disconnect_client(int slot, uint64_t now_ms, const char *why) {
  Session &s = sessions_[slot];
  if (s.phase == Phase::FREE || s.schedule_disconnect)
    return;
  logf(LogLevel::WARN, "disconnecting client %s (slot %d): %s", s.peer.c_str(), slot, why);
  s.schedule_disconnect = true;
  pending_close_.push_back(slot);
  if (!processing_)
    drain_closes(now_ms);
}

void Server::drain_closes(uint64_t now_ms) {
  processing_ = true;
  while (!pending_close_.empty()) {
    int slot = pending_close_.back();
    pending_close_.pop_back();
    if (sessions_[slot].phase == Phase::FREE)
      continue;
    leave_cluster(slot, now_ms);
    sessions_[slot].reset();
    transport_->close_connection(slot);
  }
  processing_ = false;
  notify();
}

void Server::reschedule_dpd(Session &s, uint64_t now_ms) {
  if (s.phase == Phase::ACTIVE && s.heartbeat_ms > 0)
    s.deadline_ms = now_ms + uint64_t(DPD_COEFFICIENT * s.heartbeat_ms);
  else if (s.phase != Phase::FREE && s.phase != Phase::ACTIVE)
    s.deadline_ms = now_ms + PREACTIVE_TIMEOUT_MS;
}

void Server::tick(uint64_t now_ms) {
  for (int i = 0; i < MAX_CLIENTS; i++) {
    Session &s = sessions_[i];
    if (s.phase == Phase::FREE || s.deadline_ms == 0 || s.schedule_disconnect)
      continue;
    if (now_ms >= s.deadline_ms)
      disconnect_client(i, now_ms, "dead peer detection timeout");
  }
}

// ---------------------------------------------------------------- framing

void Server::on_data(int slot, const uint8_t *data, size_t len, uint64_t now_ms) {
  if (slot < 0 || slot >= MAX_CLIENTS || sessions_[slot].phase == Phase::FREE)
    return;
  Session &s = sessions_[slot];
  if (s.schedule_disconnect)
    return;
  s.rx.insert(s.rx.end(), data, data + len);
  reschedule_dpd(s, now_ms);

  processing_ = true;
  while (sessions_[slot].phase != Phase::FREE && !sessions_[slot].schedule_disconnect) {
    std::vector<uint8_t> &rx = sessions_[slot].rx;
    if (rx.size() < MSG_HEADER_LEN)
      break;
    uint16_t type = be_get16(rx.data());
    uint32_t plen = be_get32(rx.data() + 2);
    if (plen > max_rx_frame_) {
      MsgDecoded dummy;
      send_err(slot, dummy, ReplyErrorCode::MESSAGE_TOO_LONG);
      disconnect_client(slot, now_ms, "frame too long");
      break;
    }
    if (rx.size() < MSG_HEADER_LEN + plen)
      break;
    handle_frame(slot, MsgType(type), rx.data(), MSG_HEADER_LEN + plen,
                 rx.data() + MSG_HEADER_LEN, plen, now_ms);
    // handle_frame may have scheduled a disconnect but rx is still valid
    rx.erase(rx.begin(), rx.begin() + MSG_HEADER_LEN + plen);
  }
  processing_ = false;
  drain_closes(now_ms);
}

void Server::handle_frame(int slot, MsgType type, const uint8_t *frame, size_t frame_len,
                          const uint8_t *payload, size_t payload_len, uint64_t now_ms) {
  MsgDecoded m;
  if (!msg_decode(type, payload, payload_len, m)) {
    MsgDecoded dummy;
    send_err(slot, dummy, ReplyErrorCode::ERROR_DECODING_MSG);
    disconnect_client(slot, now_ms, "malformed message");
    return;
  }
  Session &s = sessions_[slot];
  logf(LogLevel::DEBUG, "slot %d: %s", slot, msg_type_str(type));

  // phase gating, mirroring upstream handler preambles
  if (type != MsgType::PREINIT && s.phase == Phase::WAIT_PREINIT) {
    send_err(slot, m, ReplyErrorCode::PREINIT_REQUIRED);
    return;
  }
  switch (type) {
    case MsgType::PREINIT:
      handle_preinit(slot, m);
      return;
    case MsgType::INIT:
      handle_init(slot, m, now_ms);
      return;
    default:
      break;
  }
  if (s.phase != Phase::ACTIVE) {
    send_err(slot, m, ReplyErrorCode::INIT_REQUIRED);
    return;
  }
  switch (type) {
    case MsgType::SET_OPTION:
      handle_set_option(slot, m, now_ms);
      break;
    case MsgType::ECHO_REQUEST:
      send(slot, build_echo_reply(frame, frame_len));
      break;
    case MsgType::NODE_LIST:
      handle_node_list(slot, m);
      break;
    case MsgType::VOTE_INFO_REPLY:
      handle_vote_info_reply(slot, m);
      break;
    case MsgType::HEURISTICS_CHANGE:
      handle_heuristics_change(slot, m);
      break;
    case MsgType::ASK_FOR_VOTE:
      // ffsplit does not support ask_for_vote (upstream error 14)
      send_err(slot, m, ReplyErrorCode::UNSUPPORTED_DECISION_ALGORITHM_MESSAGE);
      break;
    case MsgType::STARTTLS:
      send_err(slot, m, ReplyErrorCode::UNSUPPORTED_MESSAGE);
      break;
    default:
      // server-to-client types arriving from a client
      send_err(slot, m, ReplyErrorCode::UNEXPECTED_MESSAGE);
      break;
  }
}

// ---------------------------------------------------------------- handshake

void Server::handle_preinit(int slot, const MsgDecoded &m) {
  Session &s = sessions_[slot];
  if (m.cluster_name.empty()) {
    send_err(slot, m, ReplyErrorCode::DOESNT_CONTAIN_REQUIRED_OPTION);
    return;
  }
  if (s.phase != Phase::WAIT_PREINIT) {
    send_err(slot, m, ReplyErrorCode::UNEXPECTED_MESSAGE);
    return;
  }
  s.cluster_name = m.cluster_name;
  s.phase = Phase::WAIT_INIT;
  send(slot, build_preinit_reply(m.seq_number_set, m.seq_number, TlsSupported::UNSUPPORTED, 0));
}

void Server::handle_init(int slot, const MsgDecoded &m, uint64_t now_ms) {
  Session &s = sessions_[slot];
  ReplyErrorCode code = ReplyErrorCode::NO_ERROR;

  if (s.phase == Phase::ACTIVE)
    code = ReplyErrorCode::UNEXPECTED_MESSAGE;

  if (code == ReplyErrorCode::NO_ERROR && !m.node_id_set)
    code = ReplyErrorCode::DOESNT_CONTAIN_REQUIRED_OPTION;
  else
    s.node_id = m.node_id;

  if (code == ReplyErrorCode::NO_ERROR && !m.ring_id_set)
    code = ReplyErrorCode::DOESNT_CONTAIN_REQUIRED_OPTION;
  else
    s.last_ring_id = m.ring_id;

  if (code == ReplyErrorCode::NO_ERROR) {
    if (!m.heartbeat_interval_set) {
      code = ReplyErrorCode::DOESNT_CONTAIN_REQUIRED_OPTION;
    } else if (m.heartbeat_interval < HEARTBEAT_MIN_MS || m.heartbeat_interval > HEARTBEAT_MAX_MS) {
      code = ReplyErrorCode::INVALID_HEARTBEAT_INTERVAL;
    } else {
      s.heartbeat_ms = m.heartbeat_interval;
    }
  }

  if (code == ReplyErrorCode::NO_ERROR) {
    if (!m.tie_breaker_set)
      code = ReplyErrorCode::DOESNT_CONTAIN_REQUIRED_OPTION;
    else
      s.tie_breaker = m.tie_breaker;
  }

  if (code == ReplyErrorCode::NO_ERROR) {
    if (!m.decision_algorithm_set)
      code = ReplyErrorCode::DOESNT_CONTAIN_REQUIRED_OPTION;
    else if (m.decision_algorithm != Algorithm::FFSPLIT)
      code = ReplyErrorCode::UNSUPPORTED_DECISION_ALGORITHM;
    else
      s.algorithm = m.decision_algorithm;
  }

  // consistency with existing members of the same cluster (upstream
  // init_check_new_client)
  if (code == ReplyErrorCode::NO_ERROR) {
    for (int i = 0; i < MAX_CLIENTS; i++) {
      const Session &o = sessions_[i];
      if (i == slot || o.phase != Phase::ACTIVE || o.cluster_name != s.cluster_name)
        continue;
      if (!(s.tie_breaker == o.tie_breaker)) {
        code = ReplyErrorCode::TIE_BREAKER_DIFFERS_FROM_OTHER_NODES;
        break;
      }
      if (s.algorithm != o.algorithm) {
        code = ReplyErrorCode::ALGORITHM_DIFFERS_FROM_OTHER_NODES;
        break;
      }
      if (s.node_id == o.node_id) {
        code = ReplyErrorCode::DUPLICATE_NODE_ID;
        break;
      }
    }
  }

  if (code == ReplyErrorCode::NO_ERROR) {
    int c = find_or_create_cluster(s.cluster_name);
    if (c < 0) {
      code = ReplyErrorCode::INTERNAL_ERROR;
    } else {
      s.cluster = c;
      if (clusters_[c].members == 0) {
        clusters_[c].state = FfClusterState::WAITING_FOR_CHANGE;
        clusters_[c].quorate_partition.clear();
      }
      clusters_[c].members++;
      s.phase = Phase::ACTIVE;
      reschedule_dpd(s, now_ms);
      logf(LogLevel::INFO, "cluster \"%s\": node %" PRIu32 " joined (%s, hb %" PRIu32 " ms)",
           s.cluster_name.c_str(), s.node_id, s.peer.c_str(), s.heartbeat_ms);
    }
  }

  send(slot, build_init_reply(m.seq_number_set, m.seq_number, code, m.supported_messages_present,
                              m.supported_options_present, max_rx_frame_, max_rx_frame_));
  notify();
}

void Server::handle_set_option(int slot, const MsgDecoded &m, uint64_t now_ms) {
  Session &s = sessions_[slot];
  if (m.heartbeat_interval_set) {
    if (m.heartbeat_interval < HEARTBEAT_MIN_MS || m.heartbeat_interval > HEARTBEAT_MAX_MS) {
      send_err(slot, m, ReplyErrorCode::INVALID_HEARTBEAT_INTERVAL);
      return;
    }
    s.heartbeat_ms = m.heartbeat_interval;
    reschedule_dpd(s, now_ms);
  }
  if (m.keep_active_partition_tie_breaker_set)
    s.keep_active_partition_tb = m.keep_active_partition_tie_breaker != 0;
  send(slot, build_set_option_reply(m.seq_number_set, m.seq_number, m.heartbeat_interval_set,
                                    s.heartbeat_ms, m.keep_active_partition_tie_breaker_set,
                                    s.keep_active_partition_tb ? 1 : 0));
}

// ---------------------------------------------------------------- node lists

void Server::handle_node_list(int slot, const MsgDecoded &m) {
  Session &s = sessions_[slot];
  if (!m.node_list_type_set || !m.seq_number_set) {
    send_err(slot, m, ReplyErrorCode::DOESNT_CONTAIN_REQUIRED_OPTION);
    return;
  }
  Vote result = Vote::NO_CHANGE;
  ReplyErrorCode code = ReplyErrorCode::NO_ERROR;

  switch (m.node_list_type) {
    case NodeListType::INITIAL_CONFIG:
    case NodeListType::CHANGED_CONFIG: {
      // upstream ffsplit config_node_list_received
      if (m.nodes.empty() || m.nodes.find(s.node_id) == nullptr) {
        send_err(slot, m, ReplyErrorCode::INVALID_CONFIG_NODE_LIST);
        return;
      }
      bool initial = m.node_list_type == NodeListType::INITIAL_CONFIG;
      if (initial || s.membership_list.empty()) {
        result = Vote::ASK_LATER;
      } else {
        TriggerView tv{slot, false, &s.last_ring_id, &m.nodes, &s.membership_list,
                       s.last_heuristics};
        code = ffsplit_do(tv, result);
      }
      break;
    }
    case NodeListType::MEMBERSHIP: {
      if (!m.ring_id_set) {
        send_err(slot, m, ReplyErrorCode::DOESNT_CONTAIN_REQUIRED_OPTION);
        return;
      }
      if (m.nodes.empty() || m.nodes.find(s.node_id) == nullptr) {
        send_err(slot, m, ReplyErrorCode::INVALID_MEMBERSHIP_NODE_LIST);
        return;
      }
      if (s.config_list.empty()) {
        result = Vote::ASK_LATER;
      } else {
        TriggerView tv{slot, false, &m.ring_id, &s.config_list, &m.nodes, m.heuristics};
        code = ffsplit_do(tv, result);
      }
      break;
    }
    case NodeListType::QUORUM:
      if (!m.quorate_set) {
        send_err(slot, m, ReplyErrorCode::DOESNT_CONTAIN_REQUIRED_OPTION);
        return;
      }
      result = Vote::NO_CHANGE;  // informative only
      break;
  }

  if (code != ReplyErrorCode::NO_ERROR) {
    send_err(slot, m, code);
    return;
  }

  // store lists AFTER the algorithm ran (it compares new vs stored views)
  switch (m.node_list_type) {
    case NodeListType::INITIAL_CONFIG:
    case NodeListType::CHANGED_CONFIG:
      s.config_list = m.nodes;
      s.config_version_set = m.config_version_set;
      s.config_version = m.config_version;
      break;
    case NodeListType::MEMBERSHIP:
      s.membership_list = m.nodes;
      s.last_ring_id = m.ring_id;
      s.last_heuristics = m.heuristics;
      break;
    case NodeListType::QUORUM:
      s.quorum_list = m.nodes;
      break;
  }

  s.last_sent_vote = result;
  if (result == Vote::ACK || result == Vote::NACK)
    s.last_ack_nack = result;

  send(slot, build_node_list_reply(m.seq_number, m.node_list_type, s.last_ring_id, result));
  notify();
}

void Server::handle_vote_info_reply(int slot, const MsgDecoded &m) {
  // port of ffsplit vote_info_reply_received
  Session &s = sessions_[slot];
  if (!m.seq_number_set) {
    send_err(slot, m, ReplyErrorCode::DOESNT_CONTAIN_REQUIRED_OPTION);
    return;
  }
  if (m.seq_number != s.vote_info_seq) {
    logf(LogLevel::DEBUG, "stale vote-info reply from node %" PRIu32 " (seq %" PRIu32 ", expected %" PRIu32 ")",
         s.node_id,
         m.seq_number, s.vote_info_seq);
    return;
  }
  s.ff_state = FfClientState::WAITING_FOR_CHANGE;
  Cluster &cl = clusters_[s.cluster];
  if (cl.state == FfClusterState::SENDING_NACKS) {
    if (ffsplit_count_state(s.cluster, FfClientState::SENDING_NACK) == 0) {
      logf(LogLevel::DEBUG, "cluster \"%s\": all NACKs acknowledged", cl.name.c_str());
      cl.state = FfClusterState::SENDING_ACKS;
      TriggerView tv{slot, false, &s.last_ring_id, &s.config_list, &s.membership_list,
                     s.last_heuristics};
      if (ffsplit_send_votes(tv, true) == 0)
        cl.state = FfClusterState::WAITING_FOR_CHANGE;
    }
  } else if (cl.state == FfClusterState::SENDING_ACKS) {
    if (ffsplit_count_state(s.cluster, FfClientState::SENDING_ACK) == 0) {
      logf(LogLevel::DEBUG, "cluster \"%s\": all ACKs acknowledged", cl.name.c_str());
      cl.state = FfClusterState::WAITING_FOR_CHANGE;
    }
  }
  notify();
}

void Server::handle_heuristics_change(int slot, const MsgDecoded &m) {
  Session &s = sessions_[slot];
  if (!m.seq_number_set || m.heuristics == Heuristics::UNDEFINED) {
    send_err(slot, m, ReplyErrorCode::DOESNT_CONTAIN_REQUIRED_OPTION);
    return;
  }
  Vote result = Vote::NO_CHANGE;
  ReplyErrorCode code = ReplyErrorCode::NO_ERROR;
  if (s.config_list.empty() || s.membership_list.empty()) {
    result = Vote::ASK_LATER;
  } else {
    TriggerView tv{slot, false, &s.last_ring_id, &s.config_list, &s.membership_list, m.heuristics};
    code = ffsplit_do(tv, result);
  }
  if (code != ReplyErrorCode::NO_ERROR) {
    send_err(slot, m, code);
    return;
  }
  s.last_heuristics = m.heuristics;
  s.last_sent_vote = result;
  if (result == Vote::ACK || result == Vote::NACK)
    s.last_ack_nack = result;
  send(slot, build_heuristics_change_reply(m.seq_number_set, m.seq_number, result));
  notify();
}

// ---------------------------------------------------------------- clusters

int Server::find_or_create_cluster(const std::string &name) {
  for (int i = 0; i < MAX_CLUSTERS; i++)
    if (clusters_[i].used && clusters_[i].name == name)
      return i;
  for (int i = 0; i < MAX_CLUSTERS; i++) {
    if (!clusters_[i].used) {
      clusters_[i] = Cluster();
      clusters_[i].used = true;
      clusters_[i].name = name;
      return i;
    }
  }
  return -1;
}

void Server::leave_cluster(int slot, uint64_t now_ms) {
  (void)now_ms;
  Session &s = sessions_[slot];
  if (s.phase != Phase::ACTIVE || s.cluster < 0)
    return;
  // upstream ffsplit client_disconnect: re-run the algorithm with the
  // leaving client excluded, so the surviving partition can be promoted
  Vote result;
  TriggerView tv{slot, true, &s.last_ring_id, &s.config_list, &s.membership_list,
                 s.last_heuristics};
  ffsplit_do(tv, result);
  Cluster &cl = clusters_[s.cluster];
  cl.members--;
  if (cl.members <= 0)
    cl = Cluster();
  s.cluster = -1;
}

// ---------------------------------------------------------------- ffsplit

const NodeList *Server::eff_config(const Session &s, const TriggerView &tv) const {
  return (!tv.leaving && &s == &sessions_[tv.slot]) ? tv.config : &s.config_list;
}
const NodeList *Server::eff_membership(const Session &s, const TriggerView &tv) const {
  return (!tv.leaving && &s == &sessions_[tv.slot]) ? tv.membership : &s.membership_list;
}
const RingId *Server::eff_ring(const Session &s, const TriggerView &tv) const {
  return (!tv.leaving && &s == &sessions_[tv.slot]) ? tv.ring_id : &s.last_ring_id;
}
Heuristics Server::eff_heuristics(const Session &s, const TriggerView &tv) const {
  return (!tv.leaving && &s == &sessions_[tv.slot]) ? tv.heuristics : s.last_heuristics;
}

Server::Session *Server::cluster_client_by_node_id(int cluster, uint32_t node_id, int except_slot) {
  for (int i = 0; i < MAX_CLIENTS; i++) {
    Session &s = sessions_[i];
    if (i != except_slot && s.phase == Phase::ACTIVE && s.cluster == cluster &&
        s.node_id == node_id)
      return &s;
  }
  return nullptr;
}

bool Server::ffsplit_is_stable(const TriggerView &tv) const {
  int cluster = sessions_[tv.slot].cluster;
  // 1. all active clients share the same config node-id set (pairwise)
  for (int i = 0; i < MAX_CLIENTS; i++) {
    const Session &c1 = sessions_[i];
    if (c1.phase != Phase::ACTIVE || c1.cluster != cluster || skip_in_cluster_walk(c1, tv))
      continue;
    for (int j = 0; j < MAX_CLIENTS; j++) {
      const Session &c2 = sessions_[j];
      if (i == j || c2.phase != Phase::ACTIVE || c2.cluster != cluster ||
          skip_in_cluster_walk(c2, tv))
        continue;
      const NodeList *l1 = eff_config(c1, tv);
      const NodeList *l2 = eff_config(c2, tv);
      for (const auto &n : l1->nodes)
        if (l2->find(n.node_id) == nullptr)
          return false;
    }
  }
  // 2. clients within one partition share ring id and membership set
  for (int i = 0; i < MAX_CLIENTS; i++) {
    const Session &c1 = sessions_[i];
    if (c1.phase != Phase::ACTIVE || c1.cluster != cluster || skip_in_cluster_walk(c1, tv))
      continue;
    const NodeList *mem1 = eff_membership(c1, tv);
    const RingId *ring1 = eff_ring(c1, tv);
    for (const auto &n : mem1->nodes) {
      const Session *c2 = nullptr;
      for (int j = 0; j < MAX_CLIENTS; j++) {
        const Session &cand = sessions_[j];
        if (cand.phase == Phase::ACTIVE && cand.cluster == cluster &&
            cand.node_id == n.node_id && !skip_in_cluster_walk(cand, tv)) {
          c2 = &cand;
          break;
        }
      }
      if (c2 == nullptr)
        continue;  // that member is not connected to us
      const NodeList *mem2 = eff_membership(*c2, tv);
      const RingId *ring2 = eff_ring(*c2, tv);
      if (*ring1 != *ring2)
        return false;
      for (const auto &n3 : mem1->nodes)
        if (mem2->find(n3.node_id) == nullptr)
          return false;
    }
  }
  return true;
}

bool Server::ffsplit_is_preferred_partition(const Session *c, const NodeList *cfg,
                                            const NodeList *mem) const {
  uint32_t preferred = 0;
  switch (c->tie_breaker.mode) {
    case TieBreakerMode::LOWEST: {
      preferred = cfg->nodes.front().node_id;
      for (const auto &n : cfg->nodes)
        if (n.node_id < preferred)
          preferred = n.node_id;
      break;
    }
    case TieBreakerMode::HIGHEST: {
      preferred = cfg->nodes.front().node_id;
      for (const auto &n : cfg->nodes)
        if (n.node_id > preferred)
          preferred = n.node_id;
      break;
    }
    case TieBreakerMode::NODE_ID:
      preferred = c->tie_breaker.node_id;
      break;
  }
  return mem->find(preferred) != nullptr;
}

void Server::ffsplit_partition_stats(const Session *c, const NodeList *mem, Heuristics h,
                                     size_t &clients, size_t &pass, size_t &fail) const {
  clients = pass = fail = 0;
  if (c == nullptr || mem == nullptr)
    return;
  int cluster = c->cluster;
  for (const auto &n : mem->nodes) {
    for (int i = 0; i < MAX_CLIENTS; i++) {
      const Session &s = sessions_[i];
      if (s.phase != Phase::ACTIVE || s.cluster != cluster || s.node_id != n.node_id)
        continue;
      clients++;
      Heuristics eff = (&s == c) ? h : s.last_heuristics;
      if (eff == Heuristics::PASS)
        pass++;
      else if (eff == Heuristics::FAIL)
        fail++;
      break;
    }
  }
}

bool Server::ffsplit_partition_better(const Session *c1, const NodeList *cfg1,
                                      const NodeList *mem1, Heuristics h1, const Session *c2,
                                      const NodeList *cfg2, const NodeList *mem2, Heuristics h2,
                                      const NodeList &prev_quorate, bool keep_active_tb) const {
  (void)cfg2;  // upstream carries it in the signature but never reads it
  if (cfg1->size() % 2 != 0) {
    // odd clusters never split 50:50: strict majority or nothing
    return mem1->size() > cfg1->size() / 2;
  }
  if (mem1->size() > cfg1->size() / 2)
    return true;
  if (mem1->size() < cfg1->size() / 2)
    return false;

  // exact 50:50 split
  size_t n1, p1, f1, n2, p2, f2;
  ffsplit_partition_stats(c1, mem1, h1, n1, p1, f1);
  ffsplit_partition_stats(c2, mem2, h2, n2, p2, f2);
  // fail <= active, so this cannot go negative
  int64_t score1 = int64_t(n1) + (int64_t(p1) - int64_t(f1));
  int64_t score2 = int64_t(n2) + (int64_t(p2) - int64_t(f2));
  if (score1 != score2)
    return score1 > score2;
  if (n1 != n2)
    return n1 > n2;

  if (keep_active_tb && c2 != nullptr) {
    bool in1 = prev_quorate.find(c1->node_id) != nullptr;
    bool in2 = prev_quorate.find(c2->node_id) != nullptr;
    if (in1 != in2)
      return in1;
  }
  return ffsplit_is_preferred_partition(c1, cfg1, mem1);
}

const NodeList *Server::ffsplit_select_partition(const TriggerView &tv) const {
  int cluster = sessions_[tv.slot].cluster;
  const Cluster &cl = clusters_[cluster];

  bool keep_active_tb = true;
  for (int i = 0; i < MAX_CLIENTS; i++) {
    const Session &s = sessions_[i];
    if (s.phase == Phase::ACTIVE && s.cluster == cluster && !s.keep_active_partition_tb) {
      keep_active_tb = false;
      break;
    }
  }

  const Session *best = nullptr;
  const NodeList *best_cfg = nullptr;
  const NodeList *best_mem = nullptr;
  Heuristics best_h = Heuristics::UNDEFINED;

  for (int i = 0; i < MAX_CLIENTS; i++) {
    const Session &s = sessions_[i];
    if (s.phase != Phase::ACTIVE || s.cluster != cluster || skip_in_cluster_walk(s, tv))
      continue;
    const NodeList *cfg = eff_config(s, tv);
    const NodeList *mem = eff_membership(s, tv);
    Heuristics h = eff_heuristics(s, tv);
    if (cfg->empty() || mem->empty())
      continue;
    if (ffsplit_partition_better(&s, cfg, mem, h, best, best_cfg, best_mem, best_h,
                                 cl.quorate_partition, keep_active_tb)) {
      best = &s;
      best_cfg = cfg;
      best_mem = mem;
      best_h = h;
    }
  }
  return best_mem;
}

void Server::ffsplit_update_states(const TriggerView &tv, const NodeList *winner) {
  int cluster = sessions_[tv.slot].cluster;
  for (int i = 0; i < MAX_CLIENTS; i++) {
    Session &s = sessions_[i];
    if (s.phase != Phase::ACTIVE || s.cluster != cluster)
      continue;
    if (tv.leaving && s.node_id == sessions_[tv.slot].node_id) {
      s.ff_state = FfClientState::WAITING_FOR_CHANGE;
      continue;
    }
    if (winner == nullptr || winner->find(s.node_id) == nullptr)
      s.ff_state = FfClientState::SENDING_NACK;
    else
      s.ff_state = FfClientState::SENDING_ACK;
  }
}

size_t Server::ffsplit_send_votes(const TriggerView &tv, bool send_acks) {
  int cluster = sessions_[tv.slot].cluster;
  size_t sent = 0;
  for (int i = 0; i < MAX_CLIENTS; i++) {
    Session &s = sessions_[i];
    if (s.phase != Phase::ACTIVE || s.cluster != cluster || skip_in_cluster_walk(s, tv))
      continue;
    Vote v = Vote::UNDEFINED;
    if (send_acks && s.ff_state == FfClientState::SENDING_ACK)
      v = Vote::ACK;
    if (!send_acks && s.ff_state == FfClientState::SENDING_NACK)
      v = Vote::NACK;
    if (v == Vote::UNDEFINED)
      continue;
    const RingId *ring = eff_ring(s, tv);
    s.vote_info_seq++;
    sent++;
    s.last_sent_vote = v;
    s.last_ack_nack = v;
    logf(LogLevel::INFO, "cluster \"%s\": vote-info %s -> node %" PRIu32 " (ring %" PRIu32 "/%" PRIu64 ")",
         clusters_[cluster].name.c_str(), vote_str(v), s.node_id, ring->node_id, ring->seq);
    send(i, build_vote_info(s.vote_info_seq, *ring, v));
  }
  return sent;
}

size_t Server::ffsplit_count_state(int cluster, FfClientState st) const {
  size_t n = 0;
  for (int i = 0; i < MAX_CLIENTS; i++) {
    const Session &s = sessions_[i];
    if (s.phase == Phase::ACTIVE && s.cluster == cluster && s.ff_state == st)
      n++;
  }
  return n;
}

ReplyErrorCode Server::ffsplit_do(const TriggerView &tv, Vote &result) {
  int cluster = sessions_[tv.slot].cluster;
  Cluster &cl = clusters_[cluster];

  cl.state = FfClusterState::WAITING_FOR_STABLE_MEMBERSHIP;
  if (!ffsplit_is_stable(tv)) {
    logf(LogLevel::DEBUG, "cluster \"%s\": membership not yet stable", cl.name.c_str());
    result = Vote::WAIT_FOR_REPLY;
    return ReplyErrorCode::NO_ERROR;
  }

  const NodeList *winner = ffsplit_select_partition(tv);
  decisions_++;
  if (winner == nullptr)
    logf(LogLevel::WARN, "cluster \"%s\": no partition can be quorate", cl.name.c_str());
  else
    logf(LogLevel::INFO, "cluster \"%s\": quorate partition selected (%u nodes)",
         cl.name.c_str(), (unsigned)winner->size());

  // note: winner may point at a client's stored list or the trigger's
  // incoming list, never at cl.quorate_partition, so this copy is safe
  NodeList new_quorate = winner ? *winner : NodeList();

  ffsplit_update_states(tv, winner);
  cl.quorate_partition = std::move(new_quorate);

  cl.state = FfClusterState::SENDING_NACKS;
  if (ffsplit_send_votes(tv, false) == 0) {
    cl.state = FfClusterState::SENDING_ACKS;
    if (ffsplit_send_votes(tv, true) == 0)
      cl.state = FfClusterState::WAITING_FOR_CHANGE;
  }
  result = Vote::NO_CHANGE;
  return ReplyErrorCode::NO_ERROR;
}

// ---------------------------------------------------------------- status

int Server::connected_clients() const {
  int n = 0;
  for (const auto &s : sessions_)
    if (s.phase == Phase::ACTIVE)
      n++;
  return n;
}

bool Server::any_ack() const {
  for (const auto &s : sessions_)
    if (s.phase == Phase::ACTIVE && s.last_ack_nack == Vote::ACK)
      return true;
  return false;
}

std::string Server::status_string() const {
  std::string out;
  for (int c = 0; c < MAX_CLUSTERS; c++) {
    if (!clusters_[c].used)
      continue;
    if (!out.empty())
      out += " | ";
    out += clusters_[c].name + ":";
    for (const auto &s : sessions_) {
      if (s.phase != Phase::ACTIVE || s.cluster != c)
        continue;
      char buf[48];
      snprintf(buf, sizeof(buf), " %" PRIu32 "=%s", s.node_id, vote_str(s.last_ack_nack));
      out += buf;
    }
  }
  if (out.empty())
    out = "idle";
  return out;
}

}  // namespace qnetd_core
