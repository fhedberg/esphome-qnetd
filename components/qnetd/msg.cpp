#include "msg.h"

namespace qnetd_core {

const uint16_t SUPPORTED_MESSAGES[18] = {0, 1, 2,  3,  4,  5,  6,  7,  8,
                                         9, 10, 11, 12, 13, 14, 15, 16, 17};
const uint16_t SUPPORTED_OPTIONS[24] = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                                        12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};
const uint16_t SUPPORTED_ALGORITHMS[1] = {uint16_t(Algorithm::FFSPLIT)};

static bool decode_node_info(const uint8_t *data, size_t len, NodeInfo &out) {
  TlvIterator it(data, len);
  bool have_id = false;
  int r;
  while ((r = it.next()) > 0) {
    switch (TlvOpt(it.type())) {
      case TlvOpt::NODE_ID:
        if (it.length() != 4)
          return false;
        out.node_id = be_get32(it.data());
        have_id = true;
        break;
      case TlvOpt::DATA_CENTER_ID:
        if (it.length() != 4)
          return false;
        out.data_center_id = be_get32(it.data());
        break;
      case TlvOpt::NODE_STATE:
        if (it.length() != 1)
          return false;
        out.state = NodeState(it.data()[0]);
        break;
      default:
        break;  // unknown nested options ignored
    }
  }
  return r == 0 && have_id;
}

bool msg_decode(MsgType type, const uint8_t *payload, size_t len, MsgDecoded &out) {
  out = MsgDecoded();
  out.type = type;
  TlvIterator it(payload, len);
  int r;
  while ((r = it.next()) > 0) {
    const uint8_t *d = it.data();
    uint16_t l = it.length();
    switch (TlvOpt(it.type())) {
      case TlvOpt::MSG_SEQ_NUMBER:
        if (l != 4)
          return false;
        out.seq_number = be_get32(d);
        out.seq_number_set = true;
        break;
      case TlvOpt::CLUSTER_NAME:
        out.cluster_name.assign(reinterpret_cast<const char *>(d), l);
        break;
      case TlvOpt::TLS_SUPPORTED:
        if (l != 1 || d[0] > 2)
          return false;
        out.tls_supported = TlsSupported(d[0]);
        out.tls_supported_set = true;
        break;
      case TlvOpt::TLS_CLIENT_CERT_REQUIRED:
        if (l != 1)
          return false;
        out.tls_client_cert_required = d[0];
        out.tls_client_cert_required_set = true;
        break;
      case TlvOpt::SUPPORTED_MESSAGES:
        if (l % 2 != 0)
          return false;
        out.supported_messages_present = true;
        break;
      case TlvOpt::SUPPORTED_OPTIONS:
        if (l % 2 != 0)
          return false;
        out.supported_options_present = true;
        break;
      case TlvOpt::NODE_ID:
        if (l != 4)
          return false;
        out.node_id = be_get32(d);
        out.node_id_set = true;
        break;
      case TlvOpt::DECISION_ALGORITHM:
        if (l != 2)
          return false;
        out.decision_algorithm = Algorithm(be_get16(d));
        out.decision_algorithm_set = true;
        break;
      case TlvOpt::HEARTBEAT_INTERVAL:
        if (l != 4)
          return false;
        out.heartbeat_interval = be_get32(d);
        out.heartbeat_interval_set = true;
        break;
      case TlvOpt::RING_ID:
        if (l != 12)
          return false;
        out.ring_id.node_id = be_get32(d);
        out.ring_id.seq = be_get64(d + 4);
        out.ring_id_set = true;
        break;
      case TlvOpt::CONFIG_VERSION:
        if (l != 8)
          return false;
        out.config_version = be_get64(d);
        out.config_version_set = true;
        break;
      case TlvOpt::NODE_INFO: {
        NodeInfo ni;
        if (!decode_node_info(d, l, ni))
          return false;
        if (out.nodes.size() >= MAX_NODES_PER_LIST)
          return false;
        out.nodes.nodes.push_back(ni);
        break;
      }
      case TlvOpt::NODE_LIST_TYPE:
        if (l != 1 || d[0] > 3)
          return false;
        out.node_list_type = NodeListType(d[0]);
        out.node_list_type_set = true;
        break;
      case TlvOpt::VOTE:
        if (l != 1 || d[0] > 5)
          return false;
        out.vote = Vote(d[0]);
        out.vote_set = true;
        break;
      case TlvOpt::QUORATE:
        if (l != 1 || d[0] > 1)
          return false;
        out.quorate = d[0];
        out.quorate_set = true;
        break;
      case TlvOpt::TIE_BREAKER:
        if (l != 5 || d[0] < 1 || d[0] > 3)
          return false;
        out.tie_breaker.mode = TieBreakerMode(d[0]);
        out.tie_breaker.node_id = be_get32(d + 1);
        out.tie_breaker_set = true;
        break;
      case TlvOpt::HEURISTICS:
        if (l != 1 || d[0] > 2)
          return false;
        out.heuristics = Heuristics(d[0]);
        break;
      case TlvOpt::KEEP_ACTIVE_PARTITION_TIE_BREAKER:
        if (l != 1)
          return false;
        out.keep_active_partition_tie_breaker = d[0];
        out.keep_active_partition_tie_breaker_set = true;
        break;
      default:
        break;  // upstream: "protocol ignores unknown options"
    }
  }
  return r == 0;
}

// --- builders ---

static Frame begin_frame(MsgType type) {
  Frame f;
  be_put16(f, uint16_t(type));
  be_put32(f, 0);  // patched by end_frame
  return f;
}

static Frame end_frame(Frame f) {
  uint32_t len = uint32_t(f.size() - MSG_HEADER_LEN);
  f[2] = uint8_t(len >> 24);
  f[3] = uint8_t(len >> 16);
  f[4] = uint8_t(len >> 8);
  f[5] = uint8_t(len);
  return f;
}

Frame build_preinit_reply(bool seq_set, uint32_t seq, TlsSupported tls, uint8_t cert_required) {
  Frame f = begin_frame(MsgType::PREINIT_REPLY);
  TlvWriter w(f);
  if (seq_set)
    w.add_u32(TlvOpt::MSG_SEQ_NUMBER, seq);
  w.add_u8(TlvOpt::TLS_SUPPORTED, uint8_t(tls));
  w.add_u8(TlvOpt::TLS_CLIENT_CERT_REQUIRED, cert_required);
  return end_frame(std::move(f));
}

Frame build_server_error(bool seq_set, uint32_t seq, ReplyErrorCode code) {
  Frame f = begin_frame(MsgType::SERVER_ERROR);
  TlvWriter w(f);
  if (seq_set)
    w.add_u32(TlvOpt::MSG_SEQ_NUMBER, seq);
  w.add_u16(TlvOpt::REPLY_ERROR_CODE, uint16_t(code));
  return end_frame(std::move(f));
}

Frame build_init_reply(bool seq_set, uint32_t seq, ReplyErrorCode code,
                       bool include_supported_messages, bool include_supported_options,
                       uint32_t max_request_size, uint32_t max_reply_size) {
  Frame f = begin_frame(MsgType::INIT_REPLY);
  TlvWriter w(f);
  if (seq_set)
    w.add_u32(TlvOpt::MSG_SEQ_NUMBER, seq);
  w.add_u16(TlvOpt::REPLY_ERROR_CODE, uint16_t(code));
  if (include_supported_messages)
    w.add_u16_array(TlvOpt::SUPPORTED_MESSAGES, SUPPORTED_MESSAGES, 18);
  if (include_supported_options)
    w.add_u16_array(TlvOpt::SUPPORTED_OPTIONS, SUPPORTED_OPTIONS, 24);
  w.add_u32(TlvOpt::SERVER_MAXIMUM_REQUEST_SIZE, max_request_size);
  w.add_u32(TlvOpt::SERVER_MAXIMUM_REPLY_SIZE, max_reply_size);
  w.add_u16_array(TlvOpt::SUPPORTED_DECISION_ALGORITHMS, SUPPORTED_ALGORITHMS, 1);
  return end_frame(std::move(f));
}

Frame build_set_option_reply(bool seq_set, uint32_t seq, bool hb_set, uint32_t hb,
                             bool kaptb_set, uint8_t kaptb) {
  Frame f = begin_frame(MsgType::SET_OPTION_REPLY);
  TlvWriter w(f);
  if (seq_set)
    w.add_u32(TlvOpt::MSG_SEQ_NUMBER, seq);
  if (hb_set)
    w.add_u32(TlvOpt::HEARTBEAT_INTERVAL, hb);
  if (kaptb_set)
    w.add_u8(TlvOpt::KEEP_ACTIVE_PARTITION_TIE_BREAKER, kaptb);
  return end_frame(std::move(f));
}

Frame build_echo_reply(const uint8_t *request_frame, size_t frame_len) {
  Frame f(request_frame, request_frame + frame_len);
  f[0] = uint8_t(uint16_t(MsgType::ECHO_REPLY) >> 8);
  f[1] = uint8_t(uint16_t(MsgType::ECHO_REPLY));
  return f;
}

Frame build_node_list_reply(uint32_t seq, NodeListType type, const RingId &ring, Vote vote) {
  Frame f = begin_frame(MsgType::NODE_LIST_REPLY);
  TlvWriter w(f);
  w.add_u32(TlvOpt::MSG_SEQ_NUMBER, seq);
  w.add_u8(TlvOpt::NODE_LIST_TYPE, uint8_t(type));
  w.add_ring_id(ring);
  w.add_u8(TlvOpt::VOTE, uint8_t(vote));
  return end_frame(std::move(f));
}

Frame build_vote_info(uint32_t seq, const RingId &ring, Vote vote) {
  Frame f = begin_frame(MsgType::VOTE_INFO);
  TlvWriter w(f);
  w.add_u32(TlvOpt::MSG_SEQ_NUMBER, seq);
  w.add_u8(TlvOpt::VOTE, uint8_t(vote));
  w.add_ring_id(ring);
  return end_frame(std::move(f));
}

Frame build_heuristics_change_reply(bool seq_set, uint32_t seq, Vote vote) {
  Frame f = begin_frame(MsgType::HEURISTICS_CHANGE_REPLY);
  TlvWriter w(f);
  if (seq_set)
    w.add_u32(TlvOpt::MSG_SEQ_NUMBER, seq);
  w.add_u8(TlvOpt::VOTE, uint8_t(vote));
  return end_frame(std::move(f));
}

const char *vote_str(Vote v) {
  switch (v) {
    case Vote::UNDEFINED:
      return "undef";
    case Vote::ACK:
      return "ACK";
    case Vote::NACK:
      return "NACK";
    case Vote::ASK_LATER:
      return "ask-later";
    case Vote::WAIT_FOR_REPLY:
      return "wait";
    case Vote::NO_CHANGE:
      return "no-change";
  }
  return "?";
}

const char *msg_type_str(MsgType t) {
  switch (t) {
    case MsgType::PREINIT:
      return "preinit";
    case MsgType::PREINIT_REPLY:
      return "preinit-reply";
    case MsgType::STARTTLS:
      return "starttls";
    case MsgType::INIT:
      return "init";
    case MsgType::INIT_REPLY:
      return "init-reply";
    case MsgType::SERVER_ERROR:
      return "server-error";
    case MsgType::SET_OPTION:
      return "set-option";
    case MsgType::SET_OPTION_REPLY:
      return "set-option-reply";
    case MsgType::ECHO_REQUEST:
      return "echo-request";
    case MsgType::ECHO_REPLY:
      return "echo-reply";
    case MsgType::NODE_LIST:
      return "node-list";
    case MsgType::NODE_LIST_REPLY:
      return "node-list-reply";
    case MsgType::ASK_FOR_VOTE:
      return "ask-for-vote";
    case MsgType::ASK_FOR_VOTE_REPLY:
      return "ask-for-vote-reply";
    case MsgType::VOTE_INFO:
      return "vote-info";
    case MsgType::VOTE_INFO_REPLY:
      return "vote-info-reply";
    case MsgType::HEURISTICS_CHANGE:
      return "heuristics-change";
    case MsgType::HEURISTICS_CHANGE_REPLY:
      return "heuristics-change-reply";
  }
  return "?";
}

}  // namespace qnetd_core
