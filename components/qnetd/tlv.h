// qnetd wire types and TLV encoding.
// Ported from corosync-qdevice qdevices/tlv.{h,c} (BSD).
#pragma once
#include <cstdint>
#include <cstddef>
#include <cstring>
#include <vector>

namespace qnetd_core {

// --- protocol enums (values are wire values; do not renumber) ---

enum class MsgType : uint16_t {
  PREINIT = 0,
  PREINIT_REPLY = 1,
  STARTTLS = 2,
  INIT = 3,
  INIT_REPLY = 4,
  SERVER_ERROR = 5,
  SET_OPTION = 6,
  SET_OPTION_REPLY = 7,
  ECHO_REQUEST = 8,
  ECHO_REPLY = 9,
  NODE_LIST = 10,
  NODE_LIST_REPLY = 11,
  ASK_FOR_VOTE = 12,
  ASK_FOR_VOTE_REPLY = 13,
  VOTE_INFO = 14,
  VOTE_INFO_REPLY = 15,
  HEURISTICS_CHANGE = 16,
  HEURISTICS_CHANGE_REPLY = 17,
};

enum class TlvOpt : uint16_t {
  MSG_SEQ_NUMBER = 0,
  CLUSTER_NAME = 1,
  TLS_SUPPORTED = 2,
  TLS_CLIENT_CERT_REQUIRED = 3,
  SUPPORTED_MESSAGES = 4,
  SUPPORTED_OPTIONS = 5,
  REPLY_ERROR_CODE = 6,
  SERVER_MAXIMUM_REQUEST_SIZE = 7,
  SERVER_MAXIMUM_REPLY_SIZE = 8,
  NODE_ID = 9,
  SUPPORTED_DECISION_ALGORITHMS = 10,
  DECISION_ALGORITHM = 11,
  HEARTBEAT_INTERVAL = 12,
  RING_ID = 13,
  CONFIG_VERSION = 14,
  DATA_CENTER_ID = 15,
  NODE_STATE = 16,
  NODE_INFO = 17,
  NODE_LIST_TYPE = 18,
  VOTE = 19,
  QUORATE = 20,
  TIE_BREAKER = 21,
  HEURISTICS = 22,
  KEEP_ACTIVE_PARTITION_TIE_BREAKER = 23,
};

enum class TlsSupported : uint8_t { UNSUPPORTED = 0, SUPPORTED = 1, REQUIRED = 2 };

enum class ReplyErrorCode : uint16_t {
  NO_ERROR = 0,
  UNSUPPORTED_NEEDED_MESSAGE = 1,
  UNSUPPORTED_NEEDED_OPTION = 2,
  TLS_REQUIRED = 3,
  UNSUPPORTED_MESSAGE = 4,
  MESSAGE_TOO_LONG = 5,
  PREINIT_REQUIRED = 6,
  DOESNT_CONTAIN_REQUIRED_OPTION = 7,
  UNEXPECTED_MESSAGE = 8,
  ERROR_DECODING_MSG = 9,
  INTERNAL_ERROR = 10,
  INIT_REQUIRED = 11,
  UNSUPPORTED_DECISION_ALGORITHM = 12,
  INVALID_HEARTBEAT_INTERVAL = 13,
  UNSUPPORTED_DECISION_ALGORITHM_MESSAGE = 14,
  TIE_BREAKER_DIFFERS_FROM_OTHER_NODES = 15,
  ALGORITHM_DIFFERS_FROM_OTHER_NODES = 16,
  DUPLICATE_NODE_ID = 17,
  INVALID_CONFIG_NODE_LIST = 18,
  INVALID_MEMBERSHIP_NODE_LIST = 19,
};

enum class Algorithm : uint16_t { TEST = 0, FFSPLIT = 1, TWONODELMS = 2, LMS = 3 };

enum class NodeState : uint8_t { NOT_SET = 0, MEMBER = 1, DEAD = 2, LEAVING = 3 };

enum class NodeListType : uint8_t {
  INITIAL_CONFIG = 0,
  CHANGED_CONFIG = 1,
  MEMBERSHIP = 2,
  QUORUM = 3,
};

enum class Vote : uint8_t {
  UNDEFINED = 0,
  ACK = 1,
  NACK = 2,
  ASK_LATER = 3,
  WAIT_FOR_REPLY = 4,
  NO_CHANGE = 5,
};

enum class Heuristics : uint8_t { UNDEFINED = 0, PASS = 1, FAIL = 2 };

enum class TieBreakerMode : uint8_t { LOWEST = 1, HIGHEST = 2, NODE_ID = 3 };

struct RingId {
  uint32_t node_id = 0;
  uint64_t seq = 0;
  bool operator==(const RingId &o) const { return node_id == o.node_id && seq == o.seq; }
  bool operator!=(const RingId &o) const { return !(*this == o); }
};

struct TieBreaker {
  TieBreakerMode mode = TieBreakerMode::LOWEST;
  uint32_t node_id = 0;  // only meaningful for mode == NODE_ID
  bool operator==(const TieBreaker &o) const {
    if (mode != o.mode)
      return false;
    return mode != TieBreakerMode::NODE_ID || node_id == o.node_id;
  }
};

struct NodeInfo {
  uint32_t node_id = 0;
  uint32_t data_center_id = 0;         // 0 = not set
  NodeState state = NodeState::NOT_SET;
};

// Bounded node list. Small and copied by value where upstream clones lists.
struct NodeList {
  std::vector<NodeInfo> nodes;
  bool empty() const { return nodes.empty(); }
  size_t size() const { return nodes.size(); }
  void clear() { nodes.clear(); }
  const NodeInfo *find(uint32_t node_id) const {
    for (const auto &n : nodes) {
      if (n.node_id == node_id)
        return &n;
    }
    return nullptr;
  }
};

// --- big-endian helpers ---

inline void be_put16(std::vector<uint8_t> &b, uint16_t v) {
  b.push_back(uint8_t(v >> 8));
  b.push_back(uint8_t(v));
}
inline void be_put32(std::vector<uint8_t> &b, uint32_t v) {
  b.push_back(uint8_t(v >> 24));
  b.push_back(uint8_t(v >> 16));
  b.push_back(uint8_t(v >> 8));
  b.push_back(uint8_t(v));
}
inline void be_put64(std::vector<uint8_t> &b, uint64_t v) {
  be_put32(b, uint32_t(v >> 32));
  be_put32(b, uint32_t(v));
}
inline uint16_t be_get16(const uint8_t *p) { return uint16_t(p[0]) << 8 | p[1]; }
inline uint32_t be_get32(const uint8_t *p) {
  return uint32_t(p[0]) << 24 | uint32_t(p[1]) << 16 | uint32_t(p[2]) << 8 | p[3];
}
inline uint64_t be_get64(const uint8_t *p) {
  return uint64_t(be_get32(p)) << 32 | be_get32(p + 4);
}

// --- TLV writer: appends options to a byte buffer ---

class TlvWriter {
 public:
  explicit TlvWriter(std::vector<uint8_t> &buf) : buf_(buf) {}

  void add(TlvOpt opt, const uint8_t *data, uint16_t len) {
    be_put16(buf_, uint16_t(opt));
    be_put16(buf_, len);
    buf_.insert(buf_.end(), data, data + len);
  }
  void add_u8(TlvOpt opt, uint8_t v) { add(opt, &v, 1); }
  void add_u16(TlvOpt opt, uint16_t v) {
    uint8_t t[2] = {uint8_t(v >> 8), uint8_t(v)};
    add(opt, t, 2);
  }
  void add_u32(TlvOpt opt, uint32_t v) {
    uint8_t t[4] = {uint8_t(v >> 24), uint8_t(v >> 16), uint8_t(v >> 8), uint8_t(v)};
    add(opt, t, 4);
  }
  void add_u64(TlvOpt opt, uint64_t v) {
    uint8_t t[8];
    for (int i = 0; i < 8; i++)
      t[i] = uint8_t(v >> (56 - 8 * i));
    add(opt, t, 8);
  }
  void add_string(TlvOpt opt, const char *s, size_t len) {
    add(opt, reinterpret_cast<const uint8_t *>(s), uint16_t(len));
  }
  void add_u16_array(TlvOpt opt, const uint16_t *arr, size_t n) {
    be_put16(buf_, uint16_t(opt));
    be_put16(buf_, uint16_t(n * 2));
    for (size_t i = 0; i < n; i++)
      be_put16(buf_, arr[i]);
  }
  void add_ring_id(const RingId &r) {
    uint8_t t[12];
    t[0] = uint8_t(r.node_id >> 24);
    t[1] = uint8_t(r.node_id >> 16);
    t[2] = uint8_t(r.node_id >> 8);
    t[3] = uint8_t(r.node_id);
    for (int i = 0; i < 8; i++)
      t[4 + i] = uint8_t(r.seq >> (56 - 8 * i));
    add(TlvOpt::RING_ID, t, 12);
  }
  void add_tie_breaker(const TieBreaker &tb) {
    uint8_t t[5];
    t[0] = uint8_t(tb.mode);
    uint32_t id = (tb.mode == TieBreakerMode::NODE_ID) ? tb.node_id : 0;
    t[1] = uint8_t(id >> 24);
    t[2] = uint8_t(id >> 16);
    t[3] = uint8_t(id >> 8);
    t[4] = uint8_t(id);
    add(TlvOpt::TIE_BREAKER, t, 5);
  }
  // node info is a nested TLV blob
  void add_node_info(const NodeInfo &ni) {
    std::vector<uint8_t> sub;
    TlvWriter w(sub);
    w.add_u32(TlvOpt::NODE_ID, ni.node_id);
    if (ni.data_center_id != 0)
      w.add_u32(TlvOpt::DATA_CENTER_ID, ni.data_center_id);
    if (ni.state != NodeState::NOT_SET)
      w.add_u8(TlvOpt::NODE_STATE, uint8_t(ni.state));
    add(TlvOpt::NODE_INFO, sub.data(), uint16_t(sub.size()));
  }

 private:
  std::vector<uint8_t> &buf_;
};

// --- TLV iterator over a raw payload ---

class TlvIterator {
 public:
  TlvIterator(const uint8_t *payload, size_t len) : p_(payload), len_(len) {}

  // Advance to next option. Returns 1 = positioned, 0 = clean end, -1 = malformed.
  int next() {
    size_t next_pos = first_ ? 0 : pos_ + 4 + cur_len_;
    first_ = false;
    if (next_pos == len_)
      return 0;
    if (next_pos + 4 > len_)
      return -1;
    pos_ = next_pos;
    cur_type_ = be_get16(p_ + pos_);
    cur_len_ = be_get16(p_ + pos_ + 2);
    if (pos_ + 4 + cur_len_ > len_)
      return -1;
    return 1;
  }
  uint16_t type() const { return cur_type_; }
  uint16_t length() const { return cur_len_; }
  const uint8_t *data() const { return p_ + pos_ + 4; }

 private:
  const uint8_t *p_;
  size_t len_;
  size_t pos_ = 0;
  uint16_t cur_type_ = 0;
  uint16_t cur_len_ = 0;
  bool first_ = true;
};

}  // namespace qnetd_core
