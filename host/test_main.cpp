// Scenario and unit tests for the qnetd core, run on the host.
// Includes a minimal protocol client that builds the same frames
// corosync-qdevice sends, so the server is tested over real bytes.
#include <cassert>
#include <cstdio>
#include <deque>
#include <map>
#include "server.h"

using namespace qnetd_core;

static int tests_run = 0, tests_failed = 0;
#define CHECK(cond)                                                         \
  do {                                                                      \
    tests_run++;                                                            \
    if (!(cond)) {                                                          \
      tests_failed++;                                                       \
      printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                \
    }                                                                       \
  } while (0)

// ---------------------------------------------------------------- harness

struct MockTransport : Transport {
  struct Sent {
    int slot;
    std::vector<uint8_t> frame;
  };
  std::vector<Sent> sent;          // full ordered log, never cleared
  std::vector<int> closed;

  void send_frame(int slot, const uint8_t *d, size_t n) override {
    sent.push_back({slot, std::vector<uint8_t>(d, d + n)});
  }
  void close_connection(int slot) override { closed.push_back(slot); }

  // frames sent to `slot` since index `from` (into the global log)
  std::vector<MsgDecoded> frames_for(int slot, size_t from = 0) {
    std::vector<MsgDecoded> out;
    for (size_t i = from; i < sent.size(); i++) {
      if (sent[i].slot != slot)
        continue;
      const auto &f = sent[i].frame;
      MsgDecoded m;
      bool ok = msg_decode(MsgType(be_get16(f.data())), f.data() + 6, f.size() - 6, m);
      assert(ok);
      out.push_back(m);
    }
    return out;
  }
  size_t mark() const { return sent.size(); }
};

static void log_print(LogLevel lvl, const char *msg) {
  const char *l[] = {"D", "I", "W", "E"};
  if (getenv("QNETD_TEST_VERBOSE"))
    printf("  [%s] %s\n", l[int(lvl)], msg);
}

// Minimal qdevice-side frame builders.
struct MockClient {
  uint32_t seq = 0;
  Frame preinit(const char *cluster) {
    Frame f;
    be_put16(f, 0);
    be_put32(f, 0);
    TlvWriter w(f);
    w.add_u32(TlvOpt::MSG_SEQ_NUMBER, ++seq);
    w.add_string(TlvOpt::CLUSTER_NAME, cluster, strlen(cluster));
    patch(f);
    return f;
  }
  Frame init(uint32_t node_id, const RingId &ring, uint32_t hb = 8000,
             TieBreaker tb = {}, Algorithm alg = Algorithm::FFSPLIT) {
    Frame f;
    be_put16(f, 3);
    be_put32(f, 0);
    TlvWriter w(f);
    w.add_u32(TlvOpt::MSG_SEQ_NUMBER, ++seq);
    static const uint16_t msgs[] = {0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17};
    w.add_u16_array(TlvOpt::SUPPORTED_MESSAGES, msgs, 17);
    static const uint16_t opts[] = {0, 1, 2, 3, 4, 5};
    w.add_u16_array(TlvOpt::SUPPORTED_OPTIONS, opts, 6);
    w.add_u32(TlvOpt::NODE_ID, node_id);
    w.add_u16(TlvOpt::DECISION_ALGORITHM, uint16_t(alg));
    w.add_u32(TlvOpt::HEARTBEAT_INTERVAL, hb);
    w.add_tie_breaker(tb);
    w.add_ring_id(ring);
    patch(f);
    return f;
  }
  Frame config_list(NodeListType type, const std::vector<uint32_t> &ids,
                    bool version_set = false, uint64_t version = 0) {
    Frame f;
    be_put16(f, 10);
    be_put32(f, 0);
    TlvWriter w(f);
    w.add_u32(TlvOpt::MSG_SEQ_NUMBER, ++seq);
    w.add_u8(TlvOpt::NODE_LIST_TYPE, uint8_t(type));
    if (version_set)
      w.add_u64(TlvOpt::CONFIG_VERSION, version);
    for (uint32_t id : ids)
      w.add_node_info({id, 0, NodeState::NOT_SET});
    patch(f);
    return f;
  }
  Frame membership(const RingId &ring, const std::vector<uint32_t> &ids,
                   Heuristics h = Heuristics::UNDEFINED) {
    Frame f;
    be_put16(f, 10);
    be_put32(f, 0);
    TlvWriter w(f);
    w.add_u32(TlvOpt::MSG_SEQ_NUMBER, ++seq);
    w.add_u8(TlvOpt::NODE_LIST_TYPE, uint8_t(NodeListType::MEMBERSHIP));
    w.add_ring_id(ring);
    if (h != Heuristics::UNDEFINED)
      w.add_u8(TlvOpt::HEURISTICS, uint8_t(h));
    for (uint32_t id : ids)
      w.add_node_info({id, 0, NodeState::MEMBER});
    patch(f);
    return f;
  }
  Frame quorum_list(bool quorate, const std::vector<uint32_t> &ids) {
    Frame f;
    be_put16(f, 10);
    be_put32(f, 0);
    TlvWriter w(f);
    w.add_u32(TlvOpt::MSG_SEQ_NUMBER, ++seq);
    w.add_u8(TlvOpt::NODE_LIST_TYPE, uint8_t(NodeListType::QUORUM));
    w.add_u8(TlvOpt::QUORATE, quorate ? 1 : 0);
    for (uint32_t id : ids)
      w.add_node_info({id, 0, NodeState::MEMBER});
    patch(f);
    return f;
  }
  Frame vote_info_reply(uint32_t vi_seq) {
    Frame f;
    be_put16(f, 15);
    be_put32(f, 0);
    TlvWriter w(f);
    w.add_u32(TlvOpt::MSG_SEQ_NUMBER, vi_seq);
    patch(f);
    return f;
  }
  Frame echo_request() {
    Frame f;
    be_put16(f, 8);
    be_put32(f, 0);
    TlvWriter w(f);
    w.add_u32(TlvOpt::MSG_SEQ_NUMBER, ++seq);
    patch(f);
    return f;
  }
  static void patch(Frame &f) {
    uint32_t len = uint32_t(f.size() - 6);
    f[2] = uint8_t(len >> 24);
    f[3] = uint8_t(len >> 16);
    f[4] = uint8_t(len >> 8);
    f[5] = uint8_t(len);
  }
};

struct Rig {
  MockTransport tp;
  Server server{&tp, log_print};
  uint64_t now = 1000;
  std::map<int, MockClient> clients;

  int connect() { return server.on_connect(now, "test-peer"); }
  void feed(int slot, const Frame &f) { server.on_data(slot, f.data(), f.size(), now); }
  // full handshake up to ACTIVE
  int join(const char *cluster, uint32_t node_id, const RingId &ring,
           TieBreaker tb = {}) {
    int s = connect();
    feed(s, clients[s].preinit(cluster));
    feed(s, clients[s].init(node_id, ring, 8000, tb));
    return s;
  }
  // find latest vote_info sent to slot after `from`; returns wire seq or 0
  uint32_t last_vote_info(int slot, Vote &v, size_t from = 0) {
    uint32_t seq = 0;
    for (auto &m : tp.frames_for(slot, from)) {
      if (m.type == MsgType::VOTE_INFO) {
        seq = m.seq_number;
        v = m.vote;
      }
    }
    return seq;
  }
  // count vote_infos in the whole log matching a predicate
  int count_vote_infos(int slot, Vote v, size_t from = 0) {
    int n = 0;
    for (auto &m : tp.frames_for(slot, from))
      if (m.type == MsgType::VOTE_INFO && m.vote == v)
        n++;
    return n;
  }
};

// ---------------------------------------------------------------- tests

static void test_golden_bytes() {
  // PREINIT_REPLY with seq 7: type 0x0001, len, TLVs: seq(0,4,7) tls(2,1,0) cert(3,1,0)
  Frame f = build_preinit_reply(true, 7, TlsSupported::UNSUPPORTED, 0);
  const uint8_t expect[] = {0x00, 0x01, 0x00, 0x00, 0x00, 0x12,              // hdr, len 18
                            0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x07,  // seq=7
                            0x00, 0x02, 0x00, 0x01, 0x00,                    // tls=0
                            0x00, 0x03, 0x00, 0x01, 0x00};                   // cert=0
  CHECK(f.size() == sizeof(expect));
  CHECK(memcmp(f.data(), expect, sizeof(expect)) == 0);

  // ring id encoding: node 4, seq 0xb7c
  Frame v = build_vote_info(1, {4, 0xb7c}, Vote::ACK);
  // hdr(6) + seq tlv(8) + vote tlv(5) + ring tlv(16)
  CHECK(v.size() == 6 + 8 + 5 + 16);
  const uint8_t ring_expect[] = {0x00, 0x0d, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x04,
                                 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0b, 0x7c};
  CHECK(memcmp(v.data() + 6 + 8 + 5, ring_expect, sizeof(ring_expect)) == 0);

  // round-trip
  MsgDecoded m;
  CHECK(msg_decode(MsgType(be_get16(v.data())), v.data() + 6, v.size() - 6, m));
  CHECK(m.type == MsgType::VOTE_INFO && m.vote_set && m.vote == Vote::ACK);
  CHECK(m.ring_id_set && m.ring_id.node_id == 4 && m.ring_id.seq == 0xb7c);
}

static void test_handshake_and_ordering() {
  Rig r;
  int s = r.connect();
  CHECK(s == 0);

  // init before preinit -> PREINIT_REQUIRED, connection stays
  r.feed(s, r.clients[s].init(4, {4, 1}));
  auto fr = r.tp.frames_for(s);
  CHECK(fr.size() == 1 && fr[0].type == MsgType::SERVER_ERROR);
  CHECK(r.tp.closed.empty());

  size_t mk = r.tp.mark();
  r.feed(s, r.clients[s].preinit("testcluster"));
  fr = r.tp.frames_for(s, mk);
  CHECK(fr.size() == 1 && fr[0].type == MsgType::PREINIT_REPLY);
  CHECK(fr[0].tls_supported_set && fr[0].tls_supported == TlsSupported::UNSUPPORTED);

  // bad heartbeat -> error 13 in INIT_REPLY... upstream carries it in init_reply
  mk = r.tp.mark();
  r.feed(s, r.clients[s].init(4, {4, 1}, 100));  // 100 ms < min
  fr = r.tp.frames_for(s, mk);
  CHECK(fr.size() == 1 && fr[0].type == MsgType::INIT_REPLY);

  mk = r.tp.mark();
  r.feed(s, r.clients[s].init(4, {4, 1}));
  fr = r.tp.frames_for(s, mk);
  CHECK(fr.size() == 1 && fr[0].type == MsgType::INIT_REPLY);
  CHECK(r.server.connected_clients() == 1);

  // echo round-trip, seq preserved
  mk = r.tp.mark();
  Frame e = r.clients[s].echo_request();
  r.feed(s, e);
  fr = r.tp.frames_for(s, mk);
  CHECK(fr.size() == 1 && fr[0].type == MsgType::ECHO_REPLY);
  CHECK(fr[0].seq_number_set && fr[0].seq_number == r.clients[s].seq);
}

static void test_two_nodes_join_ack() {
  Rig r;
  RingId ring{2, 100};
  int a = r.join("testcluster", 4, ring);

  // A: initial config -> ASK_LATER
  size_t mk = r.tp.mark();
  r.feed(a, r.clients[a].config_list(NodeListType::INITIAL_CONFIG, {2, 4}));
  auto fr = r.tp.frames_for(a, mk);
  CHECK(fr.size() == 1 && fr[0].type == MsgType::NODE_LIST_REPLY && fr[0].vote == Vote::ASK_LATER);

  // A: membership [2,4], B not yet connected -> stable -> vote_info ACK to A
  mk = r.tp.mark();
  r.feed(a, r.clients[a].membership(ring, {2, 4}));
  Vote v = Vote::UNDEFINED;
  uint32_t seq = r.last_vote_info(a, v, mk);
  CHECK(seq != 0 && v == Vote::ACK);
  r.feed(a, r.clients[a].vote_info_reply(seq));

  // B boots later: config + membership -> both end ACKed
  int b = r.join("testcluster", 2, ring);
  r.feed(b, r.clients[b].config_list(NodeListType::INITIAL_CONFIG, {2, 4}));
  mk = r.tp.mark();
  r.feed(b, r.clients[b].membership(ring, {2, 4}));
  Vote va = Vote::UNDEFINED, vb = Vote::UNDEFINED;
  uint32_t sa = r.last_vote_info(a, va, mk);
  uint32_t sb = r.last_vote_info(b, vb, mk);
  CHECK(vb == Vote::ACK);
  if (sa)
    r.feed(a, r.clients[a].vote_info_reply(sa));
  if (sb)
    r.feed(b, r.clients[b].vote_info_reply(sb));
  CHECK(r.server.any_ack());
  CHECK(r.count_vote_infos(a, Vote::NACK) == 0);
  CHECK(r.count_vote_infos(b, Vote::NACK) == 0);
}

static void test_first_reporter_waits_for_silent_peer() {
  // when both nodes are connected but only one has sent its lists, the
  // reporter gets WAIT_FOR_REPLY, not a vote (upstream stability rule)
  Rig r;
  RingId ring{2, 100};
  int a = r.join("testcluster", 4, ring);
  int b = r.join("testcluster", 2, ring);
  (void)b;
  r.feed(a, r.clients[a].config_list(NodeListType::INITIAL_CONFIG, {2, 4}));
  size_t mk = r.tp.mark();
  r.feed(a, r.clients[a].membership(ring, {2, 4}));
  auto fr = r.tp.frames_for(a, mk);
  CHECK(fr.size() == 1 && fr[0].type == MsgType::NODE_LIST_REPLY);
  CHECK(fr[0].vote == Vote::WAIT_FOR_REPLY);
  Vote v;
  CHECK(r.last_vote_info(a, v, mk) == 0);
}

// build a two-node ACKed cluster, return slots
static void ack_two_nodes(Rig &r, int &a, int &b, RingId ring = {2, 100}) {
  a = r.join("testcluster", 4, ring);
  b = r.join("testcluster", 2, ring);
  r.feed(a, r.clients[a].config_list(NodeListType::INITIAL_CONFIG, {2, 4}));
  r.feed(a, r.clients[a].membership(ring, {2, 4}));
  Vote v;
  uint32_t s1 = r.last_vote_info(a, v);
  if (s1)
    r.feed(a, r.clients[a].vote_info_reply(s1));
  r.feed(b, r.clients[b].config_list(NodeListType::INITIAL_CONFIG, {2, 4}));
  size_t mk = r.tp.mark();
  r.feed(b, r.clients[b].membership(ring, {2, 4}));
  uint32_t s2 = r.last_vote_info(b, v, mk);
  uint32_t s3 = r.last_vote_info(a, v, mk);
  if (s2)
    r.feed(b, r.clients[b].vote_info_reply(s2));
  if (s3)
    r.feed(a, r.clients[a].vote_info_reply(s3));
  assert(r.server.any_ack());
}

static void test_node_reboot_survivor_keeps_vote() {
  Rig r;
  int a, b;
  ack_two_nodes(r, a, b);

  // B goes away (reboot)
  size_t mk = r.tp.mark();
  r.server.on_disconnected(b, r.now);
  // survivor's membership still [2,4]: partition of 2 of 2 -> stays ACK.
  // ring reforms: A now alone
  RingId ring2{4, 101};
  r.feed(a, r.clients[a].membership(ring2, {4}));
  Vote v = Vote::UNDEFINED;
  uint32_t seq = r.last_vote_info(a, v, mk);
  CHECK(seq != 0 && v == Vote::ACK);  // 1 of 2 with no competitor -> ACK
  r.feed(a, r.clients[a].vote_info_reply(seq));
  CHECK(r.server.any_ack());
  CHECK(r.count_vote_infos(a, Vote::NACK, mk) == 0);
}

static void test_split_brain_exactly_one_ack() {
  Rig r;
  int a, b;  // a = node 4, b = node 2
  ack_two_nodes(r, a, b);

  // DAC cut: each node reforms alone. A (node 4) reports first.
  size_t mk = r.tp.mark();
  RingId ring_a{4, 200};
  r.feed(a, r.clients[a].membership(ring_a, {4}));
  // not stable yet (B still claims [2,4] on the old ring) -> WAIT_FOR_REPLY
  auto fr = r.tp.frames_for(a, mk);
  CHECK(fr.size() == 1 && fr[0].type == MsgType::NODE_LIST_REPLY);
  CHECK(fr[0].vote == Vote::WAIT_FOR_REPLY);

  // B (node 2) reports its half
  mk = r.tp.mark();
  RingId ring_b{2, 201};
  r.feed(b, r.clients[b].membership(ring_b, {2}));

  // decision: tie-breaker lowest -> node 2 (b) wins; node 4 (a) NACKed
  Vote va = Vote::UNDEFINED, vb = Vote::UNDEFINED;
  uint32_t sa = r.last_vote_info(a, va, mk);
  uint32_t sb = r.last_vote_info(b, vb, mk);
  CHECK(sa != 0 && va == Vote::NACK);
  CHECK(sb == 0);  // *** ACK must not be sent before the NACK is acknowledged ***

  // loser acknowledges its NACK; only now may the winner see ACK
  r.feed(a, r.clients[a].vote_info_reply(sa));
  sb = r.last_vote_info(b, vb, mk);
  CHECK(sb != 0 && vb == Vote::ACK);
  r.feed(b, r.clients[b].vote_info_reply(sb));

  // exactly one ACK holder
  CHECK(r.count_vote_infos(a, Vote::ACK, mk) == 0);
  CHECK(r.count_vote_infos(b, Vote::ACK, mk) == 1);
}

static void test_split_with_dead_loser_needs_dpd() {
  Rig r;
  int a, b;
  ack_two_nodes(r, a, b);

  // split; A reports alone; B is dead and never reports
  size_t mk = r.tp.mark();
  RingId ring_a{4, 300};
  r.feed(a, r.clients[a].membership(ring_a, {4}));
  auto fr = r.tp.frames_for(a, mk);
  CHECK(fr[0].vote == Vote::WAIT_FOR_REPLY);  // unstable: B's stored view disagrees
  Vote v = Vote::UNDEFINED;
  CHECK(r.last_vote_info(a, v, mk) == 0);  // no vote of any kind yet

  // time passes; the survivor keeps heartbeating, B stays silent
  r.now += 6500;
  r.server.tick(r.now);
  r.feed(a, r.clients[a].echo_request());  // refreshes A's DPD deadline
  r.now += 6500;
  r.server.tick(r.now);  // B (silent since the split) crosses 1.5 * 8000 ms
  CHECK(r.tp.closed.size() == 1 && r.tp.closed[0] == b);  // only B disconnected

  // with B gone the cluster is stable again and A gets the vote
  uint32_t seq = r.last_vote_info(a, v, mk);
  CHECK(seq != 0 && v == Vote::ACK);
  r.feed(a, r.clients[a].vote_info_reply(seq));
  CHECK(r.server.any_ack());
}

static void test_stale_vote_info_reply_ignored() {
  Rig r;
  int a, b;
  ack_two_nodes(r, a, b);
  size_t mk = r.tp.mark();
  RingId ring_a{4, 400};
  r.feed(a, r.clients[a].membership(ring_a, {4}));
  RingId ring_b{2, 401};
  r.feed(b, r.clients[b].membership(ring_b, {2}));
  Vote va;
  uint32_t sa = r.last_vote_info(a, va, mk);
  CHECK(sa != 0 && va == Vote::NACK);
  // stale seq (sa - 1) must be ignored: no ACK released
  r.feed(a, r.clients[a].vote_info_reply(sa - 1));
  Vote vb = Vote::UNDEFINED;
  CHECK(r.last_vote_info(b, vb, mk) == 0);
  // correct seq releases it
  r.feed(a, r.clients[a].vote_info_reply(sa));
  CHECK(r.last_vote_info(b, vb, mk) != 0 && vb == Vote::ACK);
}

static void test_init_consistency_errors() {
  Rig r;
  RingId ring{2, 1};
  int a = r.join("testcluster", 4, ring);
  CHECK(r.server.connected_clients() == 1);
  (void)a;

  // duplicate node id
  int b = r.connect();
  r.feed(b, r.clients[b].preinit("testcluster"));
  size_t mk = r.tp.mark();
  r.feed(b, r.clients[b].init(4, ring));
  auto fr = r.tp.frames_for(b, mk);
  CHECK(fr.size() == 1 && fr[0].type == MsgType::INIT_REPLY);
  CHECK(r.server.connected_clients() == 1);  // b not admitted

  // differing tie-breaker
  int c = r.connect();
  r.feed(c, r.clients[c].preinit("testcluster"));
  mk = r.tp.mark();
  r.feed(c, r.clients[c].init(2, ring, 8000, {TieBreakerMode::HIGHEST, 0}));
  fr = r.tp.frames_for(c, mk);
  CHECK(fr.size() == 1 && fr[0].type == MsgType::INIT_REPLY);
  CHECK(r.server.connected_clients() == 1);

  // unsupported algorithm
  int d = r.connect();
  r.feed(d, r.clients[d].preinit("testcluster"));
  mk = r.tp.mark();
  r.feed(d, r.clients[d].init(2, ring, 8000, {}, Algorithm::LMS));
  fr = r.tp.frames_for(d, mk);
  CHECK(fr.size() == 1 && fr[0].type == MsgType::INIT_REPLY);
  CHECK(r.server.connected_clients() == 1);
}

static void test_malformed_and_oversize() {
  Rig r;
  int s = r.connect();
  // oversize frame: header claims 1 MB
  uint8_t big[6] = {0x00, 0x00, 0x00, 0x10, 0x00, 0x00};
  r.server.on_data(s, big, 6, r.now);
  CHECK(!r.tp.closed.empty());

  // truncated TLV inside a full frame
  int s2 = r.connect();
  Frame f;
  be_put16(f, 0);       // preinit
  be_put32(f, 3);       // claims 3-byte payload
  f.push_back(0x00);    // half a TLV header
  f.push_back(0x01);
  f.push_back(0x00);
  r.feed(s2, f);
  bool closed2 = false;
  for (int c : r.tp.closed)
    if (c == s2)
      closed2 = true;
  CHECK(closed2);

  // unknown message type after handshake -> UNEXPECTED_MESSAGE, stays open
  Rig r2;
  RingId ring{2, 1};
  int a = r2.join("testcluster", 4, ring);
  size_t mk = r2.tp.mark();
  Frame weird;
  be_put16(weird, 11);  // NODE_LIST_REPLY from a client = nonsense
  be_put32(weird, 0);
  r2.feed(a, weird);
  auto fr = r2.tp.frames_for(a, mk);
  CHECK(fr.size() == 1 && fr[0].type == MsgType::SERVER_ERROR);
  CHECK(r2.tp.closed.empty());
}

static void test_quorum_list_informative() {
  Rig r;
  int a, b;
  ack_two_nodes(r, a, b);
  size_t mk = r.tp.mark();
  r.feed(a, r.clients[a].quorum_list(true, {2, 4}));
  auto fr = r.tp.frames_for(a, mk);
  CHECK(fr.size() == 1 && fr[0].type == MsgType::NODE_LIST_REPLY);
  CHECK(fr[0].vote == Vote::NO_CHANGE);
}

static void test_fragmented_delivery() {
  // frames delivered one byte at a time must still parse
  Rig r;
  int s = r.connect();
  Frame f = r.clients[s].preinit("testcluster");
  for (uint8_t byte : f)
    r.server.on_data(s, &byte, 1, r.now);
  auto fr = r.tp.frames_for(s);
  CHECK(fr.size() == 1 && fr[0].type == MsgType::PREINIT_REPLY);
}

static void test_heuristics_break_the_split() {
  // 50:50 split where the tie-breaker prefers node 2 but node 2's
  // heuristics FAIL: score (active + pass - fail) must override it
  Rig r;
  int a, b;  // a = node 4, b = node 2
  ack_two_nodes(r, a, b);
  size_t mk = r.tp.mark();
  r.feed(a, r.clients[a].membership({4, 500}, {4}, Heuristics::PASS));
  r.feed(b, r.clients[b].membership({2, 501}, {2}, Heuristics::FAIL));
  Vote va = Vote::UNDEFINED, vb = Vote::UNDEFINED;
  uint32_t sb = r.last_vote_info(b, vb, mk);
  CHECK(sb != 0 && vb == Vote::NACK);  // tie-breaker's favourite loses on score
  r.feed(b, r.clients[b].vote_info_reply(sb));
  uint32_t sa = r.last_vote_info(a, va, mk);
  CHECK(sa != 0 && va == Vote::ACK);
}

int main() {
  test_golden_bytes();
  test_handshake_and_ordering();
  test_two_nodes_join_ack();
  test_first_reporter_waits_for_silent_peer();
  test_node_reboot_survivor_keeps_vote();
  test_split_brain_exactly_one_ack();
  test_split_with_dead_loser_needs_dpd();
  test_stale_vote_info_reply_ignored();
  test_init_consistency_errors();
  test_malformed_and_oversize();
  test_quorum_list_informative();
  test_fragmented_delivery();
  test_heuristics_break_the_split();
  printf("%d checks, %d failures\n", tests_run, tests_failed);
  return tests_failed ? 1 : 0;
}
