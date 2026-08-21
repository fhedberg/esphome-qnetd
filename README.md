# esphome-qnetd

A **corosync-qnetd quorum arbiter as an ESPHome component** — an ESP32 casts
the tie-breaking third vote for a two-node Proxmox VE / corosync cluster. The
nodes run stock `corosync-qdevice`; the ESP replaces the `corosync-qnetd`
daemon on TCP 5403.

Why it's defensible: an *absent* arbiter costs nothing — both nodes still see
each other (2 of 3 votes) and stay quorate. Upstream qnetd keeps no persistent
state either; safety rests on the client dropping the device vote the instant
its TCP session dies, and on NACK-before-ACK vote transfer. This ports that
algorithm (ffsplit) from upstream and asserts the safety property in its
tests.

| Event | Behaviour |
|---|---|
| ESP reboots (OTA, power, crash) | Both nodes lose the TCP session and drop the device vote → 2-of-3, still quorate if the ring is up. Fresh decisions on reconnect; same as restarting real qnetd. |
| ESP hangs with TCP still open | The clients' own timers (echo replies stop) disconnect them → as above. |
| LAN partition isolates the ESP | Nodes still see each other: 2-of-3, quorate. Nothing happens. |
| ESP answers *wrongly* | The dangerous case — mitigated by porting upstream's algorithm verbatim, and by tests asserting the both-sides-ACKed state is unreachable. |

## Security

**TLS is deliberately not implemented.** The server advertises
TLS-unsupported; the stock client, whose default is `tls: on`, accepts that
and continues in plaintext. Only `tls: required` on the node side refuses.

The consequence is worth stating plainly: anyone who can reach the segment can
impersonate the arbiter, and a hostile arbiter can ACK both sides of a
partition at once. That is split-brain — two partitions each believing they
are quorate, both running the same VMs against the same storage. Run this only
on a network you would already trust with unauthenticated corosync traffic.

## Usage

Requires **ESPHome 2026.2.0 or newer**; that release replaced the socket API
this component uses, so older versions will not compile.

```yaml
external_components:
  - source: github://fhedberg/esphome-qnetd
    components: [qnetd]

qnetd:
  port: 5403

api:
  reboot_timeout: 0s   # mandatory: never reboot because HA is unreachable
```

`reboot_timeout: 0s` is not optional. ESPHome reboots when no API client has
connected for 15 minutes by default, and Home Assistant is typically a guest
*on* the arbitrated cluster — during the exact outage the arbiter exists for,
HA may well be down. The same applies to `wifi:`/`ethernet:` reboot timeouts.

Full example with entities and wired Ethernet (Olimex ESP32-POE-ISO):
`example/qnetd-arbiter.yaml`.

On the cluster nodes, edit `/etc/pve/corosync.conf` by hand and bump
`config_version` (`pvecm qdevice setup` needs SSH to the arbiter):

```
quorum {
  provider: corosync_votequorum
  device {
    votes: 1
    model: net
    net {
      tls: on              # "on" = use if offered; the ESP declines, plaintext follows
      host: <esp-ip>
      algorithm: ffsplit
      tie_breaker: lowest
    }
  }
}
```

Then on both nodes:

```sh
apt install corosync-qdevice
# tls:on makes the client initialize NSS even though TLS goes unused; without
# a cert database it dies with "bad database":
mkdir -p /etc/corosync/qdevice/net/nssdb   # skip if cert9.db already exists
certutil -N -d sql:/etc/corosync/qdevice/net/nssdb --empty-password
systemctl restart corosync-qdevice
```

Verify with `pvecm status` (3 expected votes plus a `Qdevice` line) and
`corosync-qdevice-tool -s`.

## Entities

| Platform | Key | Meaning |
|---|---|---|
| `sensor` | `connected_clients` | nodes with a completed handshake |
| `sensor` | `decisions` | ffsplit decisions taken since boot |
| `binary_sensor` | `vote_granted` | some node currently holds the ACK |
| `text_sensor` | `status` | per-cluster `node=vote` summary |

## Limits

Compile-time caps in `components/qnetd/server.h`, sized for the two-node case:

| Cap | Default | Rationale |
|---|---|---|
| clients | 8 | 2 needed; slack for reconnect races |
| clusters | 2 | one real, one to test against it |
| nodes per list | 16 | clusters that size don't need a QDevice |
| rx frame | 32 KiB | the floor the client demands; buffers grow on demand |
| heartbeat | 1–120 s | upstream bounds, enforced (error 13) |
| handshake | 30 s | ours: caps a stuck PREINIT/INIT |

Only the **ffsplit** algorithm is implemented — the one Proxmox uses. Others
are rejected with error 12. A build for the Olimex ESP32-POE-ISO comes to
roughly 15% RAM and 21% flash; added to an existing ESP32-S3 config it cost
+432 B RAM and +21 KB flash.

Prefer `logger: level: INFO` on a device that does other work too. At `DEBUG`,
each log line with an API subscriber attached is a protobuf encode plus a TCP
write, and a membership-change burst — a dozen frames across two clients in
one iteration — can overrun ESPHome's 50 ms loop budget and log a warning.
Nothing is at risk when it does (the heartbeat is 8 s, dead-peer detection
12 s), but it is noisy.

## Development

```sh
cd host
make test          # protocol + scenario tests (UBSan)
make build/qnetd_lite && ./build/qnetd_lite   # same core as a POSIX daemon
```

The component itself runs without hardware on ESPHome's `host` platform —
a native binary with the real socket and loop path, which the `integration/`
rig can be pointed at:

```sh
cd example && esphome run qnetd-host.yaml     # listens on :5403
```

- `components/qnetd/` — ESPHome component; `tlv.h`/`msg.*`/`server.*` are the
  portable, I/O-free protocol core (namespace `qnetd_core`)
- `host/` — tests and the POSIX wrapper
- `integration/` — docker-compose interop rig against real corosync-qdevice

Pull requests welcome; keep `make test` green and land core behaviour changes
with their tests. CI runs the tests and a host-platform build of the component
on every push.

## License

BSD 3-Clause — see [LICENSE](LICENSE); the same license as upstream
[corosync/corosync-qdevice](https://github.com/corosync/corosync-qdevice),
from which the protocol and ffsplit algorithm are derived. No upstream code is
copied, but the wire format and algorithm follow it deliberately and cite the
files they mirror.
