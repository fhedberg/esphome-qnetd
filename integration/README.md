# Interop test against real corosync-qdevice

Runs two containers with corosync 3 + corosync-qdevice forming a knet
cluster, pointed at `qnetd_lite` (the same core the ESP runs). Requires
Docker with Linux containers; not runnable natively on macOS.

```sh
docker compose up --build -d
sleep 20
docker compose exec node1 corosync-quorumtool -s
```

Expected: `Quorate: Yes`, membership shows `Qdevice`, total votes 3.

Checks worth performing:

1. `docker compose stop node2` → node1 must remain quorate (2 of 3).
2. `docker compose start node2` → back to 3 of 3.
3. `docker compose restart qnetd` → both nodes drop to 2 of 3 votes
   (still quorate) and reconnect within their timeout.
4. `docker compose exec node1 corosync-qdevice-tool -s` → state connected,
   vote ACK.

To point the nodes at an ESP32 running the component instead, remove the
`qnetd` service and add to both nodes:

```yaml
    extra_hosts:
      - "qnetd:192.168.1.53"
```

No hardware needed: `example/qnetd-host.yaml` runs the same component as a
native binary (ESPHome's `host` platform), and the containers can reach it on
the host via the Docker gateway. Start it with `cd example && esphome run
qnetd-host.yaml`, then bring the nodes up without the `qnetd` container:

```sh
docker compose up -d --no-deps --build node1 node2   # with the override below
```

```yaml
# override.yml
services:
  node1: { extra_hosts: ["qnetd:host-gateway"] }
  node2: { extra_hosts: ["qnetd:host-gateway"] }
```
