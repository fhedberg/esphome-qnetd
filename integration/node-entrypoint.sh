#!/bin/sh
set -e
cp /etc/corosync/corosync.conf.tmpl /etc/corosync/corosync.conf
# with tls:on the client initializes NSS even if TLS ends up unused; it
# needs an (empty) cert database or it dies with "bad database"
if [ ! -f /etc/corosync/qdevice/net/nssdb/cert9.db ]; then
  mkdir -p /etc/corosync/qdevice/net/nssdb
  certutil -N -d sql:/etc/corosync/qdevice/net/nssdb --empty-password
fi
mkdir -p /var/log/corosync /run/corosync-qdevice
corosync -f &
sleep 3
corosync-qdevice -f -d &
# keep the container alive and stream quorum state
while true; do
  sleep 15
  corosync-quorumtool -s || true
done
