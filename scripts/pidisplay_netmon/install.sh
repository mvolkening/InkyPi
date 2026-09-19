#!/usr/bin/env bash
# Installs the netmon daemon on the Pi that hosts InfluxDB.
#
#   sudo ./install.sh                       # install/upgrade the daemon and service
#   sudo INFLUX_ADMIN_TOKEN=... ./install.sh   # ...and also create the buckets + a scoped token
#
# Safe to re-run: an existing /etc/netmon/netmon.conf is never overwritten.
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "Run as root (sudo)." >&2
    exit 1
fi

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONF=/etc/netmon/netmon.conf
ORG="${INFLUX_ORG:-none}"
INFLUX_URL="${INFLUX_URL:-http://localhost:8086}"
RAW_BUCKET=netmon_raw
BUCKET=netmon

echo "==> Installing packages"
# A broken third-party repo (e.g. an expired Grafana key) must not block the install.
apt-get update -qq || echo "    warning: apt-get update failed; continuing with cached package lists"
apt-get install -y speedtest-cli python3

echo "==> Installing daemon"
id netmon &>/dev/null || useradd --system --no-create-home --shell /usr/sbin/nologin netmon
install -d -m 755 /opt/netmon
install -m 755 "$HERE/netmon.py" /opt/netmon/netmon.py
install -m 755 "$HERE/migrate_from_inkypi.py" /opt/netmon/migrate_from_inkypi.py
install -d -m 750 -o root -g netmon /etc/netmon
if [[ ! -f $CONF ]]; then
    install -m 640 -o root -g netmon "$HERE/netmon.conf.example" "$CONF"
    echo "    created $CONF"
else
    echo "    keeping existing $CONF"
fi
install -m 644 "$HERE/netmon.service" /etc/systemd/system/netmon.service

if [[ -n "${INFLUX_ADMIN_TOKEN:-}" ]]; then
    echo "==> Creating InfluxDB buckets and a scoped token"
    export INFLUX_HOST="$INFLUX_URL" INFLUX_TOKEN="$INFLUX_ADMIN_TOKEN"
    influx bucket create -o "$ORG" -n "$RAW_BUCKET" -r 48h >/dev/null 2>&1 || echo "    $RAW_BUCKET already exists"
    influx bucket create -o "$ORG" -n "$BUCKET" -r 400d >/dev/null 2>&1 || echo "    $BUCKET already exists"
    RAW_ID=$(influx bucket list -o "$ORG" -n "$RAW_BUCKET" --hide-headers | awk '{print $1}')
    MAIN_ID=$(influx bucket list -o "$ORG" -n "$BUCKET" --hide-headers | awk '{print $1}')
    NEW_TOKEN=$(influx auth create -o "$ORG" -d "netmon daemon + InkyPi reader" \
        --read-bucket "$RAW_ID" --write-bucket "$RAW_ID" \
        --read-bucket "$MAIN_ID" --write-bucket "$MAIN_ID" --json | python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])')
    sed -i -E "s|^token *=.*|token = ${NEW_TOKEN}|; s|^org *=.*|org = ${ORG}|; s|^url *=.*|url = ${INFLUX_URL}|" "$CONF"
    echo "    wrote token to $CONF (use the same token as INFLUXDB_NETMON_TOKEN in InkyPi's .env)"
fi

if grep -qE '^token *= *REPLACE_ME' "$CONF"; then
    echo "!! $CONF still has the placeholder token; edit it, then: systemctl restart netmon" >&2
fi

echo "==> Starting service"
systemctl daemon-reload
systemctl enable netmon.service
systemctl restart netmon.service
sleep 2
systemctl --no-pager --lines=8 status netmon.service || true
