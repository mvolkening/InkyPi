#!/usr/bin/env bash
# Installs the forecastlog daemon on the Pi that hosts InfluxDB.
#
#   sudo ./install.sh                          # install/upgrade the daemon and service
#   sudo INFLUX_ADMIN_TOKEN=... ./install.sh   # ...and also create the bucket + a scoped token
#
# Safe to re-run: an existing /etc/forecastlog/forecastlog.conf is never overwritten.
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "Run as root (sudo)." >&2
    exit 1
fi

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONF=/etc/forecastlog/forecastlog.conf
ORG="${INFLUX_ORG:-none}"
INFLUX_URL="${INFLUX_URL:-http://localhost:8086}"
BUCKET=forecast

echo "==> Installing daemon"
id forecastlog &>/dev/null || useradd --system --no-create-home --shell /usr/sbin/nologin forecastlog
install -d -m 755 /opt/forecastlog
install -m 755 "$HERE/forecastlog.py" /opt/forecastlog/forecastlog.py
install -d -m 750 -o root -g forecastlog /etc/forecastlog
if [[ ! -f $CONF ]]; then
    install -m 640 -o root -g forecastlog "$HERE/forecastlog.conf.example" "$CONF"
    echo "    created $CONF"
else
    echo "    keeping existing $CONF"
fi
install -m 644 "$HERE/forecastlog.service" /etc/systemd/system/forecastlog.service

if [[ -n "${INFLUX_ADMIN_TOKEN:-}" ]]; then
    echo "==> Creating InfluxDB bucket and a scoped token"
    export INFLUX_HOST="$INFLUX_URL" INFLUX_TOKEN="$INFLUX_ADMIN_TOKEN"
    # Retention 0 = keep forever; the data is tiny (~25k points per location per year).
    influx bucket create -o "$ORG" -n "$BUCKET" -r 0 >/dev/null 2>&1 || echo "    $BUCKET already exists"
    BUCKET_ID=$(influx bucket list -o "$ORG" -n "$BUCKET" --hide-headers | awk '{print $1}')
    NEW_TOKEN=$(influx auth create -o "$ORG" -d "forecastlog daemon + InkyPi reader" \
        --read-bucket "$BUCKET_ID" --write-bucket "$BUCKET_ID" --json | python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])')
    sed -i -E "s|^token *=.*|token = ${NEW_TOKEN}|; s|^org *=.*|org = ${ORG}|; s|^url *=.*|url = ${INFLUX_URL}|" "$CONF"
    echo "    wrote token to $CONF (use the same token as INFLUXDB_FORECAST_TOKEN in InkyPi's .env)"
fi

if grep -qE '^token *= *REPLACE_ME' "$CONF"; then
    echo "!! $CONF still has the placeholder token; edit it, then: systemctl restart forecastlog" >&2
fi

echo "==> Starting service"
systemctl daemon-reload
systemctl enable forecastlog.service
systemctl restart forecastlog.service
sleep 3
systemctl --no-pager --lines=8 status forecastlog.service || true
