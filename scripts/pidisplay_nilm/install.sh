#!/usr/bin/env bash
# Installs the nilm (appliance disaggregation) daemon on the Pi that hosts InfluxDB.
#
#   sudo ./install.sh                         # install/upgrade the daemon and service
#   sudo INFLUX_READ_TOKEN=... ./install.sh   # ...and set the InfluxDB token (read access to the
#                                             # PowerMeter and homeassistant buckets is all it needs)
#
# Safe to re-run: an existing /etc/nilm/nilm.conf is never overwritten.
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "Run as root (sudo)." >&2
    exit 1
fi

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONF=/etc/nilm/nilm.conf

echo "==> Installing daemon"
id nilm &>/dev/null || useradd --system --no-create-home --shell /usr/sbin/nologin nilm
install -d -m 755 /opt/nilm
install -m 755 "$HERE/nilm.py" /opt/nilm/nilm.py
ln -sf /opt/nilm/nilm.py /usr/local/bin/nilm
install -d -m 750 -o root -g nilm /etc/nilm
if [[ ! -f $CONF ]]; then
    install -m 640 -o root -g nilm "$HERE/nilm.conf.example" "$CONF"
    echo "    created $CONF"
else
    echo "    keeping existing $CONF"
fi
install -m 644 "$HERE/nilm.service" /etc/systemd/system/nilm.service

if [[ -n "${INFLUX_READ_TOKEN:-}" ]]; then
    # A read-only token is enough: results are stored locally (/var/lib/nilm/nilm.db).
    sed -i -E "s|^token *=.*|token = ${INFLUX_READ_TOKEN}|" "$CONF"
    echo "    wrote the read token to $CONF"
fi

if grep -qE '^token *= *REPLACE_ME' "$CONF"; then
    echo "!! $CONF still has the placeholder token; edit it, then: systemctl restart nilm" >&2
fi

echo "==> Starting service"
systemctl daemon-reload
systemctl enable nilm.service
systemctl restart nilm.service
sleep 3
systemctl --no-pager --lines=8 status nilm.service || true
echo "First start learns from the stored history (a few minutes); then: sudo nilm --report"
