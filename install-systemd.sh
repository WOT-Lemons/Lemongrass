#!/bin/bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SYSTEMD_DIR="$REPO_DIR/systemd"

if [[ $EUID -ne 0 ]]; then
    echo "Run as root: sudo $0"
    exit 1
fi

HOSTNAME="$(hostname)"
if [[ "$HOSTNAME" != car252* ]]; then
    echo "This script is intended for car252 hosts (current hostname: $HOSTNAME)"
    exit 1
fi

SERVICES=(
    lemongrass.service
    pisugar-boot-watchdog.service
    pisugar-watchdog.service
    reverse-tunnel.service
)

CHANGED=()

for service in "${SERVICES[@]}"; do
    dest="/etc/systemd/system/$service"
    src="$SYSTEMD_DIR/$service"
    if cmp -s "$src" "$dest"; then
        echo "  $service unchanged"
    else
        cp "$src" "$dest"
        CHANGED+=("$service")
        echo "  $service installed"
    fi
done

if [[ ${#CHANGED[@]} -gt 0 ]]; then
    systemctl daemon-reload
fi

NOT_STARTED=()

for service in "${SERVICES[@]}"; do
    if ! systemctl is-enabled --quiet "$service"; then
        systemctl enable "$service"
        echo "  $service enabled"
    fi

    if systemctl is-active --quiet "$service"; then
        for changed in "${CHANGED[@]}"; do
            if [[ "$changed" == "$service" ]]; then
                systemctl restart "$service"
                echo "  $service restarted to pick up changes"
                break
            fi
        done
    else
        NOT_STARTED+=("$service")
    fi
done

echo ""
if [[ ${#NOT_STARTED[@]} -gt 0 ]]; then
    echo "Done. The following services are not running:"
    for service in "${NOT_STARTED[@]}"; do
        echo "  $service"
    done
    echo "To start now: sudo systemctl start ${NOT_STARTED[*]}"
else
    echo "Done. All services are running."
fi
