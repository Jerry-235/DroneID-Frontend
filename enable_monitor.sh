#!/bin/bash
# Puts a Wi-Fi adapter into monitor mode and prints its type.
# Usage: ./enable_monitor.sh <interface>   (find it with: iw dev)
set -euo pipefail

if [ $# -ne 1 ]; then
    echo "Usage: $0 <interface>   (find it with: iw dev)" >&2
    exit 1
fi

sudo ip link set "$1" down
sudo iw dev "$1" set type monitor
sudo ip link set "$1" up
iw dev "$1" info | grep type