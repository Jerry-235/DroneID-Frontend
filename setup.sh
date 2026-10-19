#!/bin/bash
# One-time setup: a virtualenv with the requirements, and raw-socket capabilities
# so the sniffers can run without sudo.
set -euo pipefail
cd "$(dirname "$0")"

python3 -m venv .venv
.venv/bin/pip3 install -r requirements.txt

# The capabilities go on the interpreter binary. .venv/bin/python3 is a symlink,
# so setcap lands on the system interpreter it resolves to, and every user of that
# interpreter gets the capabilities. Run the receivers under systemd with
# AmbientCapabilities instead if that is too broad for the machine.
sudo setcap 'cap_net_raw,cap_net_admin+eip' "$(readlink -f .venv/bin/python3)"

# The Wi-Fi receiver switches the adapter with these tools (Library/utils.py).
for tool in iw ip rfkill; do
    command -v "$tool" >/dev/null || echo "missing: $tool (for the Wi-Fi receiver: sudo apt install iw rfkill)"
done
echo "Setup complete. Activate with: source .venv/bin/activate"