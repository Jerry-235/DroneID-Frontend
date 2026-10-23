# DroneID remote node

This branch is the software for a remote Remote ID station. A remote node
receives Remote ID broadcasts on Bluetooth and Wi-Fi, decodes them, and serves
the decoded output on its Tailscale address. The master server (the `main`
branch) connects to that address and merges the node's detections into its own
map and history. The remote node has no web UI and no database.

```
Bluetooth dongle ──► bluetooth_receiver.py ─┐  ZMQ 127.0.0.1:4222
                                            ├─► zmq_decoder.py ──► tcp://<tailscale-ip>:4224 ──► master
Wi-Fi card (monitor) ──► wifi_receiver.py ──┘  ZMQ 127.0.0.1:4223

node_agent.py ──► tcp://<tailscale-ip>:4230 ──► master
(position and sniffer health; every node runs it)
```

The master's setup, including `nodes.json` and the map, is in
`frontend/MULTI-NODE.md` on the `main` branch.

## Requirements

- Linux (developed and tested on Ubuntu 24.04), Python 3
- Tailscale, with the node on your tailnet
- A Sonoff dongle flashed with Sniffle, for Bluetooth
- A Wi-Fi card whose driver supports monitor mode (see the top-level README)
- Optional: a GPS receiver with gpsd, for a roaming node

## Install

```bash
git clone -b multi-node https://github.com/Jerry-235/DroneID-Frontend.git
cd DroneID-Frontend
git submodule update --init
./setup.sh                 # creates .venv, installs requirements, sets capabilities
source .venv/bin/activate  # optional; the commands below use .venv/bin/python directly
```

`setup.sh` checks for `iw`, `ip` and `rfkill`. If any are missing, install them
with `sudo apt install iw rfkill`.

## Wi-Fi: monitor mode

```bash
iw dev                                       # find the adapter, e.g. wlan0
sudo rfkill unblock all
sudo nmcli device set wlan0 managed no       # only if NetworkManager is running
./enable_monitor.sh wlan0                    # ends with "type monitor" when it worked
```

`nmcli ... managed no` stops NetworkManager switching the card back to managed
mode. `wifi_receiver.py` sets monitor mode itself when you give it `--interface`,
so `enable_monitor.sh` is only needed to check the card by hand.

## Run

Start each of these, in its own terminal or as a service. Replace
`<tailscale-ip>` with this node's address on your tailnet. Bind to the Tailscale
address, not `0.0.0.0`, so only your tailnet can reach the node.

### 1. Bluetooth receiver (Sonoff/Sniffle dongle)

```bash
.venv/bin/python bluetooth_receiver.py -s /dev/ttyUSB1 -b 2000000 -e -l -z --zmqsetting 127.0.0.1:4222
```

- `-b 2000000` is for newer Sonoff dongles only. Leave it out for older ones.
- `-e` captures extended (BT 5) advertising. Remote ID arrives this way, and the
  decoder ignores everything else.
- `-l` uses the long-range (coded) PHY. `-e -l` is the combination used on this
  hardware.
- Always publishes to ZMQ. `-z` is accepted but has no effect.

### 2. Wi-Fi receiver (card in monitor mode)

```bash
.venv/bin/python wifi_receiver.py --interface wlan0 -z --zmqsetting 127.0.0.1:4223
```

Optional flags:

- `-g` also listens on 5 GHz channel 149, and hops between 6 and 149.
- `--channel N` fixes the card on one channel (1–14 or 36–165). It turns hopping off.
- `--min-rssi -85` drops frames weaker than that many dBm, and frames with no signal reading.

### 3. Decoder, published to the master

```bash
.venv/bin/python zmq_decoder.py -z --zmqsetting <tailscale-ip>:4224 \
    --zmqclients 127.0.0.1:4222,127.0.0.1:4223
```

Bind the decoder to the Tailscale address, not `0.0.0.0`. The decoder tags every
detection with its radio, its RSSI and its channel, which the master shows in
the drone panel.

### 4. Position and health beacon

Every node runs this. A fixed node doesn't need a position, because its position
is set on the master.

```bash
# roaming node with a GPS receiver (gpsd must be running)
.venv/bin/python node_agent.py --bind <tailscale-ip>:4230 --gpsd 127.0.0.1:2947

# fixed node, health only
.venv/bin/python node_agent.py --bind <tailscale-ip>:4230

# fixed node, reporting a set position (no GPS)
.venv/bin/python node_agent.py --bind <tailscale-ip>:4230 --lat <latitude> --lon <longitude>
```

- The agent has no ID or name of its own. The master knows which station this is
  by the `ip` listed for it in `nodes.json`, so the address in `--bind` must be
  this node's Tailscale address exactly as it appears there.
- `--bluetooth-zmq` and `--wifi-zmq` default to `127.0.0.1:4222` and
  `127.0.0.1:4223`. Pass them if the sniffers use other ports.
- `--interval` sets the seconds between reports (default 2).

The agent doesn't send detections. It reports the node's position and the
health of its two sniffers, green (sending), yellow (connected but quiet) or
red (not connected). A node with no agent still works, but its sniffer lights
stay red on the master.

## What the master needs from this node

- Decoder output on `tcp://<tailscale-ip>:4224`. This is the only required port.
- The agent's beacon on `tcp://<tailscale-ip>:4230`, for the map's status lights
  and, on a roaming node, its position.
- An entry in the master's `nodes.json` with the same `id` as `--node-id`.

Ports on the node: 4222 (Bluetooth, local), 4223 (Wi-Fi, local), 4224 (decoder,
tailnet), 4230 (agent, tailnet).

## Tailscale

- Check reachability from the master with `tailscale ping <node-ip>`.
- Let the master reach ports 4224 and 4230 on the node, and nothing else. Nothing
  on these ports authenticates. The tailnet is what keeps other people out.

## Decoder and agent versions

Run the same `zmq_decoder.py` on every node as on the master. The master uses the
radio tag, RSSI and channel that this decoder adds. A node on an older decoder
still works, but its detections show no radio line.

## Troubleshooting

- **Master shows the node as red.** Check that the decoder is bound to the
  Tailscale address and that `ss -ltn` lists `<tailscale-ip>:4224`. Then check
  `tailscale ping` from the master.
- **Sniffer light is red on the master.** The agent can't see that sniffer. Check
  the `--bluetooth-zmq` and `--wifi-zmq` addresses, and that the receiver is
  running with `-z`.
- **Wi-Fi: "Enabling monitor mode failed".** See the top-level README. Run
  `iw dev <iface> info` and look for `type monitor`. If NetworkManager is running,
  set the interface unmanaged first.
- **Bluetooth: nothing decoded.** Check that `-e` is on.

## Layout

- `zmq_decoder.py`, `bluetooth_receiver.py`, `wifi_receiver.py`: receivers and
  decoder
- `node_agent.py`: the position and health beacon. On `main`, the same file lives
  at `frontend/node_agent.py`.
- `OpenDroneID/`, `Library/`: decoding and Wi-Fi helpers used by the scripts above
- `sniffle/`: Sniffle BLE sniffer firmware and host tools (submodule, `zmq` branch)
- `setup.sh`, `enable_monitor.sh`, `requirements.txt`: setup