# DroneID remote node

This branch is the software for a remote Remote ID station. A remote node
receives Remote ID broadcasts on Bluetooth and Wi-Fi, decodes them, and serves
the decoded output on its Tailscale address. The master server (the `main`
branch) connects to that address and merges the node's detections into its own
map and history. The remote node has no web UI and no database.

```
Bluetooth dongle ──► bluetooth_receiver.py ─┐  (127.0.0.1:4222)
                                            ├─► zmq_decoder.py ──► tcp://<tailscale-ip>:4224 ──► master
Wi-Fi card (monitor) ──► wifi_receiver.py ──┘  (127.0.0.1:4223)
                                                node_agent.py ──► tcp://<tailscale-ip>:4230 ──► master
                                                (position beacon, roaming nodes only)
```

## Install

```bash
git clone -b multi-node https://github.com/Jerry-235/DroneID-Frontend.git
cd DroneID-Frontend
git submodule update --init
./setup.sh                 # creates .venv, installs requirements, sets capabilities
```

For Wi-Fi, put the card in monitor mode first: `./enable_monitor.sh`.

## Run

Start each of these, in its own terminal or as a service. Replace
`<tailscale-ip>` with this node's address on your tailnet.

### 1. Bluetooth receiver (Sonoff/Sniffle dongle)
```bash
.venv/bin/python bluetooth_receiver.py -b 2000000 -s /dev/ttyUSB1 -z --zmqsetting 127.0.0.1:4222
```
Use `-b 2000000` only for newer Sonoff dongles.

### 2. Wi-Fi receiver (card in monitor mode)
```bash
.venv/bin/python wifi_receiver.py --interface wlan0 -z --zmqsetting 127.0.0.1:4223
```

### 3. Decoder, published to the master
```bash
.venv/bin/python zmq_decoder.py -z --zmqsetting <tailscale-ip>:4224 \
    --zmqclients 127.0.0.1:4222,127.0.0.1:4223
```
Bind to the Tailscale address, not `0.0.0.0`, so only your tailnet can reach it.

### 4. Position beacon (roaming nodes only)
A fixed node does not need this. Its position is set on the master.

```bash
# roaming node with a GPS receiver (gpsd must be running)
.venv/bin/python node_agent.py --node-id rover --name "Rover 1" \
    --bind <tailscale-ip>:4230 --gpsd 127.0.0.1:2947

# fixed node, reporting a set position (no GPS needed)
.venv/bin/python node_agent.py --node-id highway --name "Highway" \
    --bind <tailscale-ip>:4230 --lat 25.992678 --lon -97.176886
```

`--node-id` must match the `id` for this node in the master's `nodes.json`.

## What the master needs from this node

- Decoder output on `tcp://<tailscale-ip>:4224`. This is the only required port.
- Beacon on `tcp://<tailscale-ip>:4230` if the node is roaming.
- The master's `nodes.json` entry for this node, with the same id.

## Layout

- `zmq_decoder.py`, `bluetooth_receiver.py`, `wifi_receiver.py`: receivers and decoder
- `node_agent.py`: position beacon
- `OpenDroneID/`, `Library/`: decoding and Wi-Fi helpers used by the scripts above
- `sniffle/`: Sniffle BLE sniffer firmware (submodule)
- `setup.sh`, `enable_monitor.sh`, `requirements.txt`: setup
