# DroneID Remote ID receiver and map

Receives Open Drone ID (Remote ID) broadcasts over Bluetooth 5 and Wi-Fi,
decodes them, and shows them on a live map with flight history and Discord
alerts. The decoders come from [bkerler/DroneID](https://github.com/bkerler/DroneID);
this repo adds the receivers, a decoder that publishes to the rest of the
system, and the web frontend.

```
Sonoff/Sniffle BLE dongle ──► bluetooth_receiver.py ─┐  ZMQ 127.0.0.1:4222
Wi-Fi card (monitor mode) ──► wifi_receiver.py ──────┤  ZMQ 127.0.0.1:4223
AntSDR (DJI, optional)    ──► dji_receiver.py ───────┤  ZMQ 127.0.0.1:4221 (optional)
                                                     ▼
                                           zmq_decoder.py ──► ZMQ 127.0.0.1:4224
                                                                  │
                                                                  ▼
                                              frontend/ (web UI, history, Discord)
```

Each receiver publishes raw output on its own port. `zmq_decoder.py` decodes
all of it and publishes one stream on `:4224`. The frontend subscribes to that
stream and records every detection.

## What's in the repo

| Path | What it is |
| --- | --- |
| `bluetooth_receiver.py` | Reads a Sonoff dongle flashed with Sniffle, a BLE 5 sniffer |
| `wifi_receiver.py` | Captures Remote ID frames from a Wi-Fi card in monitor mode, or replays a pcap |
| `zmq_decoder.py` | Decodes Bluetooth and Wi-Fi output into one stream with radio details |
| `dji_receiver.py` | Optional. Reads DJI DroneID from an AntSDR |
| `bluetooth_spoof.py` | Transmits the Remote ID packets described in `drone.json` through the dongle, to test receivers |
| `frontend/` | The web UI, database, Discord alerts. See `frontend/README.md` |
| `frontend/MULTI-NODE.md` | Adding remote stations over Tailscale |
| `OpenDroneID/`, `Library/` | Decoding and Wi-Fi helpers used by the scripts |
| `sniffle/` | Sniffle BLE firmware and host tools (git submodule) |
| `setup.sh`, `enable_monitor.sh`, `requirements.txt` | Setup |
| `examples/` | Sample captures for `wifi_receiver.py --pcap` |

## Requirements

- Linux. Developed and tested on Ubuntu 24.04.
- Python 3.
- For Bluetooth: a Sonoff dongle (or similar) flashed with Sniffle.
- For Wi-Fi: a card whose driver supports monitor mode. Check with
  `iw list | grep -A12 "Supported interface modes" | grep monitor`. If `monitor`
  doesn't appear, the card can't capture Remote ID.
- For the frontend: nothing beyond Python. Optional Discord webhook.

## Install

```bash
git clone https://github.com/Jerry-235/DroneID-Frontend.git
cd DroneID-Frontend
git submodule update --init
./setup.sh
```

`setup.sh` does three things:

1. Creates `.venv/` and installs `requirements.txt` into it.
2. Gives the venv's Python the raw-socket capabilities (`cap_net_raw`,
   `cap_net_admin`), so the Wi-Fi receiver runs without `sudo`. Those
   capabilities apply to every program that uses that interpreter.
3. Checks for `iw`, `ip` and `rfkill`, and prints the `apt` command if any are
   missing:

   ```bash
   sudo apt install iw rfkill
   ```

Activate the venv with `source`, not by running the file:

```bash
source .venv/bin/activate
```

The frontend needs its own packages. Install them into the same venv:

```bash
.venv/bin/pip install -r frontend/requirements.txt
```

## Bluetooth Remote ID

```bash
.venv/bin/python bluetooth_receiver.py -s /dev/ttyUSB1 -b 2000000 -e -l -z --zmqsetting 127.0.0.1:4222
```

- `-s` is the dongle's serial port. Find it with `ls /dev/ttyUSB*` before and
  after plugging the dongle in.
- `-b 2000000` is for newer Sonoff dongles only. Leave it out for older ones.
- `-e` captures extended (BT 5) advertising. Remote ID arrives this way, and
  the decoder ignores anything else, so without `-e` nothing is decoded.
- `-l` uses the long-range (coded) PHY. `-e -l` is the combination used on
  this hardware.
- `-z` publishes to ZMQ. Without it the receiver only prints.

## Wi-Fi Remote ID

### 1. Put the card in monitor mode

```bash
iw dev                                   # find the adapter, e.g. wlp0s12f0
sudo rfkill unblock all                  # clear any software block
sudo nmcli device set wlp0s12f0 managed no   # only if NetworkManager is running
./enable_monitor.sh wlp0s12f0            # ends with "type monitor" when it worked
```

`nmcli ... managed no` matters on desktop-style installs. Without it,
NetworkManager can switch the card back to managed mode right after you set
monitor mode. `enable_monitor.sh` is optional, because `wifi_receiver.py` sets
monitor mode itself when you give it `--interface`. It restores managed mode
when it exits.

### 2. Run the receiver

```bash
.venv/bin/python wifi_receiver.py --interface wlp0s12f0 -z --zmqsetting 127.0.0.1:4223
```

By default it stays on channel 6 (2.4 GHz). Channel 6 and channel 149 (5 GHz)
are the two the receiver knows to look at for Remote ID, so these options
cover them:

| Option | What it does |
| --- | --- |
| `-g` | Also use 5 GHz channel 149, and hop between channel 6 and 149 |
| `--hop-cycle 3,1` | Seconds to dwell on 2.4 GHz and on 5 GHz while hopping |
| `--no-hop` | With `-g`, stay on 5 GHz only |
| `--channel N` | Fix the card on one channel, 1–14 or 36–165. Turns hopping off |
| `--min-rssi -85` | Drop frames weaker than this many dBm before publishing |
| `-v` | Print every decoded message |

Every Wi-Fi detection carries the channel and signal strength it was heard on.

### Replay a capture

```bash
.venv/bin/python wifi_receiver.py --pcap examples/odid_wifi_sample.pcap -z --zmqsetting 127.0.0.1:4223
```

Replays the file until Ctrl-C, so the rest of the pipeline can be tested without
a drone. It needs no interface and no monitor mode.

## Decoder

```bash
.venv/bin/python zmq_decoder.py -z --zmqsetting 127.0.0.1:4224 --zmqclients 127.0.0.1:4222,127.0.0.1:4223
```

- `--zmqclients` lists the Bluetooth and Wi-Fi receivers it reads from. These
  are the defaults, so you can leave it out with the default ports.
- `-z` publishes the decoded stream on `--zmqsetting`.
- `--dji 127.0.0.1:4221` reads the DJI receiver, if you run one.
- `--uart /dev/ttyACM0` reads pre-decoded data from an ESP32. Needs `-z`.
- `-v` prints each decoded message.

Each detection is tagged with the radio that heard it (`wifi` or `bluetooth`),
and carries its RSSI, and its channel: the Wi-Fi channel for Wi-Fi, and the
BLE channel index Sniffle reports for Bluetooth. The two numbering systems
differ, so a Bluetooth channel of 38 is not a Wi-Fi channel.

## Frontend

```bash
cd frontend
../.venv/bin/python main.py
```

Open `http://<this-machine>:8000/`. The database is created as `droneid.db`
next to `main.py` on first use. Everything in `frontend/README.md` applies:
configuration, the map, history, Discord alerts and admin access.

## Startup order

Order isn't strict. ZMQ connects subscribers once a publisher appears. Start
them in this order anyway, so the logs read cleanly:

1. `bluetooth_receiver.py` and `wifi_receiver.py`
2. `zmq_decoder.py`
3. `frontend/main.py`

The frontend's top bar should go green once the browser connects. Drones appear
as soon as the decoder hears one.

## Ports

| Port | Source | Used by |
| --- | --- | --- |
| 4221 | `dji_receiver.py` | `zmq_decoder.py --dji` |
| 4222 | `bluetooth_receiver.py` | `zmq_decoder.py`, frontend health |
| 4223 | `wifi_receiver.py` | `zmq_decoder.py`, frontend health |
| 4224 | `zmq_decoder.py` | frontend (all drone data) |
| 4230 | `node_agent.py` on a remote node | frontend (remote stations) |
| 8000 | `frontend/main.py` | the browser |

Bind these to `127.0.0.1` unless another machine needs them. Remote stations
use their Tailscale addresses. See `frontend/MULTI-NODE.md`.

## Troubleshooting

**Wi-Fi: "Enabling monitor mode failed"**
- Run `iw dev <iface> info` and look for `type monitor`.
- If the type stays `managed`, NetworkManager is probably switching it back.
  Run `sudo nmcli device set <iface> managed no`, then try again.
- Check `sudo rfkill list`. A card that is soft- or hard-blocked can't change mode.
- Check `iw list` for `monitor` under supported modes. If it's missing, the
  card can't capture Remote ID.
- `sudo dmesg | tail -30` shows driver errors.

**Wi-Fi: frames arrive but no Remote ID**
- Confirm frames are arriving with `sudo tcpdump -i <iface> -c 20`.
- Remote ID is sparse. Leave the receiver running for a few minutes with a drone
  in range, and try `-g` for 5 GHz.
- With `--min-rssi`, a weak drone is dropped. Remove the option to check.

**Bluetooth: nothing decoded**
- Confirm `-e` is on. The decoder ignores everything else.
- Confirm the dongle is on the right port and running Sniffle.

**"Missing CAP_NET_RAW/CAP_NET_ADMIN"**
- Re-run `./setup.sh`, or check the capabilities with
  `getcap $(readlink -f .venv/bin/python3)`.

**`iwconfig: command not found`**
- The Wi-Fi scripts use `iw`, not `iwconfig`. Pull the current branch. The
  `wireless-tools` package isn't in recent Ubuntu releases and isn't needed.

## Layout of the data path

- `zmq_decoder.py` is the only thing the frontend reads drone data from.
- The sniffer ports are read by the frontend only to show health lights.
- Remote stations send their decoder output to the master's frontend over
  Tailscale. The remote-node setup is on the `multi-node` branch.