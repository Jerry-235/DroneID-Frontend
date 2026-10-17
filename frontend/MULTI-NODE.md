# Multi-station setup

One master runs the web UI. Other stations feed it over Tailscale. Every
station's detections land on one map. A drone heard by two stations is one
track, and each broadcast is stored once.

```
 Highway ──┐    decoder :4224  (tailnet)
 (fixed)      ├──► MASTER  ── web UI / map / history / Discord
 Rover node ──┘   decoder :4224  (tailnet)
 (roaming)        health  :4230  (tailnet, node_agent.py)
```

Nothing changes for a single-station setup. Without `nodes.json` the master
behaves exactly as before.

## What each station runs

**Master** (the NUC): wifi_receiver, bluetooth_receiver, zmq_decoder and the
web UI, as now. Its decoder is the `master` station.

**Remote node** (e.g. the Tailscale device at 100.100.10.90): Remote ID
receivers only, with a decoder that publishes to the tailnet. From the repo
root on the node:

```bash
# sniffers, local only
./wifi_receiver.py --interface wlan0 -z --zmqsetting 127.0.0.1:4223
./bluetooth_receiver.py -b 2000000 -s /dev/ttyUSB1 --zmqsetting 127.0.0.1:4222

# decoder, published on the tailnet address only
./zmq_decoder.py -z --zmqsetting 100.100.10.90:4224 --zmqclients 127.0.0.1:4222,127.0.0.1:4223
```

Bind the decoder to the node's Tailscale IP, not `0.0.0.0`. Then only devices
on your tailnet can reach it.

**Every remote node** also runs the position beacon, `node_agent.py`. It reports
the node's health to the master, so the master can show the link and the
sniffers on the map. It needs `pyzmq`, and gpsd if the node has a GPS:

```bash
# roaming node with GPS
python3 node_agent.py --bind 100.100.10.91:4230 --gpsd 127.0.0.1:2947

# any node, health only (the usual case for a fixed station)
python3 node_agent.py --bind 100.100.10.90:4230
```

A station's position is set on the master, in `nodes.json` or the admin page,
not on the node, so a fixed node needs no position here. Bind to the node's
Tailscale address.

The agent also watches the node's own sniffers over ZMQ. It subscribes to
`wifi_receiver.py` on `127.0.0.1:4223` and `bluetooth_receiver.py` on
`127.0.0.1:4222`, the same ports the decoder uses. If a sniffer runs on other
ports, pass them with `--wifi-zmq` and `--bluetooth-zmq`:

```bash
python3 node_agent.py --bind 100.100.10.90:4230 --wifi-zmq 127.0.0.1:4223 --bluetooth-zmq 127.0.0.1:4222
```

A station with no agent still works, but its sniffer lights stay red.

**On the map:** each station is the same ball as the master, with a link dot.
Green means the master is connected to its decoder; red means it isn't. Hover
for the title (`<station name> - CONNECTED` or `DISCONNECTED`, e.g. `Node 1 - CONNECTED`) and two lights: Wi-Fi and
Bluetooth. Each light is the same on the master and the remote nodes:

- green: the sniffer is sending
- yellow: connected to the sniffer but quiet for 12 seconds
- red: not connected to the sniffer (or the agent has stopped reporting)

The map dot is green when both sniffers are connected (sending or quiet), yellow
when one is, and red when neither is.

## The master: nodes.json

Copy `nodes.example.json` to `nodes.json` next to `main.py`. It lists only the
external stations, each with an ID number and an IP. The master is not listed:
it exists whenever the program runs.

```json
{
  "stations": [
    { "id": 1, "ip": "100.100.10.90" },
    { "id": 2, "ip": "100.100.10.91" }
  ]
}
```

| field | meaning |
|---|---|
| `id` | required. A number from 1 to 999, unique. Shown on the admin page as `Node - ID: 1`. |
| `ip` | required. The station's Tailscale address. |
| `lat`, `lon` | optional, both together. A starting position for a fixed station. The admin page can change it. |

The file sets only the ID and the address. The web UI never writes to it.
Names, Fixed or GPS mode, and manual positions are set in the admin page and
stored in the master's database, so a moving GPS never changes the file.

Ports are fixed: the decoder publishes on `:4224` and each remote node's
node_agent on `:4230`.

The file is read at startup. Restart the server after editing it. A malformed
file stops the server with a message naming the station and field.

## Tailscale

- The nodes must be on the tailnet and reachable from the master. Check with
  `tailscale ping 100.100.10.90`.
- Use a Tailscale ACL that lets the master reach the node's ports 4224 and
  4230, and nothing else needs to reach them. Nothing on these ports
  authenticates; the tailnet is what keeps other people out.

## How duplicates are handled

Two stations hearing the same drone:

- **Same drone, one track.** Both feeds use the same burst format, so the
  track is keyed by serial number exactly as before. The track shows the
  latest report, the station that sent it, and every station that has heard it
  (`seen_by`).
- **Same broadcast, stored once.** A broadcast carries its own timestamp, so
  when a second station reports the same position and timestamp it's skipped.
  Without a timestamp, the same position within 2 seconds counts.
- **A station's own repeats are kept.** Drones repeat each broadcast several
  times a second. That has always been stored, and it still is, so existing
  flight data doesn't change shape.

## Decoder update on every node

`zmq_decoder.py` tags each detection with the radio that heard it (`wifi` or
`bluetooth`). The master uses that tag for the Wi-Fi / Bluetooth lines in the
drone panel and in History. Run the updated `zmq_decoder.py` on every remote
node, the same as the master. A node still on the old decoder keeps working,
but its detections show no sensor line.

## What to check after setup

1. `journalctl`/the server log should show one line per station:
   `Subscribed to node-1 decoder output at tcp://100.100.10.90:4224`.
2. The top bar shows the master's ZMQ chip first, named after the master, then a
   chip for each remote station. Each chip is green when its ZMQ link to the
   master is up and red when it isn't. Wi-Fi and Bluetooth are shown on each
   station's map icon: green when both sniffers run, yellow when one does, red
   when neither does.
3. Each remote station appears on the map. A roaming station with no GPS fix
   stays at its last known position and fades.
4. When both stations hear the same drone, its History points show `station_id`
   (`master`, or `node-1`, `node-2` and so on for remote stations), and it's
   only in there once per broadcast.

## Limits

- A Discord alert names the station nearest the drone that heard it, and gives
  the distance from that station.
- Drone popups don't show `seen_by` yet. The data is there, and it can be
  added as a field toggle.
- A node whose agent has been quiet for `DRONEID_NODE_STATUS_STALE_S` (60 s by
  default) is marked stale and fades. A roaming node with no GPS fix reports
  that, and keeps its last known position.
- Node clocks don't matter for duplicate detection. It compares the drone's
  own timestamps, and falls back to the master's receive time. Keep node clocks
  roughly right for history.
