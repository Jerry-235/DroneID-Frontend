# Multi-station setup

One master runs the web UI. Other stations feed it over Tailscale. Every
station's detections land on one map. A drone heard by two stations is one
track, and each broadcast is stored once.

```
 Station A ──┐    decoder :4224  (tailnet)
 (fixed)   ├──► MASTER ── web UI / map / history / Discord
 Station B┘    decoder :4224  (tailnet)
 (roaming)      health    :4230  (tailnet, node_agent.py)
```

A single station needs none of this. Without `nodes.json` the master behaves as
it always has.

## What each station runs

**Master** (the NUC): the Bluetooth and Wi-Fi receivers, `zmq_decoder.py`, and
the web UI from this branch. Its decoder is the `master` station.

**Remote node** (for example a Tailscale device at `100.100.10.90`): the Remote
ID receivers and a decoder that publishes to the tailnet, plus `node_agent.py`.
The remote node has no web UI and no database. Its setup is in the `multi-node`
branch's README.

The node's decoder must bind to its Tailscale address, not `0.0.0.0`, so that
only devices on your tailnet can reach it:

```bash
.venv/bin/python zmq_decoder.py -z --zmqsetting 100.100.10.90:4224 \
    --zmqclients 127.0.0.1:4222,127.0.0.1:4223
```

### The position and health beacon

Every remote node runs `node_agent.py`. It sends two things to the master on
`:4230`:

- **Position.** A roaming node reports its GPS position through gpsd. A fixed
  node reports a position you set, or none at all.
- **Health.** It watches the node's own sniffers over ZMQ and reports each one as
  green, yellow or red. The master shows those lights on the node's map icon.

```bash
# roaming node with GPS (gpsd must be running)
python3 node_agent.py --bind 100.100.10.91:4230 --gpsd 127.0.0.1:2947

# fixed node, health only (the usual case)
python3 node_agent.py --bind 100.100.10.90:4230

# fixed node, with a position reported to the master
python3 node_agent.py --bind 100.100.10.90:4230 --lat <latitude> --lon <longitude>
```

Bind to the node's Tailscale address. The agent reads its sniffers on
`127.0.0.1:4223` (Wi-Fi) and `127.0.0.1:4222` (Bluetooth) by default. If a
sniffer uses other ports, pass them with `--wifi-zmq` and `--bluetooth-zmq`.

A station with no agent still works, but its sniffer lights stay red.

A station's position is set on the master, in `nodes.json` or the admin page,
not on the node. A fixed node needs no position on the node itself.

## On the map

Each station is drawn with the icon chosen for it on the Station page, with a
small dot. The dot and the top-bar chip show different things:

- **Top-bar chip:** green while the master is connected to that station's
  decoder, red when it isn't. A quiet decoder stays green.
- **Map dot:** the station's sniffers. Green when both Wi-Fi and Bluetooth are
  connected, whether sending or quiet, yellow when one is, red when neither is.
- **Hover:** the title (for example `Station A - CONNECTED`, which follows the
  top-bar chip) and two lights: Wi-Fi and Bluetooth.
- **Sniffer lights:** green means the sniffer is sending, yellow means it's
  connected but quiet for `DRONEID_HEALTH_TIMEOUT_S` (12 seconds by default), and
  red means it isn't connected, or the agent hasn't reported for
  `DRONEID_NODE_STATUS_STALE_S` (15 seconds by default).

Both links use ZMQ heartbeats, so a station that loses power or network goes
red within seconds and comes back on its own when it returns. No restart of
`main.py` is needed.

## The master: nodes.json

Copy `nodes.example.json` to `nodes.json` next to `main.py`. It lists only the
external stations, each with an ID number and an address. The master isn't
listed, because it exists whenever the program runs.

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
| `id` | required. A number from 1 to 999, unique. Shown on the admin page as `Node - ID: 1` |
| `ip` | required. The station's Tailscale address |
| `lat`, `lon` | optional, both together. A starting position for a fixed station. The admin page can change it |

The web UI never writes to this file. Names, Fixed or GPS mode and manual
positions are set in the admin page and stored in the master's database, so a
moving GPS never changes the file.

The decoder is published on `:4224` and each remote node's agent on `:4230`.
Those ports are fixed.

The file is read at startup, so restart the server after editing it. A malformed
file stops the server with a message that names the station and the field.

## Tailscale

- Nodes must be on the tailnet and reachable from the master. Check with
  `tailscale ping 100.100.10.90`.
- Use a Tailscale ACL that lets the master reach the node's ports 4224 and 4230,
  and nothing else. Nothing on these ports authenticates. The tailnet is what
  keeps other people out.

## How duplicates are handled

Two stations hearing the same drone:

- **Same drone, one track.** Both feeds use the same burst format, so the track
  is keyed by serial number exactly as before. The track shows the latest report,
  the station that sent it, and every station that has heard it (`seen_by`).
- **Same broadcast, stored once.** A broadcast carries its own timestamp. When a
  second station reports the same position and timestamp, it's skipped. Without a
  timestamp, the same position within 2 seconds counts.
- **A station's own repeats are kept.** Drones repeat each broadcast several times
  a second. Those have always been stored, and they still are, so existing flight
  data doesn't change shape.

## Decoder update on every node

`zmq_decoder.py` tags each detection with its radio (`wifi` or `bluetooth`), its
RSSI, and its channel. The master uses the radio tag for the Wi-Fi and Bluetooth
lines in the drone panel, and the Radio, Channel and RSSI rows in BASIC.

Run the same `zmq_decoder.py` on every remote node as on the master. A node on an
older decoder still works, but its detections show no radio line and no channel
or RSSI.

## Flight place names

Flight names are set by the master, not by each station. A new flight is named
when it starts, from its GPS fix if it has one, and that name is stored on the
flight. Alerts and History use the same stored name, so a drone heard by two
stations has one name. A flight that starts without a fix stays unnamed until
the backfill script runs. See the frontend README.

## What to check after setup

1. The server log shows one line per station:
   `Subscribed to node-1 decoder output at tcp://100.100.10.90:4224`.
2. The top bar shows the master's ZMQ chip first, named after the master, then one
   chip for each remote station. Each chip is green when its ZMQ link to the master
   is up, and red when it isn't. Each station's icon on the map shows its Wi-Fi and
   Bluetooth lights.
3. Each remote station appears on the map. A roaming station with no GPS fix stays
   at its last known position and fades.
4. When two stations hear the same drone, its History points show `station_id`
   (`master`, or `node-1`, `node-2` and so on). The broadcast appears once.

## Limits

- A Discord alert names the station nearest the drone that heard it, and gives
  the distance from that station.
- Drone popups don't show `seen_by` yet. The data is there, and it can be added
  as a field toggle.
- A node whose agent has been quiet for `DRONEID_NODE_STATUS_STALE_S` (60 seconds
  by default) is marked stale and fades. A roaming node with no GPS fix reports
  that and keeps its last known position.
- Node clocks don't affect duplicate detection. It compares the drone's own
  timestamps and falls back to the master's receive time. Keep node clocks roughly
  right for history.