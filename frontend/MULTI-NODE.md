# Multi-station setup

One master runs the web UI. Other stations feed it over Tailscale. Every
station's detections land on one map. A drone heard by two stations is one
track, and each broadcast is stored once.

```
 Highway ──┐    decoder :4224  (tailnet)
 (fixed)      ├──► MASTER  ── web UI / map / history / Discord
 Rover node ──┘   decoder :4224  (tailnet)
 (roaming)        status  :4230  (tailnet, node_agent.py)
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

**Roaming node** (has a GPS receiver): the same as a remote node, plus a
position beacon. It needs gpsd running and `pyzmq` in its venv:

```bash
python3 node_agent.py --node-id rover --name "Rover" \
    --bind 100.100.10.91:4230 --gpsd 127.0.0.1:2947
```

A fixed node that doesn't need GPS can report a set position instead:
`--lat 25.99 --lon -97.17`. The beacon is optional for fixed nodes; their
position comes from `nodes.json` on the master.

## The master: nodes.json

Copy `nodes.example.json` to `nodes.json` next to `main.py`, and set the
positions and Tailscale addresses for your stations.

| field | meaning |
|---|---|
| `id` | short lowercase name, unique. Must match `--node-id` on the node. |
| `name` | label on the map and in the top bar |
| `kind` | `master`, `static` (fixed position) or `roaming` (moves with its GPS) |
| `bursts` | the node's decoder endpoint, e.g. `tcp://100.100.10.90:4224` |
| `status` | roaming nodes only: the node_agent endpoint, e.g. `tcp://100.100.10.91:4230` |
| `lat`, `lon` | static nodes only: where the node sits |

The file is read at startup. A malformed file stops the server with a message
naming the station and field, so a typo can't leave a station silently
missing. Restart the server after editing it.

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

## What to check after setup

1. `journalctl`/the server log should show one line per station:
   `Subscribed to highway decoder output at tcp://100.100.10.90:4224`.
2. The top bar shows a chip for each remote station. Green means traffic is
   flowing; yellow means connected but idle; red means not connected.
3. Each remote station appears on the map. A roaming station with no GPS fix
   stays at its last known position and fades.
4. When both stations hear the same drone, its History points show `station_id`
   (`master` or `highway`), and it's only in there once per broadcast.

## Limits

- Discord alerts still give the distance from the home station (the master).
  They don't say which station first heard the drone.
- Drone popups don't show `seen_by` yet. The data is there, and it can be
  added as a field toggle.
- A roaming node's position comes from its own GPS. A fix older than
  `DRONEID_NODE_STATUS_STALE_S` (60 s by default) is marked stale.
- Node clocks don't matter for duplicate detection. It compares the drone's
  own timestamps, and falls back to the master's receive time. Keep node clocks
  roughly right for history.
