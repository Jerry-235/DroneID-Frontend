# DroneID Web Frontend

A live map and historical playback UI for an existing
[bkerler/DroneID](https://github.com/bkerler/DroneID) setup. Subscribes to
`zmq_decoder.py`'s output, persists every detection to SQLite, and serves a
single-page map with flight history and Discord alerts.

FastAPI + stdlib `sqlite3` on the backend, Leaflet + vanilla JS on the front.
No build step.

## Install and run

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 main.py          # or: uvicorn main:app --host 0.0.0.0 --port 8000
```

Open `http://<this-machine>:8000/`. `droneid.db` is created next to `main.py`
on the first detection.

Start the DroneID pipeline first (order isn't strict — ZMQ handles a
subscriber connecting before a publisher binds):

```bash
./bluetooth_receiver.py --zmqsetting 127.0.0.1:4222 ...
./wifi_receiver.py --interface wlan0 -z --zmqsetting 127.0.0.1:4223
./zmq_decoder.py -z --zmqsetting 127.0.0.1:4224 --zmqclients 127.0.0.1:4222,127.0.0.1:4223 -v
```

On startup the log should read `Subscribed to DroneID decoder at
tcp://127.0.0.1:4224`, and the top-bar dot should go from red to green
("live") as soon as the browser's WebSocket connects — before any drone is
detected. A drone with no GPS fix yet gets a sidebar row but no map icon.

## Configuration

All via environment variables, or a `.env` file next to `main.py` (copy
`.env.example`; `.env` is gitignored). Real environment variables win over
`.env`. Everything is read at startup, so changes need a restart.

| Variable | Default | What it does |
| --- | --- | --- |
| `DRONEID_ZMQ_ADDR` | `tcp://127.0.0.1:4224` | The decoder's output — the only source of drone data |
| `DRONEID_BT_ZMQ_ADDR` | `tcp://127.0.0.1:4222` | `bluetooth_receiver.py`, health monitoring only |
| `DRONEID_WIFI_ZMQ_ADDR` | `tcp://127.0.0.1:4223` | `wifi_receiver.py`, health monitoring only |
| `DRONEID_HEALTH_TIMEOUT_S` | `12` | Silence before a sniffer dot goes green → yellow |
| `DRONEID_STALE_AFTER_S` | `45` | Silence before a track goes gray |
| `DRONEID_DROP_AFTER_S` | `105` | Silence before it leaves the map |
| `DRONEID_FLIGHT_MERGE_WINDOW_S` | `300` | Reappearance window for continuing a flight |
| `DRONEID_DISCORD_WEBHOOK` | — | Enables alerts (see below) |
| `DRONEID_DISCORD_ROLE_ID` | — | Numeric role ID to ping on each alert |
| `DRONEID_DB_PATH` | `droneid.db` beside `main.py` | SQLite file |
| `HOST` / `PORT` | `0.0.0.0` / `8000` | Where to listen |

The two sniffer ports are subscribed to purely for liveness — nothing is
parsed from them. Each of the three top-bar dots reads:

- **green** — connected, and a message arrived recently.
- **yellow** (sniffers only) — connected but idle: the process is up, nothing
  in range.
- **red** — not connected. Detected from ZMQ's socket-monitor
  connect/disconnect events rather than just silence, so a dead process is
  distinguishable from a quiet one. If a dot stays red with its sniffer
  definitely running, check the log for `Could not attach connection
  monitor` — that means the monitor API isn't available on your pyzmq build.

ZMQ is only ever green or red: it's the data pipeline, not a sniffer being
watched.

## Marker lifecycle

Measured in seconds of silence since the last packet:

| Silence | Status | On the map |
| --- | --- | --- |
| 0–45s | `live` | Normal identity colour |
| 45–105s | `stale` | Icon, operator diamond, link and trail all go **gray**; the marker stays parked at its last known position. Any number of tracks can sit here at once. |
| >105s | `dropped` | Removed from the map along with its trail, flight closed out in the DB |

So 45s to go gray, then 60s parked — 1m45s of dead air in total. A stale
sidebar row grays out and picks up a `STALE` tag.

## Flights that survive a dropout

If the same aircraft comes back within `DRONEID_FLIGHT_MERGE_WINDOW_S` of
disappearing, it continues the flight it was already on instead of starting a
new one. History shows one flight spanning the gap (labelled e.g. `2
segments`), and the Discord "new drone" alert doesn't re-fire. The window is
measured from the drop, not the last packet, so the real dead-air tolerance
is `DROP_AFTER_S` + the window. A flight left open by a server restart is
picked up the same way.

"The same aircraft" matches on any of catalog key, MAC, serial, or the
friendly name you assigned — loose on purpose, so it survives a re-randomized
MAC or a serial that hadn't decoded on the first pass. Two things follow:

- The **name** only counts when the serials don't contradict it. Two drones
  you gave the same name stay separate flights, and both alert, as long as
  both serials are known and differ. Two *un-serialled* drones sharing a name
  could still be merged. The match is exact and case-sensitive.
- A merged flight's path has a real gap. Points carry a `segment` number so
  neither the live trail nor playback draws a line across it — you get
  separate strokes.

## Using it

**Live** lists current tracks, most recent first, with altitude, speed and
heading pills and an operator-detected indicator. The ✎ pencil renames a
drone; the name is stored once per catalog key (serial, or MAC if no serial
has been seen) and joined in at display time, so every past and future flight
relabels immediately. A copy button sits at the end of the title, but only
when the title is the aircraft's own serial or MAC — not a name you typed.

**History** lists recorded flights with date, duration, point count, and
`N segments` / `merged ×N` where they apply. Click one to draw its path and
open the scrub bar; drag the slider or hit ▶ to walk a marker along it. The
operator's own trail is solid purple, bold while you're tracking it; the only
dashed purple on the map is the straight drone-to-operator link.

**The detail panel** opens when you select a drone or operator, from the
list, the map, or a focus button.

- *Desktop:* a 340px column on the right that pushes the map narrower rather
  than covering it.
- *Mobile (≤700px):* a full-width lower-third sheet; tap the grab handle to
  expand it to about three-quarters. Selecting something also collapses the
  list so the map gets the width — tap the tab to reopen.

Data sits in one scroll under three bubbles — **BASIC**, **AIRCRAFT**,
**OPERATOR** — each pinned while its section scrolls past. In Live all three
take the drone's own colour; History uses a blue Basic bubble with its
established drone orange and operator purple. Every row follows the Settings
field toggles, and changing a toggle or the unit system updates the panel
immediately, even with Settings open.

The panel's two buttons are the focus controls (the rows no longer carry
their own). Both pan to their unit and leave the zoom alone, so toggling
between them holds steady. They read **Track Drone / Track Operator** on the
live map and **Focus Drone / Focus Operator** in History, where they pan to
the unit's position at the current scrub point. Close with **×** or by tapping
empty map: in Live that clears the selection, in History the flight stays
loaded.

**Settings** holds the per-field toggles, the metric/imperial switch, and —
in admin view — Station, Discord Alerts and Debug. Station is stored
server-side, so it's the same on every device. Debug starts a synthetic
orbiting test drone through the real ingest pipeline; stopping it lets it
decay through the normal stale/drop timeouts rather than vanishing.

### Admin view

Settings > Station, Discord Alerts and Debug are hidden by default. Visit
once with `?jerry=1` (e.g. `http://<host>:8000/?jerry=1`) to unlock them for
that browser; the flag goes into `localStorage` and the parameter is stripped
from the URL so it isn't shared by a bookmark. Other browsers see only the
field toggles and Units.

This is decluttering, **not security** — anyone with dev tools can set the
same flag, and none of the endpoints behind those sections have server-side
auth.

### Merging and deleting flights (admin view)

Each History row gets a checkbox at the right-hand end of its stats line.
Tick one or more and an action bar slides up at the bottom of the panel with
**Merge**, **Delete** and **Clear**. Both actions confirm first; neither can
be undone.

**Merge** folds the selection into the oldest flight: points move across, the
span widens to cover earliest start through latest end, and everything is
renumbered to one segment so the track draws as a single continuous line,
joining each flight's end to the next one's start in time order. The row is
labelled `merged ×N`. It's refused, with the reason shown in the bar, when
fewer than two are selected, when a flight is still in progress, or when they
aren't provably the same aircraft — every one must agree on a serial, or every
one on a MAC (`Cannot merge mismatched info`). Either alone suffices, since a
MAC can be re-randomized while the serial holds and an early flight may only
ever have been seen by MAC. A shared name is never enough here.

Two consequences: a manual merge **flattens automatic segment gaps** inside
the flights it touches (you've asserted they're continuous), and the survivor
keeps the **oldest** flight's catalog key, so merging on MAC across two
serials displays under the older name.

**Delete** removes the flights and their points. Catalog rows are left alone —
they hold your nicknames, and an aircraft with no flights on record is still a
valid entry.

## Discord alerts

One message per genuinely new detection (the test drone is excluded), with
distance from station, landed/flying, speed, heading, and whether an operator
position was found:

```
## Jerry Mini 5 Detected @drone-alerts
26,614ft From Station - Currently flying at 32mph and Heading 122° - Controller Located
```

The second line renders as a code block in Discord. Setup:

1. Channel > *Edit Channel > Integrations > Webhooks > New Webhook*, copy the
   URL into `DRONEID_DISCORD_WEBHOOK`.
2. For a role ping, set `DRONEID_DISCORD_ROLE_ID` to the role's **numeric ID**
   — plain `@role-name` text never notifies anyone. Turn on *User Settings >
   Advanced > Developer Mode*, then *Server Settings > Roles* > right-click the
   role > **Copy Role ID**. Leave unset for no ping.
3. Restart. Settings > Discord Alerts will show "Configured" and a Send Test
   button.

The URL is never stored in the database and never returned by the API — it's
deployment config, not app state, since this repo is meant to live in source
control. `allowed_mentions` is locked to that one role ID on every message
regardless of content: drone nicknames are user-editable text that lands
directly in the alert, so without it, naming a drone `@everyone` would ping
the whole server.

## API

Responses over 1 KB are gzipped (a long flight's point list shrinks about
20×).

| Endpoint | Purpose |
| --- | --- |
| `GET /api/drones` | Snapshot of all current tracks |
| `WS /ws` | Snapshot on connect, then live `update` / `status` / `dropped` / `health` pushes |
| `GET /api/health` | `{"zmq": "green\|red", "bluetooth": "green\|yellow\|red", "wifi": ...}` |
| `GET /api/flights?limit=&offset=` | Flight list, most recent first. `limit` caps at 500 |
| `GET /api/flights/{id}` | One flight's metadata plus its full point list |
| `POST /api/flights/merge` | `{"flight_ids": [1,2]}` → `{"flight_id": <survivor>, "merged": n}`. 400 on fewer than two, 404 unknown id, 409 mismatched or in progress |
| `POST /api/flights/delete` | `{"flight_ids": [1,2]}` → `{"deleted": n}`. 409 if one is in progress |
| `GET /api/drones/catalog` | All catalog_key → nickname mappings |
| `PATCH /api/drones/{catalog_key}/name` | `{"nickname": "Drone 1"}`; `""` clears it back to serial/MAC |
| `GET/PUT/DELETE /api/station` | The station location, `{"lat": , "lon": }` |
| `GET /api/discord_webhook` | `{"configured": true\|false}` — never the URL |
| `POST /api/discord_webhook/test` | Sends a test alert, exercising the role-ping path too |
| `POST /api/test/drone/start`, `/stop`, `GET /status` | The synthetic test drone |

## Limitations

- **No auth on any endpoint.** The admin flag is UI-only. `POST
  /api/flights/merge` and `/delete` are destructive and irreversible and can
  be called by anyone who can reach the server. Fine on a private LAN; put a
  reverse proxy with basic auth in front of it before exposing it further.
- **No undo, no un-merge, no split.** Copy `droneid.db` before a big tidy-up.
  If the merge window is too generous for your airspace, lower
  `DRONEID_FLIGHT_MERGE_WINDOW_S` rather than fixing it afterwards.
- **Every burst is written as a DB point** — about 20 MB a month at a few
  drones. Fine at single-operator scale; batch the writes for heavier traffic.
- **Raw-units warning.** If a burst arrives in the raw/scaled coordinate shape
  rather than pre-decoded, altitude/height/speed are shown unconverted behind
  a ⚠ badge rather than guessed at. Seeing that on real traffic means it's
  worth pinning down which encoding your decoder build emits.
- **Renaming uses a plain `prompt()` dialog.**

## Files

- `main.py` — FastAPI backend: ZMQ subscriber, track correlation (serial
  first, MAC fallback), flight lifecycle, Discord alerts, REST + WebSocket.
- `db.py` — SQLite layer. Four tables: `drones` (catalog and nicknames),
  `flights` (one row per detection session), `track_points` (timestamped
  samples), `app_settings` (the station location).
- `static/index.html` — the entire UI.
- `static/icons/` — tab and bookmark icons, built by `build-icons.py` from the
  same delta the map draws. Three marks are defined: `a` (bare dart), `b`
  (dart in a ring), `c` (dart in reticle corners — shipped). `python3
  build-icons.py a` rebuilds the set from another one; filenames don't change,
  so `index.html` needs no edit. Rasterizing needs Playwright's Chromium;
  without it the SVGs are still written and the PNG/ICO step is skipped.
- `.env.example` — template for `.env`.