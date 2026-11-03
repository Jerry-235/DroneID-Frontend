# DroneID web frontend

A live map and historical playback UI for the DroneID receiver pipeline in
the parent directory. It subscribes to `zmq_decoder.py`'s output, records every
detection to SQLite, and serves a single-page map with flight history and
Discord alerts.

FastAPI and stdlib `sqlite3` on the backend, Leaflet and vanilla JavaScript on
the front. No build step.

## Install and run

From the repo root, after `./setup.sh` (see the top-level README):

```bash
.venv/bin/pip install -r frontend/requirements.txt
cd frontend
../.venv/bin/python main.py
```

Open `http://<this-machine>:8000/`. `droneid.db` is created next to `main.py`
on first use.

Start the receivers and the decoder first (see the top-level README). The
server log should read `Subscribed to DroneID decoder at tcp://127.0.0.1:4224`,
and the top-bar dot goes from red to green as soon as the browser's WebSocket
connects, before any drone is detected. A drone with no GPS fix yet gets a
sidebar row but no map icon.

## Configuration

Set environment variables, or put them in a `.env` file next to `main.py`.
Copy `.env.example` to start; `.env` is gitignored. Real environment variables
win over `.env`. Everything is read at startup, so changes need a restart.

| Variable | Default | What it does |
| --- | --- | --- |
| `DRONEID_ZMQ_ADDR` | `tcp://127.0.0.1:4224` | The decoder's output. The only source of drone data |
| `DRONEID_BT_ZMQ_ADDR` | `tcp://127.0.0.1:4222` | Bluetooth receiver. Health monitoring only |
| `DRONEID_WIFI_ZMQ_ADDR` | `tcp://127.0.0.1:4223` | Wi-Fi receiver. Health monitoring only |
| `DRONEID_HEALTH_TIMEOUT_S` | `12` | Silence before a sniffer goes from green to yellow |
| `DRONEID_STALE_AFTER_S` | `45` | Silence before a track goes gray |
| `DRONEID_DROP_AFTER_S` | `105` | Silence before a track leaves the map |
| `DRONEID_FLIGHT_MERGE_WINDOW_S` | `300` | How long after a drop the same aircraft continues its flight |
| `DRONEID_SOURCE_RECENT_S` | `15` | How recently a radio must have heard a drone to show as a source |
| `DRONEID_BROADCAST_TIMEOUT_S` | `5` | Send timeout for a WebSocket client that has stopped reading |
| `DRONEID_NODES_PATH` | `nodes.json` beside `main.py` | Remote stations file (see `MULTI-NODE.md`) |
| `DRONEID_NODE_STATUS_STALE_S` | `15` | Silence from a station's `node_agent` before its report counts as old (its map icon's Wi-Fi and Bluetooth lights go red) |
| `DRONEID_ADMIN_NETS` | Tailscale ranges and loopback | Networks allowed to use admin-only endpoints |
| `DRONEID_MASTER_GPSD` | off | `host:port` of a gpsd that reads a GPS receiver on this machine, e.g. `127.0.0.1:2947` |
| `DRONEID_TRUST_FORWARDED_FOR` | off | Read the client address from `X-Forwarded-For`. Only behind a proxy that overwrites that header |
| `DRONEID_DISCORD_WEBHOOK` | none | Enables alerts (see *Discord alerts*) |
| `DRONEID_DISCORD_ROLE_ID` | none | Numeric role ID to ping on each alert |
| `DRONEID_HEALTH_WEBHOOK` | none | Enables station health alerts, to their own channel (see *Health alerts*) |
| `DRONEID_HEALTH_ALERT_AFTER_S` | `60` | How long a component must stay down before it's reported |
| `DRONEID_HEALTH_STARTUP_GRACE_S` | `120` | Quiet period after start-up while every link connects |
| `DRONEID_LANDMARKS_PATH` | `landmarks.csv` beside `places.py` | Optional local place names. See *Place names* |
| `DRONEID_LANDMARK_RADIUS_M` | `150` | How close a point must be to a landmark to take its name |
| `DRONEID_NOMINATIM_URL` | OpenStreetMap's public server | Reverse geocoder used when no landmark matches |
| `DRONEID_GEOCODER_CONTACT` | none | Contact address sent with geocoder requests, as their usage policy asks |
| `DRONEID_PLACE_TIMEOUT_S` | `2` | How long a new flight waits for its place name before it stays unnamed |
| `DRONEID_DB_PATH` | `droneid.db` beside `main.py` | SQLite file |
| `HOST` / `PORT` | `0.0.0.0` / `8000` | Where to listen |

### Sniffer health

The two sniffer ports are subscribed to for liveness only. Nothing is parsed
from them. The top bar shows one chip per station: the master first, named
after the station, then each remote node. A chip is green while the master's
ZMQ connection to that station's decoder is up, and red when it isn't. Green
needs the ZMQ handshake to complete, so a bare TCP connect never flashes it
green. A quiet decoder, with no drones in range, stays green.

Every connection from the master to a station sends ZMQ heartbeats (every 2
seconds, 6-second timeout). A link that died without closing, such as a power
cut or a pulled cable, goes red within about 8 seconds, and the master
reconnects on its own as soon as the station is back. Each change is logged
once, for example `node:node-2 link down` and then `node:node-2 link up`.

Wi-Fi and Bluetooth are shown on the master's map icon and on each node's icon
instead. They're separate from the top-bar chip:

- **green:** that sniffer is sending.
- **yellow:** connected to the sniffer but quiet for `DRONEID_HEALTH_TIMEOUT_S`.
- **red:** not connected, or (for a remote node) its `node_agent` hasn't
  reported for `DRONEID_NODE_STATUS_STALE_S`. Connection comes from ZMQ's
  socket-monitor events, not from silence, so a dead process reads differently
  from a quiet one. If a sniffer stays red while it's running, check the log
  for `Could not attach a connection monitor`. That means your pyzmq build
  lacks the monitor API.

## Marker lifecycle

Measured in seconds of silence since the last packet:

| Silence | Status | On the map |
| --- | --- | --- |
| 0–45s | `live` | Normal identity colour |
| 45–105s | `stale` | Icon, operator diamond, link and trail all go **gray**. The marker stays parked at its last known position. Any number of tracks can sit here at once. |
| >105s | `dropped` | Removed from the map with its trail. The flight is closed out in the database |

So a drone goes gray after 45s and leaves after 105s, which is 1m45s of dead
air in total. A stale sidebar row grays out and picks up a `STALE` tag.

## Flights that survive a dropout

If the same aircraft comes back within `DRONEID_FLIGHT_MERGE_WINDOW_S` of
disappearing, it continues the flight it was already on instead of starting a
new one. History shows one flight spanning the gap, labelled for example
`2 segments`, and the Discord "new drone" alert doesn't fire again. The window
is measured from the drop, not the last packet, so the real dead-air tolerance
is `DRONEID_DROP_AFTER_S` plus the window. A flight left open by a server
restart is picked up the same way.

"The same aircraft" matches on catalog key, MAC, serial or the friendly name
you assigned. The match is deliberately loose, so it survives a re-randomized
MAC or a serial that hadn't decoded on the first pass. Two things follow:

- A **name** only counts when the serials don't contradict it. Two drones you
  gave the same name stay separate flights, and both alert, as long as both
  serials are known and differ. Two un-serialled drones sharing a name could
  still merge. The match is exact and case-sensitive.
- A merged flight's path has a real gap. Points carry a `segment` number, so
  neither the live trail nor playback draws a line across it.

## Using it

**Live** lists current tracks, most recent first, with altitude, speed and
heading pills and an operator-detected indicator. The ✎ pencil renames a
drone (admin only). The name is stored once per catalog key (serial, or MAC if no serial has
been seen) and joined in at display time, so every past and future flight
relabels immediately. A copy button sits at the end of the title, but only when
the title is the aircraft's own serial or MAC, not a name you typed.

**History** lists recorded flights, most recent first. Each row shows the date,
the duration, the point count, and the place where the flight started, followed
by `N segments` or `merged ×N` where they apply. Click a row to draw its path and
open the scrub bar. Drag the slider or press ▶ to walk a marker along the path.
The operator's own trail is solid purple, bold while you're tracking it. The
only dashed purple on the map is the straight drone-to-operator link.

**The detail panel** opens when you select a drone or operator, from the list,
the map or a focus button.

- *Desktop:* a 340px column on the right that narrows the map rather than
  covering it.
- *Mobile (700px and narrower):* a full-width lower-third sheet. Tap the grab
  handle to expand it to about three-quarters. Selecting something also collapses
  the list, so the map gets the width. Tap the tab to reopen it.

Data sits in one scroll under three bubbles: **BASIC**, **AIRCRAFT** and
**OPERATOR**. Each stays pinned while its section scrolls past. In Live all
three take the drone's colour. History uses a blue Basic bubble with the
drone's orange and the operator's purple. Every row follows the Settings
toggles, and changing a toggle or the unit system updates the panel immediately.

The panel's two buttons pan to their unit and leave the zoom alone. They read
**Track Drone / Track Operator** on the live map and **Focus Drone / Focus
Operator** in History, where they pan to the unit's position at the current
scrub point. Close the panel with **×** or by tapping empty map. In Live that
clears the selection. In History the flight stays loaded.

### Field order

The BASIC section, in order:

1. Protocol Version
2. MAC
3. Radio (`Wi-Fi` or `Bluetooth`, the radio that heard the latest detection)
4. Channel (the Wi-Fi channel, or the BLE channel index for Bluetooth)
5. RSSI (dBm)
6. Status
7. Last Seen
8. Tracked Time
9. Tracked By
10. Stations (expands to each station's distance and whether it detected the drone)

The AIRCRAFT section starts with Serial Number, UA Type and ID, followed by
Op Status, Height Type, Direction, speeds, position, altitudes, accuracies and
distances.

Each row can be toggled in **Settings**. The defaults are in `static/index.html`
under `FIELD_DEFAULTS`.

**Settings** holds the per-field toggles, the metric or imperial switch and, in
admin view, Station, Discord Alerts and Debug. Station is stored server-side, so
it's the same on every device. Debug starts a synthetic orbiting test drone
through the real ingest pipeline. Stopping it lets it decay through the normal
stale and drop timeouts rather than vanishing.

## Flight place names

Each flight is named with the place where it was first seen. That's the same
name the Discord alert uses, and it's looked up once.

- **New flights:** when a flight starts with a GPS fix, the server looks up that
  fix's name and stores it on the flight row. The Discord alert uses the same
  lookup, so each new flight costs one lookup, not two. A flight that starts
  without a fix stays unnamed until the backfill script runs.
- **Stored:** the name stays in `droneid.db` (the `flights.location` column).
  History reads it from there and never calls the geocoder.
- **No GPS fix at all:** the backfill script marks such a flight with an empty
  name, so it isn't checked again.
- **Lookup order:** a landmark in `landmarks.csv` wins if it covers the point.
  Otherwise OpenStreetMap is asked, at one request per second. Results are
  cached in memory by roughly 100 m cell, so only new locations reach the
  geocoder.

### Place names: landmarks.csv

`landmarks.csv` is optional and local. It's gitignored, so it never goes into
the repo. Columns: `name`, `lat`, `lon`, and optionally `radius_m`. A row with no
radius uses `DRONEID_LANDMARK_RADIUS_M`. A bad row is skipped with a warning, so
one typo doesn't drop the whole file. If the file was saved from a spreadsheet
as "CSV UTF-8", that's fine.

When a point falls inside more than one landmark, the nearest one wins. A
landmark name always beats an OpenStreetMap name.

To change a landmark name after flights were recorded, re-apply the file to
History. See *Backfill* below.

### Backfill

Flights recorded before place names existed have no name. Run this once, with
the server stopped, from the `frontend` folder:

```bash
../.venv/bin/python backfill_flight_locations.py
```

It names each unnamed flight from its first GPS fix, one lookup at a time, at the
geocoder's rate limit. Each name is saved as it's found, so Ctrl-C is safe. A
re-run only tries flights that are still unnamed.

After you edit `landmarks.csv`, re-apply the names with:

```bash
../.venv/bin/python backfill_flight_locations.py --landmarks
```

This reads the current file and renames any flight whose first fix falls inside
a landmark. It makes no network calls, so it finishes in seconds. Flights outside
every landmark keep their existing names. Deleting a landmark doesn't clear the
names it gave, because those names are stored.

Restart the server after either command, so new flights use the updated file.

## Master GPS

The master can take its own position from a GPS receiver plugged into the
machine, such as a USB GPS stick. The receiver is read through gpsd:

```bash
sudo apt install gpsd gpsd-clients
ls /dev/ttyACM* /dev/ttyUSB*                       # the stick's serial device
sudo gpsd /dev/ttyACM0 -F /var/run/gpsd.sock -n   # use the device you found
cgps -s                                            # wait for a 3D fix
DRONEID_MASTER_GPSD=127.0.0.1:2947 .venv/bin/python main.py
```

Then set the master to GPS on the Station settings page. Its icon follows the
fix, and distances use it. Switching back to Manual returns it to the saved home
point. Remote nodes already work the same way, through their own node_agent.

GPS state shows on each icon's hover card and on the station's top-bar name:

| Time without a fix | State | Icon |
|---|---|---|
| under 2 min | Live | Drawn at the live fix |
| 2 to 5 min | Stale | Drawn at its last fix, faded |
| 5 min, or never had one | Disconnected | Drawn at its last fix, greyed, only while the station is still connected (ZMQ up and at least one sniffer running). Otherwise it is taken off the map |

## Admin view

The admin-only endpoints (drone rename, station location, Discord and health-alert tests, debug drone,
flight merge and delete) are refused unless the request comes from a network in
`DRONEID_ADMIN_NETS`. The default is the Tailscale address ranges plus loopback,
so anyone on your tailnet is an admin and anyone else isn't. `GET /api/access`
reports what the server allows for the caller, along with the address it sees.
That's the quickest way to check why a client is refused.

The page hides the admin sections for non-admin clients. That's only tidiness.
The server's refusal is what counts, and a refused request gets `403`.

If the app sits behind a reverse proxy, the server only sees the proxy's address
unless `DRONEID_TRUST_FORWARDED_FOR` is set. Only set it when the proxy overwrites
`X-Forwarded-For`, because otherwise a client can claim any address.

### Merging and deleting flights

Each History row has a checkbox at the right-hand end of its stats line. Tick
one or more, and an action bar slides up at the bottom of the panel with
**Merge**, **Delete** and **Clear**. Both actions confirm first. Neither can be
undone.

**Merge** folds the selection into the oldest flight. Points move across, the
span widens to cover the earliest start through the latest end, and everything
is renumbered to one segment, so the track draws as one continuous line. The row
is labelled `merged ×N`. Merge is refused, with the reason shown in the bar, when
fewer than two are selected, when a flight is still in progress, or when they
aren't provably the same aircraft. Every one must agree on a serial, or every one
on a MAC (`Cannot merge mismatched info`). A shared name is never enough.

A manual merge flattens automatic segment gaps inside the flights it touches, and
the survivor keeps the oldest flight's catalog key.

**Delete** removes the flights and their points. Catalog rows are kept, because
they hold your nicknames, and an aircraft with no flights on record is still a
valid entry.

## Discord alerts

One message per genuinely new detection. The test drone is excluded. The message
gives the distance from the nearest station, whether the drone is landed or
flying, speed, heading and whether an operator position was found:

```
## Drone-1 Detected at Station A (example)
26,614ft From Station - Currently flying at 32mph and Heading 122° - Controller Located
```

The second line renders as a code block in Discord. The place name after "at" is
the flight's stored location, the same one History shows.

Setup:

1. In Discord, open the channel's *Edit Channel > Integrations > Webhooks > New
   Webhook* and copy the URL into `DRONEID_DISCORD_WEBHOOK`.
2. For a role ping, set `DRONEID_DISCORD_ROLE_ID` to the role's **numeric ID**.
   Plain `@role-name` text never notifies anyone. Turn on *User Settings >
   Advanced > Developer Mode*, then in *Server Settings > Roles*, right-click the
   role and choose **Copy Role ID**. Leave it unset for no ping.
3. Restart. Settings > Discord Alerts shows "Configured" and a Send Test button.

The URL is never stored in the database and never returned by the API. It's
deployment config, not app state. `allowed_mentions` is locked to that one role
ID on every message, whatever the content. Drone nicknames are user-editable and
land directly in the alert, so without that lock, naming a drone `@everyone`
would ping the whole server.

## Health alerts

Station health goes to a second Discord channel, through its own webhook, so it
never mixes with drone alerts or delays them. Health alerts never ping a role.

What's watched, using the same state the web page shows:

| Station | Components |
|---|---|
| Master | decoder, Wi-Fi sniffer, Bluetooth sniffer |
| Remote node | decoder, agent (`node_agent`), Wi-Fi sniffer, Bluetooth sniffer |

A remote node's sniffer states only reach the master through its agent, so while
the agent is silent they're unknown rather than down. Yellow (connected but
quiet) never alerts. GPS is not watched.

When messages are sent:

- **Down:** once a component has been down for `DRONEID_HEALTH_ALERT_AFTER_S`
  (60 seconds), one message covers everything on that station that's down at
  that moment. A blip or a service restart stays quiet.
- **Back:** once a reported component has been back for 10 seconds, one message
  covers everything on that station that's back, and names anything still down.
  A part that comes back later gets its own message.
- **Once per outage:** nothing repeats while a component stays down.
- **Start-up:** nothing is sent for `DRONEID_HEALTH_STARTUP_GRACE_S` (2 minutes)
  after `main.py` starts. Anything still down after that is reported.

Examples:

```
## ❌ Node 2 Offline
Decoder and agent not reporting

## ⚠️ Node 1 Wi-Fi Sniffer Down
Wi-Fi sniffer not reporting - Decoder, agent and Bluetooth sniffer still up

## ✅ Node 2 Online
Decoder online - Agent offline

## ✅ Node 2 Agent Online
All components reporting
```

❌ means the whole station is down, ⚠️ that part of it is, and ✅ that something
came back. As with drone alerts, the second line renders as a code block.

Setup: create a webhook in the health channel the same way as for drone alerts,
put its URL in `DRONEID_HEALTH_WEBHOOK`, and restart. Settings > Health Alerts
shows "Configured" and a Send Test button. The master can't report its own
outage: if `main.py` itself stops, nothing is sent.

## API

Responses over 1 KB are gzipped. A long flight's point list shrinks about 20×.

| Endpoint | Purpose |
| --- | --- |
| `GET /api/drones` | Snapshot of all current tracks |
| `WS /ws` | Snapshot on connect, then live `update`, `status`, `dropped`, `health` and `stations` pushes |
| `GET /api/health` | `{"health": {"zmq": "green\|red", "bluetooth": "green\|yellow\|red", "wifi": ...}}` |
| `GET /api/stations` | Every station with its position, link state and sniffer states |
| `GET /api/access` | `{"admin": true\|false, "client": "<address the server sees>"}` |
| `GET /api/flights?limit=&offset=` | Flights, most recent first, each with its `location`. `limit` caps at 500 |
| `GET /api/flights/{id}` | One flight's metadata and its full point list |
| `POST /api/flights/merge` | `{"flight_ids": [1,2]}` returns `{"flight_id": <survivor>, "merged": n}`. 400 on fewer than two, 404 for an unknown id, 409 for a mismatch or an in-progress flight |
| `POST /api/flights/delete` | `{"flight_ids": [1,2]}` returns `{"deleted": n}`. 409 if one is in progress |
| `GET /api/drones/catalog` | All catalog-key-to-nickname mappings |
| `PATCH /api/drones/{catalog_key}/name` | `{"nickname": "Drone 1"}`. An empty string clears it back to serial or MAC. Admin-only |
| `GET /api/station` | The home point, `{"lat": , "lon": }`. Set it through `station-config` |
| `PUT /api/station-config/{station_id}` | A station's name, location mode, manual position and map icon (`ball`, `ring`, `hex`, `radio`, `car`). Admin-only |
| `GET /api/discord_webhook` | `{"configured": true\|false}`. Never the URL |
| `POST /api/discord_webhook/test` | Sends a test alert, including the role-ping path |
| `GET /api/health_webhook` | `{"configured": true\|false}`. Never the URL |
| `POST /api/health_webhook/test` | Sends a test health alert to the health channel. Admin-only |
| `POST /api/test/drone/start`, `/stop`, `GET /status` | The synthetic test drone |

## Database

`droneid.db` has these tables:

- `drones`: the catalog, with each drone's nickname and protocol version.
- `flights`: one row per detection session. Includes `location`, the place name
  of the first fix (NULL until named, empty if the flight had no GPS fix).
- `track_points`: one row per received broadcast. Includes the station that heard
  it, its radio (`transport`), its Wi-Fi channel and RSSI (`channel`, `rssi`), and
  its `segment`.
- `app_settings`: the station's home point.

Columns are added automatically at startup when they're missing, so an older
database upgrades in place.

## Limitations

- **Network-based access only.** There are no user accounts. Admin-only endpoints
  check the caller's network, so anyone who can reach the server from an admin
  network can merge or delete flights. Keep the server on a private network such
  as a tailnet, and put a reverse proxy with authentication in front of it before
  exposing it further.
- **No undo, no un-merge, no split.** Copy `droneid.db` before a big tidy-up. If
  the merge window is too generous for your airspace, lower
  `DRONEID_FLIGHT_MERGE_WINDOW_S` rather than fixing it afterwards.
- **Every broadcast is a database row.** That's roughly 20 MB a month with a few
  drones. It's fine for one operator. Batch the writes for heavier traffic.
- **Raw-units warning.** If a burst arrives in the raw coordinate shape rather
  than pre-decoded, altitude, height and speed are shown unconverted behind a ⚠
  badge rather than guessed at. If you see that on real traffic, check which
  encoding your decoder build emits.
- **Radio details depend on the decoder.** Radio, channel and RSSI are only
  stored for detections whose decoder output includes them. Detections from an
  older decoder show a dash in those rows.

## Files

- `main.py`: FastAPI backend. ZMQ subscriber, track correlation (serial first,
  MAC fallback), flight lifecycle, Discord alerts, REST and WebSocket.
- `db.py`: SQLite layer. Tables are described under *Database*.
- `places.py`: place names, from `landmarks.csv` or OpenStreetMap.
- `nodes.py`: the stations file, its validation, and the state each station
  reports.
- `node_agent.py`: the position beacon each remote node runs. See `MULTI-NODE.md`.
- `backfill_flight_locations.py`: one-time backfill of flight place names, and
  the `--landmarks` re-apply. See *Backfill*.
- `static/index.html`: the entire UI.
- `MULTI-NODE.md`: setting up remote stations.
- `nodes.example.json`: a template for `nodes.json`.
- `test_nodes.py`: checks for the station and agent code.
- `static/icons/`: tab and bookmark icons, built by `build-icons.py` from the
  same mark the map draws. Three marks are defined: `a` (bare dart), `b` (dart in
  a ring) and `c` (dart in reticle corners, shipped). `python3 build-icons.py a`
  rebuilds the set from another mark. Filenames don't change, so `index.html`
  needs no edit. Rasterizing needs Playwright's Chromium. Without it the SVGs are
  still written, and the PNG and ICO step is skipped.
- `.env.example`: template for `.env`.