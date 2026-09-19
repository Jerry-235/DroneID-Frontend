# DroneID Web Frontend

A live map + historical playback frontend for an existing
[bkerler/DroneID](https://github.com/bkerler/DroneID) setup.

## How it connects to your existing backend

You already have this running:

```
./bluetooth_receiver.py --zmqsetting 127.0.0.1:4222 ...
./wifi_receiver.py --interface wlan0 -z --zmqsetting 127.0.0.1:4223
./zmq_decoder.py -z --zmqsetting 127.0.0.1:4224 --zmqclients 127.0.0.1:4222,127.0.0.1:4223 -v
```

This app subscribes as a ZMQ SUB client to the **decoder's** output —
`tcp://127.0.0.1:4224` by default, the same decoded stream you see printed
in the `zmq_decoder.py -v` terminal. Override with:

```bash
export DRONEID_ZMQ_ADDR=tcp://127.0.0.1:4224
```

The app also connects to the two raw sniffer ports directly — `tcp://127.0.0.1:4222`
(bluetooth_receiver.py) and `tcp://127.0.0.1:4223` (wifi_receiver.py) — purely
to show health/liveness for each in the top bar (ZMQ / Bluetooth / WiFi
indicators). It doesn't parse anything from these two; all actual drone data
still comes from the decoder's unified output above. Override if needed:

```bash
export DRONEID_BT_ZMQ_ADDR=tcp://127.0.0.1:4222
export DRONEID_WIFI_ZMQ_ADDR=tcp://127.0.0.1:4223
export DRONEID_HEALTH_TIMEOUT_S=12   # seconds of silence before green -> yellow
```

Each of the three health dots is one of three states:
- **Red** — not connected. For Bluetooth/WiFi this means that sniffer process
  appears to be down or unreachable (detected via ZMQ's socket-monitor
  connect/disconnect events, not just "have we heard anything" — so it can
  tell "dead" apart from "alive but quiet"). For ZMQ, same idea against the
  decoder.
- **Yellow** (Bluetooth/WiFi only) — connected, but no message in the last
  `DRONEID_HEALTH_TIMEOUT_S` seconds. The process is up, just idle (e.g. no
  drones currently in range).
- **Green** — connected and a message arrived recently. ZMQ only ever shows
  green or red (it's your own data pipeline, not a sniffer to merely watch).

## Marker lifecycle, and flights that survive a dropout

A track on the live map goes through three stages, measured in seconds of
silence since its last received packet:

| Silence | Status | On the map |
| --- | --- | --- |
| 0–45s | `live` | Normal identity color |
| 45–105s | `stale` | Icon, operator diamond, pair link and trail all turn **gray**; the marker stays parked at its last known position. Any number of tracks can sit here at once. |
| >105s | `dropped` | Removed from the map, flight closed out in the DB |

So: 45 seconds to go gray, then a further 60 seconds parked before it
disappears — 1m45s of dead air in total. The sidebar row for a stale track
goes gray too and picks up a small `STALE` tag.

```bash
export DRONEID_STALE_AFTER_S=45          # live -> stale (gray)
export DRONEID_DROP_AFTER_S=105          # stale -> dropped (removed)
export DRONEID_FLIGHT_MERGE_WINDOW_S=300 # reappearance window, see below
```

If the same aircraft comes back **within 5 minutes of disappearing from the
map**, it is treated as a continuation of the flight it was already on
rather than a new one:

- History shows **one** flight spanning the gap, not two fragments. The row
  is annotated with e.g. `2 segments` so a merged flight is still visible as
  one for what it is.
- The Discord "new drone detected" alert does **not** re-fire. It only fires
  when a genuinely new flight is opened.

The window is measured from the drop, not from the last packet, so with the
defaults above the real dead-air tolerance is 105s + 300s.

"The same aircraft" is matched loosely — any one of catalog key, MAC,
serial, or the friendly name you assigned — precisely so it survives coming
back under a slightly different identity (a re-randomized MAC, or a serial
that hadn't been decoded yet the first time round). Two side effects worth
knowing about:

- Giving two genuinely different drones the **same friendly name** will
  merge them if they appear within the window of each other. That's the
  behavior as specified; rename one if you don't want it.
- A merged flight's recorded path has a real gap in it. Points carry a
  `segment` number so neither the live trail nor History playback draws a
  straight line across it — you'll see separate strokes instead.

A flight left open by a server restart is picked back up by the same
mechanism, so restarting the app mid-flight no longer strands a half-flight
in History (as long as the drone reappears inside the window).

### Merging and deleting flights by hand (admin view only)

With the admin flag set (see *Admin view* below), each row in the History tab
gets a small checkbox at its bottom right, under the pencil. Tick one or more
and a bar appears at the top of the list with **Merge**, **Delete** and
**Clear**. Both actions ask for confirmation first, and neither can be undone.

**Merge** folds the selected flights into the oldest one: all their points
move across, the time span widens to cover earliest start through latest end,
and the whole thing is renumbered to a single segment so the track draws as
one continuous line, joining the end of each flight straight to the start of
the next in time order. The row is then labelled `merged ×N`.

Merging is refused, with the reason shown in the bar, when:

- fewer than two flights are selected;
- the flights aren't the same aircraft — every one has to agree on a serial,
  or every one has to agree on a MAC (`Cannot merge mismatched info`). Either
  alone is enough, since a MAC can be re-randomized between flights while the
  serial holds, and an early flight may only ever have been seen by MAC. A
  flight carrying neither can't be matched to anything, so it never merges;
- a flight is still in progress — a live track is writing points into it, so
  it has to close out first.

Two consequences worth knowing:

- A manual merge **flattens any automatic segment gaps** inside the flights
  it touches, because you've just asserted they're one continuous flight.
  If those flights had real dropouts, the line will now run straight through
  them.
- The merged row keeps the **oldest** flight's catalog key, so if you merged
  on MAC across two different serials, it displays under the older one's
  name.

**Delete** removes the selected flights and all their track points. The
`drones` catalog rows are left alone — they hold your nicknames, and an
aircraft with no flights on record is still a valid catalog entry.

**Worth verifying on your machine**: the connect/disconnect detection relies
on pyzmq's socket monitor API (`get_monitor_socket()` + parsing
`EVENT_CONNECTED`/`EVENT_DISCONNECTED`/`EVENT_CONNECT_RETRIED`). I couldn't
test this against a real ZMQ instance in the environment this was built in
— if a health dot stays red even with its sniffer definitely running, check
the app's logs for "Could not attach connection monitor" and let me know;
that would mean this signal isn't available the way I expected on your
pyzmq version and needs a fallback.

## Install & run

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 main.py    # or: uvicorn main:app --host 0.0.0.0 --port 8000
```

Then open `http://<this-machine>:8000/` in a browser.

### On your folder rename / structure

Putting this in its own `frontend/` subfolder inside your forked DroneID repo
is the right call — it stays decoupled from upstream so you can pull DroneID
updates without conflicts. Renaming `main.py` → `frontend.py` is also fine;
just adjust the run command's *module* name to match:

```bash
python3 frontend.py
# or
uvicorn frontend:app --host 0.0.0.0 --port 8000
```

(`uvicorn main:app` becomes `uvicorn frontend:app` because that string is
`<module_name>:<FastAPI_variable>` — the FastAPI variable is still called
`app` inside the file, only the filename/module changed.) Everything else —
imports, static file paths, the SQLite DB path — is relative to the file's
own location, so the rename and the subfolder move don't require any other
changes.

### Expected behavior when you start the full pipeline

Order isn't strict — ZMQ handles a subscriber connecting before a publisher
is bound; it just won't see anything published before the connection
completes. Recommended order:

1. Start `bluetooth_receiver.py` and `wifi_receiver.py`.
2. Start `zmq_decoder.py -v` — confirm you see decoded JSON in its terminal.
3. Start this frontend (`python3 frontend.py`).

On startup you should see a log line:
`Subscribed to DroneID decoder at tcp://127.0.0.1:4224`

Then, in the browser:
- The status dot in the top bar turns from red ("connecting…") to teal
  ("live") once the WebSocket connects — independent of whether any drone
  has been detected yet.
- When a burst arrives, a sidebar row and a map icon appear within about a
  second. If the drone doesn't yet have a GPS fix, the icon won't appear
  (no lat/lon), but the sidebar row will still show it with alt/heading as
  `—` until a fix comes through — this matches real captures like the
  ground-test one you sent where `latitude`/`longitude` were `"Unknown"`.
- A local SQLite file `droneid.db` is created next to `frontend.py` the
  first time a burst is processed. Every detection is now persisted; a
  restart of this app does **not** lose flight history. The live in-memory
  "currently open flight" bookkeeping does reset, but a drone that reappears
  within `DRONEID_FLIGHT_MERGE_WINDOW_S` is reattached to the flight it was
  already on rather than opening a second row — so a restart mid-flight no
  longer strands a half-flight in History.

## What's new in this pass

- **Discord webhook alerts** — fires once per genuinely new drone detection
  (excludes the test drone), with distance-to-station, landed/flying status,
  speed, heading, and whether an operator/controller position was found.
  Configured via the `DRONEID_DISCORD_WEBHOOK` environment variable (see
  below) — not through the UI, since it's a secret and this repo is meant
  to be kept in source control. No new dependency — uses `urllib` from the
  standard library rather than adding `requests`/`httpx`.
- **SQLite persistence** (`db.py`, stdlib `sqlite3`, no extra dependency).
  Three tables: `drones` (catalog/nicknames), `flights` (one row per
  detection session), `track_points` (timestamped samples per flight).
- **Live flight trail** — as a tracked drone moves, its path draws on the
  map in real time. Reloading the page mid-flight backfills the trail from
  the DB instead of starting blank.
- **Renaming, with retroactive relabeling** — click the ✎ pencil next to
  any drone's name (live sidebar row, live map popup, or a history list
  row) to assign a name like "Drone 1". The name is stored once per
  catalog key (serial, or MAC if no serial has ever been seen) and joined
  in at display time — so every past flight for that drone, and every
  future one, shows the new name immediately. No data is duplicated or
  needs updating retroactively.
- **History tab** — lists all recorded flights, most recent first, with
  date/time, duration, and point count. Click one to draw its full path
  and open the scrub bar.
- **Scrub playback** — drag the slider (or hit ▶) to move a marker along a
  historical flight's path, with a live readout of altitude/heading/speed
  and timestamp at that point.
- **Click a live drone → see its path so far** — the live trail described
  above; selecting a drone (row or marker) highlights its trail and dims
  others.

## Discord webhook setup

1. In the target Discord channel: *Edit Channel → Integrations → Webhooks →
   New Webhook*, then click "Copy Webhook URL".
2. Give it to the app one of two ways:
   - **`.env` file (recommended)** — copy `.env.example` to `.env` (same
     folder as `frontend.py`) and fill in the real URL:
     ```bash
     cp .env.example .env
     # then edit .env and paste your webhook URL in place of the placeholder
     ```
     `.env` is already in `.gitignore` — it will never get committed.
     `.env.example` has no real secret in it, so it's safe to commit and
     keeps the expected format documented for anyone else who clones this.
   - **Plain environment variable** — skip the file entirely:
     ```bash
     export DRONEID_DISCORD_WEBHOOK="https://discord.com/api/webhooks/.../..."
     ```
     (or set it in your `systemd` unit / process manager). A real
     environment variable always wins over whatever's in `.env` if both
     happen to be set.
3. Restart the server. Settings > Discord Alerts (admin view) will show
   "Configured" and a "Send Test" button once it picks it up.

The webhook URL is read once at startup and never stored in the database —
changing or clearing it means editing `.env` (or the environment variable)
and restarting, same as the other env-configured settings in this app (ZMQ
addresses, timeouts, DB path).

### Pinging a role, and the message format

Alerts are formatted as a Discord `##` heading line (bold, slightly larger)
followed by the details in a fenced code block on their own line:

```
## Jerry Mini 5 Detected @drone-alerts
26,614ft From Station - Currently flying at 32mph and Heading 122° - Controller Located
```

(the second line renders as a monospaced code box in Discord; shown plain
above for readability here).

To have it actually ping a role — plain `@role-name` text in a message
never notifies anyone, Discord only pings on the `<@&ROLE_ID>` mention
syntax — set `DRONEID_DISCORD_ROLE_ID` to that role's numeric ID:

1. In Discord: *User Settings → Advanced → Developer Mode* (turn it on, if
   not already).
2. *Server Settings → Roles* → right-click the role → **Copy Role ID**.
3. Add it to `.env` (or export it) alongside the webhook URL:
   ```bash
   DRONEID_DISCORD_ROLE_ID=123456789012345678
   ```
4. Restart the server.

Leave it unset for no ping — the heading line just omits the mention.
`allowed_mentions` is explicitly locked down on every outgoing message to
*only* that one role ID, regardless of what's in the message content —
this matters because drone nicknames are user-editable text that ends up
directly in the alert, so without this restriction, naming a drone
something like "@everyone" would actually ping your whole server.

## Admin view (casual-viewer declutter, not real access control)

Settings > Station, Discord Alerts, and Debug are hidden by default. Visit
the app once with `?admin=1` on the URL (e.g. `http://<host>:8000/?admin=1`)
to unlock them permanently for that browser — the flag is stored in that
browser's `localStorage` and the param is stripped from the URL right after,
so it won't linger visibly or get shared via a bookmark. Every other
browser/device just sees the field toggles and Units, with no indication
those other sections exist.

This is purely a client-side UI convenience, **not security** — anyone who
opens dev tools can set the same flag themselves, and every backend endpoint
those sections talk to (station, Discord test-send, test drone) has no
server-side auth at all regardless of this flag. Treat it as "declutter the
view for casual observers on my LAN," not as a way to prevent someone
determined from reaching those controls.

## New API surface

- `GET /api/discord_webhook` — `{"configured": true|false}` only; never
  returns the actual URL (it isn't stored anywhere the app manages — see
  above).
- `POST /api/discord_webhook/test` — sends a one-line test message so you
  can confirm it's wired up without waiting for a real detection.
- `POST /api/test/drone/start` / `POST /api/test/drone/stop` / `GET /api/test/drone/status`
  — a synthetic orbiting test drone (with an operator position), fed through
  the exact same apply_burst/persist_update/broadcast pipeline as real ZMQ
  data. Toggle it from Settings > Debug. Useful for exercising the live map,
  popups, path trail, and history without RF hardware. Stopping it doesn't
  force-remove it — it decays through the normal stale/drop timeouts like a
  real signal loss would, so that lifecycle gets exercised too.
- `GET  /api/health` — `{"health": {"zmq": "green"|"red", "bluetooth": "green"|"yellow"|"red", "wifi": "green"|"yellow"|"red"}}`,
  same data the top bar's ZMQ/Bluetooth/WiFi dots use.
- `GET  /api/flights?limit=&offset=` — flight list, most recent first.
- `GET  /api/flights/{id}` — one flight's metadata + full point list.
- `POST /api/flights/merge` — body `{"flight_ids": [1,2,3]}`. Folds them into
  the oldest; returns `{"flight_id": <survivor>, "merged": <n>}`. 400 on fewer
  than two, 404 on an unknown id, 409 on mismatched identity or an
  in-progress flight.
- `POST /api/flights/delete` — body `{"flight_ids": [1,2]}`. Returns
  `{"deleted": <n>}`. 409 on an in-progress flight.
- `GET  /api/drones/catalog` — all catalog_key → nickname mappings.
- `PATCH /api/drones/{catalog_key}/name` — body `{"nickname": "Drone 1"}`.
  Pass `{"nickname": ""}` to clear a name back to serial/MAC.

## Known limitations / next things to tighten up

- **No auth on any endpoint** — the admin-view flag above is UI-only. In
  particular, `POST /api/discord_webhook/test`, the test-drone endpoints and
  now `POST /api/flights/merge` and `/api/flights/delete` can be triggered by
  anyone who can reach this server at all, admin flag or not. The merge and
  delete endpoints are **destructive and irreversible**, which raises the
  stakes on this: fine for a private LAN box, but put a reverse proxy with
  basic auth in front of it before exposing this beyond your own network.
- **No undo on merge or delete.** There's no trash and no split — a merged
  flight can't be pulled back apart, and a deleted one is gone. If you expect
  to want either, copy `droneid.db` before a big tidy-up session.
- **Every burst is written as a DB point** — fine at "a few drones, single
  operator" scale (your stated scale), but if you ever run this against
  much heavier traffic, batch or debounce the writes.
- **Altitude/height/speed "raw units" warning** — if a burst arrives in the
  raw/scaled coordinate shape (like your "ideal" test payload) rather than
  the pre-decoded shape (like your real captures), the UI shows a ⚠ badge
  rather than a guessed number. If you start seeing that badge on real
  traffic, it's worth pinning down which encoding your decoder build
  actually emits so we can convert it properly instead of flagging it.
- **Renaming uses a plain `prompt()` dialog** for now — functional, not
  polished; an inline edit box would be a nice small upgrade later.
- **Friendly-name matching in flight merging is exact and case-sensitive**,
  and it is the one matching arm that can join two genuinely different
  aircraft. See the merge section above; it's deliberate, but it's the arm
  most likely to surprise you.
- **Merging can't un-merge.** Once two segments are recorded on one flight
  row there's no UI to split them apart again, so if the window turns out to
  be too generous for your airspace, lower
  `DRONEID_FLIGHT_MERGE_WINDOW_S` rather than fixing it up afterwards.

## Files

- `main.py` (or `frontend.py`, per your rename) — FastAPI backend: ZMQ
  subscriber, Approach A track correlation, flight lifecycle, REST +
  WebSocket API.
- `db.py` — SQLite persistence layer.
- `static/index.html` — the map UI (Leaflet, vanilla JS, no build step).