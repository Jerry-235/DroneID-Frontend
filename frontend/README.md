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
  restart of this app does **not** lose flight history (only the live
  in-memory "currently open flight" bookkeeping resets — a drone
  reappearing after an app restart opens as a new flight row, since a
  restart is indistinguishable from a real signal gap from the app's point
  of view; worth knowing about).

## What's new in this pass

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

## New API surface

- `GET  /api/flights?limit=&offset=` — flight list, most recent first.
- `GET  /api/flights/{id}` — one flight's metadata + full point list.
- `GET  /api/drones/catalog` — all catalog_key → nickname mappings.
- `PATCH /api/drones/{catalog_key}/name` — body `{"nickname": "Drone 1"}`.
  Pass `{"nickname": ""}` to clear a name back to serial/MAC.

## Known limitations / next things to tighten up

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
- **No auth** — fine for a single-operator LAN box; put a reverse proxy
  with basic auth in front of it before exposing it beyond your own
  network.
- **Discord webhook alerts** — still not built; that's the next piece.

## Files

- `main.py` (or `frontend.py`, per your rename) — FastAPI backend: ZMQ
  subscriber, Approach A track correlation, flight lifecycle, REST +
  WebSocket API.
- `db.py` — SQLite persistence layer.
- `static/index.html` — the map UI (Leaflet, vanilla JS, no build step).
