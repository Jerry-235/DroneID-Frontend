# DroneID Live Map — frontend

A live web UI for an existing [bkerler/DroneID](https://github.com/bkerler/DroneID)
setup. This first slice covers the **live map only** — history storage and
Discord alerts come next.

## How it connects to your existing backend

You already have this running:

```
./bluetooth_receiver.py --zmqsetting 127.0.0.1:4222 ...
./wifi_receiver.py --interface wlan0 -z --zmqsetting 127.0.0.1:4223
./zmq_decoder.py -z --zmqsetting 127.0.0.1:4224 --zmqclients 127.0.0.1:4222,127.0.0.1:4223 -v
```

This app subscribes as a ZMQ SUB client to the **decoder's** output —
`tcp://127.0.0.1:4224` by default — the same decoded stream you see printed
in the `zmq_decoder.py -v` terminal. It does not touch the sniffers directly.

If your decoder is on a different host/port, override it:

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

## What it does right now

- Subscribes to the decoder's ZMQ stream, parses Basic ID / Location-Vector /
  System messages.
- Correlates messages into per-drone tracks keyed by UAS serial number
  (falling back to MAC until a serial is seen) — see the design notes in the
  chat this was built from for why.
- Pushes live updates to the browser over a WebSocket; renders drone
  positions as heading-rotated icons and operator/ground-station positions
  as a separate marker, linked with a dashed line.
- Sidebar lists all currently-tracked drones with live/stale status, sorted
  by most recently seen.
- Handles the Remote ID "Unknown"/"Undefined"/heading-361 sentinel values
  correctly instead of treating them as real data.

## What's intentionally not here yet

- **Persistence** — everything is in-memory; a restart clears all tracks.
  That's next: SQLite for per-flight history + a playback/scrub UI.
- **Discord webhook alerts.**
- **Nicknaming/cataloging UI** — the data model has `nickname`/`notes`
  fields already, just no editing UI yet.
- **Auth** — fine for a single-operator LAN box; add something in front of
  it (e.g. a reverse proxy with basic auth) before exposing it beyond your
  own network.

## Files

- `main.py` — FastAPI backend: ZMQ subscriber, track correlation, REST +
  WebSocket API.
- `static/index.html` — the map UI (Leaflet, vanilla JS, no build step).
