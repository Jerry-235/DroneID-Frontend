"""
DroneID web frontend — backend.

Subscribes to the DroneID zmq_decoder.py output (default tcp://127.0.0.1:4224,
per bkerler/DroneID's README: `./zmq_decoder.py --zmqsetting 127.0.0.1:4224
--zmqclients 127.0.0.1:4222,127.0.0.1:4223`), maintains live in-memory track
state keyed by Approach A (UAS serial from Basic ID, falling back to MAC),
persists every sample to SQLite through db.py, and serves:

  live view
  - GET  /api/drones                  snapshot of all known tracks
  - GET  /api/health                  ZMQ/Bluetooth/WiFi feed liveness
  - WS   /ws                          snapshot on connect, then live push

  history
  - GET  /api/flights                 past + in-progress flights
  - GET  /api/flights/{id}            one flight's metadata and full path
  - POST /api/flights/merge           fold several flights into one
  - POST /api/flights/delete          remove flights and their points

  catalog / config
  - GET   /api/drones/catalog         all user-assigned nicknames
  - PATCH /api/drones/{key}/name      rename (applies retroactively)
  - GET/PUT/DELETE /api/station       the station ("home point") location
  - GET  /api/discord_webhook         whether an alert webhook is configured
  - POST /api/discord_webhook/test    send a test alert

  debug
  - POST /api/test/drone/start|stop, GET /api/test/drone/status

  UI
  - GET  /                            the map UI (static/index.html)
  - GET  /favicon.ico                 redirect to the real icon
"""

import asyncio
import json
import logging
import math
import os
import time
import urllib.error
import urllib.request
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, asdict
from typing import Optional

import zmq
import zmq.asyncio
import zmq.utils.monitor
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

try:
    from dotenv import load_dotenv
    # Looked for next to this file specifically (not the process's current
    # working directory), so it's found the same way regardless of where
    # you launch the server from — same convention as the SQLite DB path
    # below. Silently does nothing if the file isn't there; env vars set
    # directly in the shell/systemd/etc. still work exactly as before and
    # take priority over anything in .env (load_dotenv doesn't overwrite
    # variables that are already set).
    load_dotenv(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".env"))
except ImportError:
    pass  # python-dotenv not installed — fine, .env is an optional convenience

import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("droneid-frontend")

ZMQ_ADDR = os.environ.get("DRONEID_ZMQ_ADDR", "tcp://127.0.0.1:4224")
# Raw per-sniffer ports, per bkerler/DroneID's own defaults — subscribed to
# here only for health/liveness monitoring (is each sniffer process actually
# publishing), not for parsing: the decoder's unified output above is still
# the only source used for actual drone data.
BT_ZMQ_ADDR = os.environ.get("DRONEID_BT_ZMQ_ADDR", "tcp://127.0.0.1:4222")
WIFI_ZMQ_ADDR = os.environ.get("DRONEID_WIFI_ZMQ_ADDR", "tcp://127.0.0.1:4223")
# Marker lifecycle, in seconds of silence since the last received packet:
#   0-45s    "live"    — normal identity color
#   45-105s  "stale"   — icon goes gray, marker stays put at its last known
#                        position (any number of tracks can sit here at once)
#   >105s    "dropped" — removed from the map, flight closed out in the DB
# i.e. 45s to go gray, then a further 60s parked before it disappears.
STALE_AFTER_S = float(os.environ.get("DRONEID_STALE_AFTER_S", "45"))
DROP_AFTER_S = float(os.environ.get("DRONEID_DROP_AFTER_S", "105"))
# How long after an aircraft disappears from the map it can come back and
# still count as the same flight rather than a new one. Measured from the
# drop (the moment the icon left the map), not from the last packet — so
# with the defaults above, the real dead-air tolerance is 105s + 300s.
FLIGHT_MERGE_WINDOW_S = float(os.environ.get("DRONEID_FLIGHT_MERGE_WINDOW_S", "300"))
HEALTH_TIMEOUT_S = float(os.environ.get("DRONEID_HEALTH_TIMEOUT_S", "12"))

# Two independent signals per channel:
#  - connection_state: real TCP-level connect/disconnect, from ZMQ's socket
#    monitor — tells us whether the remote process is actually up and its
#    PUB socket reachable, regardless of whether it has anything to say.
#  - last_message_at: when a message was last actually received, used to
#    tell "connected but idle" apart from "connected and actively sending".
CONNECTED = "connected"
DISCONNECTED = "disconnected"
connection_state = {"zmq": DISCONNECTED, "bluetooth": DISCONNECTED, "wifi": DISCONNECTED}
last_message_at = {"zmq": None, "bluetooth": None, "wifi": None}


def compute_health(now: Optional[float] = None) -> dict:
    """Returns {"zmq": ..., "bluetooth": ..., "wifi": ...} each one of
    "green" (connected + actively receiving), "yellow" (connected but no
    message within HEALTH_TIMEOUT_S — process alive, just idle), or "red"
    (not connected — process unreachable/dead). ZMQ never reports yellow:
    it's just connected-and-flowing (green) or not (red), since that channel
    is our own data pipeline rather than a sniffer we're merely watching."""
    now = now if now is not None else time.time()

    def status_for(channel: str, use_freshness: bool) -> str:
        if connection_state.get(channel) != CONNECTED:
            return "red"
        if not use_freshness:
            return "green"
        ts = last_message_at.get(channel)
        if ts is not None and (now - ts) < HEALTH_TIMEOUT_S:
            return "green"
        return "yellow"

    return {
        "zmq": status_for("zmq", use_freshness=False),
        "bluetooth": status_for("bluetooth", use_freshness=True),
        "wifi": status_for("wifi", use_freshness=True),
    }


# Remote ID sentinel values that mean "no data", not a literal reading.
UNKNOWN_STRINGS = {"unknown", "undefined", "invalid"}


def clean_float(value) -> Optional[float]:
    """Convert a Remote ID numeric-ish field to float, or None if it's a
    known sentinel ('Unknown', 'Undefined', or heading 361)."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        return None if v == 361 else v
    s = str(value).strip()
    low = s.lower()
    if any(tok in low for tok in UNKNOWN_STRINGS):
        return None
    # strip trailing units like "0.0 m/s", "9.0 m"
    num = ""
    for ch in s:
        if ch.isdigit() or ch in ".-":
            num += ch
        elif num:
            break
    try:
        v = float(num)
    except ValueError:
        return None
    return None if v == 361 else v


def parse_latlon(value) -> Optional[float]:
    """Handle both shapes seen in the wild:
      - already-decoded decimal degrees (e.g. 28.3917611)
      - raw ASTM F3411 int32 encoding, resolution 1e-7 deg (e.g. 336577004)
    (0, 0) is treated as "no fix" — the conventional null-island sentinel
    most Remote ID stacks emit before GPS lock, not a real position."""
    v = clean_float(value)
    if v is None:
        return None
    if abs(v) > 180:
        v = v / 1e7
    return v


@dataclass
class DroneTrack:
    key: str  # serial if known, else MAC
    mac: Optional[str] = None
    serial: Optional[str] = None
    registration_id: Optional[str] = None  # e.g. CAA-assigned registration, if broadcast alongside serial
    operator_id: Optional[str] = None
    description: Optional[str] = None      # Self ID text, if any
    ua_type: Optional[object] = None       # int or descriptive string, decoder-dependent
    raw_units_warning: bool = False  # set when alt/height/speed arrived in an unrecognized
                                      # raw/scaled shape we chose not to guess-convert

    lat: Optional[float] = None
    lon: Optional[float] = None
    alt: Optional[float] = None       # geodetic altitude, meters
    height_agl: Optional[float] = None
    heading: Optional[float] = None   # degrees, None if unknown (361)
    speed: Optional[float] = None     # m/s

    op_lat: Optional[float] = None    # operator/ground-station position
    op_lon: Optional[float] = None

    # additional Remote ID fields, requested for the field-toggle settings menu
    protocol_version: Optional[str] = None
    vert_speed: Optional[float] = None
    pressure_altitude: Optional[float] = None
    vertical_accuracy: Optional[str] = None
    horizontal_accuracy: Optional[str] = None
    baro_accuracy: Optional[str] = None
    speed_accuracy: Optional[str] = None
    op_status: Optional[str] = None
    height_type: Optional[str] = None
    loc_timestamp: Optional[object] = None  # raw value, string or number depending on source
    op_location_type: Optional[str] = None
    op_classification_type: Optional[str] = None

    nickname: Optional[str] = None
    flight_id: Optional[int] = None  # current open flight row in the DB, if any
    segment: int = 0                 # which detection segment of that flight we're in:
                                     # 0 the first time, +1 each time this flight gets
                                     # resumed after a short disappearance

    first_seen: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)

    def touch(self):
        self.last_seen = time.time()

    def status(self) -> str:
        age = time.time() - self.last_seen
        if age > DROP_AFTER_S:
            return "dropped"
        if age > STALE_AFTER_S:
            return "stale"
        return "live"

    def to_dict(self) -> dict:
        d = asdict(self)
        d["status"] = self.status()
        return d


class TrackStore:
    """Keeps current drone state and the MAC->key mapping used for
    Approach A correlation (serial-first, MAC fallback)."""

    def __init__(self):
        self.tracks: dict[str, DroneTrack] = {}
        self.mac_to_key: dict[str, str] = {}
        # Called as on_rekey(old_key, new_key) when a track that was being
        # followed under a bare MAC gets folded into a serial-keyed one (i.e.
        # its Basic ID finally decoded). Anything else keyed by track key —
        # the open-flight map, connected clients' markers — has to be told,
        # or it keeps referring to a key that no longer exists. Set by the
        # app; left None in tests that only exercise correlation.
        self.on_rekey = None

    def _resolve_key(self, mac: Optional[str], serial: Optional[str]) -> str:
        if serial:
            if mac:
                self.mac_to_key[mac] = serial
            return serial
        if mac:
            # no serial yet this burst — use whatever we already know for this
            # MAC, or fall back to the MAC itself for a brand-new track.
            return self.mac_to_key.get(mac, mac)
        # neither MAC nor serial: nothing to correlate against across bursts.
        # This shouldn't happen with compliant Remote ID traffic (Basic ID is
        # mandatory), so surface it loudly rather than silently losing the plot.
        log.warning("Burst with neither MAC nor serial — cannot correlate across packets")
        return f"anon:{int(time.time()*1000)}"

    @staticmethod
    def _pick_serial(messages: list[dict]) -> tuple[Optional[str], Optional[str]]:
        """A drone can broadcast more than one Basic ID message (e.g. Serial
        Number AND a CAA-assigned Registration ID). Prefer the ANSI/CTA-2063-A
        serial as the primary key; keep any registration ID as a secondary field."""
        serial, registration = None, None
        for msg in messages:
            b = msg.get("Basic ID")
            if not b:
                continue
            id_val = b.get("id") or None
            id_type = (b.get("id_type") or "").lower()
            if not id_val:
                continue
            if "serial" in id_type:
                serial = serial or id_val
            elif "registration" in id_type or "caa" in id_type:
                registration = registration or id_val
            elif serial is None:
                serial = id_val  # unknown id_type, still usable as identity
        return serial, registration

    @staticmethod
    def _get_loc(msg: dict) -> tuple[Optional[dict], bool]:
        """Returns (fields, nested) for a Location/Vector message, or
        (None, False) if this message isn't one. `nested` is True for the
        newer/raw shape that puts the readings under a "coord" sub-object —
        the caller needs to know, because that shape's numbers may be in raw
        F3411 units rather than decoded ones."""
        for key in ("Location/Vector Message", "Location Vector"):
            if key in msg:
                loc = msg[key]
                coord = loc.get("coord")
                if coord:
                    # newer/raw shape nests direction/speed/lat/lon/alt under
                    # "coord" but keeps op_status/height_type/protocol_version
                    # at the outer level — merge so both are visible, with
                    # coord's values winning on any overlapping key.
                    merged = dict(loc)
                    merged.update(coord)
                    return merged, True
                return loc, False
        return None, False

    @staticmethod
    def _get_system(msg: dict) -> Optional[dict]:
        for key in ("System Message", "System"):
            if key in msg:
                return msg[key]
        return None

    @staticmethod
    def _get_self_id(msg: dict) -> Optional[str]:
        """The free-text description a drone broadcasts about itself, under
        whichever key the upstream decoder used. zmq_decoder.py emits
        "Self ID"; dji_receiver.py (AntSDR path) emits "Self-ID Message" and
        is also the one source that puts a real model name in there, so
        missing this key meant silently dropping exactly the most useful
        value of the lot."""
        for key in ("Self ID", "Self-ID Message", "Self-ID", "SelfID"):
            block = msg.get(key)
            if isinstance(block, dict):
                text = block.get("text") or block.get("description") or block.get("Text")
                if text:
                    return str(text)
            elif isinstance(block, str) and block:
                return block
        return None

    def apply_burst(self, mac: Optional[str], messages: list[dict]) -> DroneTrack:
        serial, registration = self._pick_serial(messages)
        key = self._resolve_key(mac, serial)

        track = self.tracks.get(key)
        if track is None:
            # if we just learned the serial for a MAC previously tracked
            # anonymously under its own MAC key, fold that track in.
            old = self.tracks.pop(mac, None) if mac and key != mac else None
            track = old or DroneTrack(key=key, mac=mac)
            self.tracks[key] = track
            if old is not None:
                # The folded-in track still carried its old MAC as .key, which
                # is what everything downstream keys off: the DB catalog row it
                # writes to, the open-flight map, and the marker each connected
                # browser is holding. Left unchanged, this aircraft would keep
                # filing under the weaker key while also appearing on the map
                # twice — once under the MAC (never updated again, and never
                # dropped either, since the sweeper only walks self.tracks) and
                # once under the serial.
                old_key = old.key
                old.key = key
                if old_key != key and self.on_rekey:
                    self.on_rekey(old_key, key)

        if mac:
            track.mac = mac
        if serial:
            track.serial = serial
        if registration:
            track.registration_id = registration

        for msg in messages:
            if "Basic ID" in msg:
                b = msg["Basic ID"]
                if track.ua_type is None and b.get("ua_type") is not None:
                    track.ua_type = b.get("ua_type")
                if b.get("protocol_version"):
                    track.protocol_version = b.get("protocol_version")

            if "Operator ID" in msg:
                op_id = msg["Operator ID"].get("id")
                if op_id:
                    track.operator_id = op_id

            # Separate `if`, not an `elif` on the branch above: some decoders
            # pack more than one message into a single dict, and chaining
            # these meant whichever came second was never looked at.
            self_id = self._get_self_id(msg)
            if self_id:
                track.description = self_id

            loc, loc_nested = self._get_loc(msg)
            if loc:
                lat = parse_latlon(loc.get("latitude"))
                lon = parse_latlon(loc.get("longitude"))
                if lat is not None and lon is not None and not (lat == 0 and lon == 0):
                    track.lat, track.lon = lat, lon

                alt = clean_float(loc.get("geodetic_altitude"))
                hagl = clean_float(loc.get("height_agl"))
                speed = clean_float(loc.get("speed"))
                if loc_nested and isinstance(loc.get("geodetic_altitude"), int):
                    # F3411 alt/speed can use more than one scale/offset scheme
                    # depending on flags; rather than guess, pass the raw value
                    # through and flag it so the UI shows "unverified" instead
                    # of a confidently-wrong number.
                    track.raw_units_warning = True
                if alt is not None:
                    track.alt = alt
                if hagl is not None:
                    track.height_agl = hagl
                if speed is not None:
                    track.speed = speed

                heading = clean_float(loc.get("direction"))
                if heading is not None:
                    track.heading = heading

                vspeed = clean_float(loc.get("vert_speed"))
                if vspeed is not None:
                    track.vert_speed = vspeed
                pa = clean_float(loc.get("pressure_altitude"))
                if pa is not None:
                    track.pressure_altitude = pa
                if loc.get("protocol_version"):
                    track.protocol_version = loc.get("protocol_version")
                # accuracy/status/type fields are categorical (e.g. "<1 m",
                # "Airborne") — pass through as-is, no numeric conversion.
                if loc.get("vertical_accuracy"):
                    track.vertical_accuracy = loc.get("vertical_accuracy")
                if loc.get("horizontal_accuracy"):
                    track.horizontal_accuracy = loc.get("horizontal_accuracy")
                if loc.get("baro_accuracy"):
                    track.baro_accuracy = loc.get("baro_accuracy")
                if loc.get("speed_accuracy"):
                    track.speed_accuracy = loc.get("speed_accuracy")
                if loc.get("op_status"):
                    track.op_status = loc.get("op_status")
                if loc.get("height_type"):
                    track.height_type = loc.get("height_type")
                if loc.get("timestamp") is not None:
                    track.loc_timestamp = loc.get("timestamp")

            sysm = self._get_system(msg)
            if sysm:
                op_lat = parse_latlon(sysm.get("latitude"))
                op_lon = parse_latlon(sysm.get("longitude"))
                if op_lat is not None and op_lon is not None and not (op_lat == 0 and op_lon == 0):
                    track.op_lat, track.op_lon = op_lat, op_lon
                if sysm.get("operator_location_type"):
                    track.op_location_type = sysm.get("operator_location_type")
                if sysm.get("classification_type"):
                    track.op_classification_type = sysm.get("classification_type")
                # Overwrites, like every other field here. It used to only fill
                # a blank, which meant a stale protocol version could never be
                # corrected by a later burst from this one message type.
                if sysm.get("protocol_version"):
                    track.protocol_version = sysm.get("protocol_version")

        track.touch()
        return track

    def snapshot(self) -> list[dict]:
        return [t.to_dict() for t in self.tracks.values()]


store = TrackStore()
catalog_nicknames: dict[str, str] = {}   # catalog_key -> user-assigned nickname, cached from DB
active_flights: dict[str, int] = {}      # catalog_key -> currently-open flight row id
last_status_sent: dict[str, str] = {}    # catalog_key -> last status the sweeper announced,
                                         # so it only speaks up when one actually changes


class ConnectionManager:
    def __init__(self):
        self.active: set[WebSocket] = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.add(ws)

    def disconnect(self, ws: WebSocket):
        self.active.discard(ws)

    async def broadcast(self, message: dict):
        dead = []
        payload = json.dumps(message)
        # Snapshot the set first: send_text awaits, and a browser connecting
        # (or dropping) during one of those awaits mutates self.active mid-loop
        # — "Set changed size during iteration", which would abort the whole
        # broadcast and, from the zmq listener, get logged as an ingest error.
        for ws in list(self.active):
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()


def _on_track_rekey(old_key: str, new_key: str):
    """A track has stopped being followed under `old_key` (its MAC) and is now
    `new_key` (its serial). Move its open flight across so points keep landing
    on the same row, and tell clients the old marker is gone — nothing else
    ever will, since the sweeper only ages out keys still in store.tracks."""
    flight_id = active_flights.pop(old_key, None)
    if flight_id is not None:
        active_flights.setdefault(new_key, flight_id)
    last_status_sent.pop(old_key, None)
    log.info("Track %s is now keyed as %s (serial decoded)", old_key, new_key)
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return  # no loop (unit test / sync context) — nothing to notify
    asyncio.create_task(manager.broadcast({"type": "dropped", "key": old_key}))


store.on_rekey = _on_track_rekey


def parse_message_payload(raw: bytes):
    """The decoder can emit either a bare JSON list of messages, or (in some
    forks/newer builds) a JSON object with a topic/MAC wrapper. Handle both,
    and also tolerate an optional 'TOPIC {json}' framing on the PUB socket.

    Returns whatever JSON shape arrived (list or dict) for extract_bursts to
    normalize, or None if none of the candidates parsed."""
    text = raw.decode("utf-8", errors="replace").strip()
    candidates = [text]
    if " " in text:
        candidates.append(text.split(" ", 1)[1])
    for candidate in candidates:
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue
    return None


def _burst_from_message_list(messages: list[dict]) -> tuple[Optional[str], list[dict]]:
    """Pull the MAC (if any) out of one aircraft's message list. MAC can show
    up as its own standalone {"MAC": "..."} element, as a "MAC" key riding
    along on individual messages, or not be present at all."""
    mac = None
    cleaned = []
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        if set(msg.keys()) == {"MAC"}:
            mac = msg["MAC"]
            continue
        if "MAC" in msg:
            mac = msg.get("MAC") or mac
        cleaned.append(msg)
    return mac, cleaned


def extract_bursts(data) -> list[tuple[Optional[str], list[dict]]]:
    """Normalize decoder output into a list of (mac_or_none, [messages])
    bursts. Handles the shapes seen so far:
      - a flat list of messages, each individually tagged with "MAC"
      - a list of per-aircraft bursts (a list of lists), where MAC appears as
        its own element, on individual messages, or not at all
      - dict-wrapped forms, e.g. {"DroneID": {"<mac>": [...]}}"""
    if isinstance(data, list) and data and all(isinstance(el, list) for el in data):
        return [_burst_from_message_list(el) for el in data]

    if isinstance(data, list):
        by_mac: dict[str, list[dict]] = {}
        untagged: list[dict] = []
        for msg in data:
            if not isinstance(msg, dict):
                continue
            mac = msg.get("MAC")
            if mac:
                by_mac.setdefault(mac, []).append(msg)
            else:
                untagged.append(msg)
        if by_mac:
            return list(by_mac.items())
        # nothing carried a MAC — treat the whole list as one aircraft's burst
        if untagged:
            mac, cleaned = _burst_from_message_list(untagged)
            return [(mac, cleaned)]
        return []

    if isinstance(data, dict):
        # wrapped form, e.g. {"DroneID": {"<mac>": {...}}} or {"<mac>": [...]}
        inner = data.get("DroneID", data)
        bursts = []
        for mac, val in inner.items():
            if isinstance(val, list):
                bursts.append((mac, val))
            elif isinstance(val, dict) and "AdvData" in val:
                continue  # raw undecoded advert — nothing to correlate yet
        return bursts

    return []


async def persist_update(track: DroneTrack):
    """Append a point to this track's open flight, opening one first if it
    doesn't have one yet. Also refreshes the catalog row (mac/serial/
    registration/ua_type/protocol version) and applies any user-assigned
    nickname to the live track.

    "Opening one" is where re-acquisition is handled. Before starting a
    genuinely new flight, we look for one this aircraft was flying until it
    dropped off the map less than FLIGHT_MERGE_WINDOW_S ago — matched on
    catalog key, MAC, serial, or friendly name, with the name only counting
    when the two serials don't contradict it (see db.find_resumable_flight)
    — and continue that one instead. Two consequences, both intended:
    History shows a single flight
    spanning the gap rather than a pile of fragments, and the Discord
    "new drone detected" alert doesn't re-fire for an aircraft that was
    already announced minutes earlier and merely blinked out of range."""
    key = track.key
    # ensure the catalog row exists before a flight can reference it (FK)
    await db.upsert_catalog(
        key, track.mac, track.serial, track.registration_id,
        track.ua_type, track.protocol_version, track.last_seen,
    )
    nickname = catalog_nicknames.get(key)
    is_new = key not in active_flights
    resumed = False
    if is_new:
        candidate = await db.find_resumable_flight(
            key, track.mac, track.serial, nickname,
            track.last_seen - FLIGHT_MERGE_WINDOW_S,
            # Never adopt a flight another live track is still writing to.
            exclude_ids=list(active_flights.values()),
        )
        if candidate:
            track.segment = await db.resume_flight(
                candidate["id"], key, track.mac, track.serial
            )
            active_flights[key] = candidate["id"]
            resumed = True
            log.info(
                "Resuming flight %s for %s (segment %s) — last seen %.0fs ago, within the "
                "%.0fs reappearance window",
                candidate["id"], key, track.segment,
                track.last_seen - (candidate["last_activity"] or track.last_seen),
                FLIGHT_MERGE_WINDOW_S,
            )
        else:
            active_flights[key] = await db.start_flight(key, track.mac, track.serial, track.last_seen)
            track.segment = 0
    track.flight_id = active_flights[key]
    point_fields = {c: getattr(track, c, None) for c in db.POINT_COLUMNS}
    await db.add_point(track.flight_id, track.last_seen, point_fields)
    track.nickname = nickname

    if is_new and not resumed and key != TEST_DRONE_SERIAL:
        # Fire-and-forget: never let a slow/unreachable webhook stall ingest.
        asyncio.create_task(send_new_drone_alert(track))


def zmq_ctx():
    """One ZMQ context for all three subscriptions. Each listener used to make
    its own, which means three sets of I/O threads and three lots of buffers
    for what is a handful of small messages a second — and nothing to close
    them with either. Context.instance() hands back the same shared one every
    time, created on first use inside the running loop."""
    return zmq.asyncio.Context.instance()


async def watch_connection_state(monitor, channel: str):
    """Reads events off an already-attached ZMQ socket monitor. Must be
    given the monitor socket itself (from get_monitor_socket(), called
    synchronously before the owning socket's connect()) rather than
    attaching it here, since asyncio.create_task() doesn't run its coroutine
    body until the next loop iteration — too late to guarantee catching the
    very first CONNECTED event if attached inside the task itself."""
    while True:
        try:
            msg = await monitor.recv_multipart()
            event = zmq.utils.monitor.parse_monitor_message(msg)
            ev = event.get("event")
            if ev == zmq.EVENT_CONNECTED:
                connection_state[channel] = CONNECTED
            elif ev in (zmq.EVENT_DISCONNECTED, zmq.EVENT_CLOSED, zmq.EVENT_CONNECT_RETRIED):
                connection_state[channel] = DISCONNECTED
        except Exception:
            log.exception("Error reading connection-monitor events for %s", channel)
            await asyncio.sleep(1)


def attach_connection_monitor(sock, channel: str):
    """Call before sock.connect() so the monitor is guaranteed in place in
    time to catch the first CONNECTED event. If this fails for any reason,
    the channel just stays "disconnected"/red rather than crashing the app —
    worth checking the logs if that happens, since it means this signal
    isn't available on this pyzmq/platform."""
    try:
        monitor = sock.get_monitor_socket()
    except Exception:
        log.exception("Could not attach connection monitor for %s — its health dot will stay red", channel)
        return
    asyncio.create_task(watch_connection_state(monitor, channel))


async def zmq_listener():
    sock = zmq_ctx().socket(zmq.SUB)
    attach_connection_monitor(sock, "zmq")
    sock.connect(ZMQ_ADDR)
    sock.setsockopt(zmq.SUBSCRIBE, b"")
    log.info("Subscribed to DroneID decoder at %s", ZMQ_ADDR)
    while True:
        try:
            raw = await sock.recv()
            last_message_at["zmq"] = time.time()
            data = parse_message_payload(raw)
            if data is None:
                continue
            for mac, messages in extract_bursts(data):
                track = store.apply_burst(mac, messages)
                await persist_update(track)
                await manager.broadcast({"type": "update", "drone": track.to_dict()})
        except Exception:
            log.exception("Error in zmq listener loop")
            await asyncio.sleep(1)


async def sniffer_health_listener(addr: str, channel: str):
    """Health-only subscription to a raw sniffer's ZMQ port (bluetooth_
    receiver.py on 4222, wifi_receiver.py on 4223 by default). Doesn't parse
    content — tracks both the TCP-level connection state (process alive?)
    and message arrival timing (actively sending vs. idle)."""
    sock = zmq_ctx().socket(zmq.SUB)
    attach_connection_monitor(sock, channel)
    sock.connect(addr)
    sock.setsockopt(zmq.SUBSCRIBE, b"")
    log.info("Health-monitoring %s sniffer at %s", channel, addr)
    while True:
        try:
            await sock.recv()
            last_message_at[channel] = time.time()
        except Exception:
            log.exception("Error in %s health listener", channel)
            await asyncio.sleep(1)


async def health_broadcaster():
    """Pushes ZMQ/Bluetooth/WiFi health to connected clients periodically,
    independent of whether any drone data is actually flowing."""
    while True:
        try:
            await asyncio.sleep(5)
            await manager.broadcast({"type": "health", "health": compute_health()})
        except Exception:
            log.exception("Error in health broadcaster loop")


# ---- Discord webhook alerts ----

DISCORD_ALERT_TIMEOUT_S = 5  # keep short so an unreachable webhook can't back up ingest

# Read once from the environment at startup, not stored in the DB — this
# repo is meant to be pushed to source control, and a webhook URL is a
# bearer secret (anyone who has it can post into that channel), so it
# belongs in the deployment environment, not in a file that could get
# committed or synced alongside everything else.
discord_webhook_url: Optional[str] = os.environ.get("DRONEID_DISCORD_WEBHOOK") or None
if discord_webhook_url and not discord_webhook_url.startswith((
    "https://discord.com/api/webhooks/", "https://discordapp.com/api/webhooks/",
)):
    log.warning(
        "DRONEID_DISCORD_WEBHOOK is set but doesn't look like a Discord webhook URL "
        "(expected it to start with https://discord.com/api/webhooks/) — alerts will "
        "likely fail to send"
    )

# Optional: pings a specific role on every alert. Must be the role's numeric
# snowflake ID (Server Settings > Roles > right-click the role > Copy Role
# ID, with Developer Mode on), NOT its name — Discord only pings on the
# <@&ROLE_ID> mention syntax, plain "@role-name" text in a message never
# notifies anyone.
DISCORD_ROLE_ID: Optional[str] = os.environ.get("DRONEID_DISCORD_ROLE_ID") or None
if DISCORD_ROLE_ID and not DISCORD_ROLE_ID.isdigit():
    log.warning(
        "DRONEID_DISCORD_ROLE_ID is set but isn't purely numeric — Discord role IDs are "
        "numeric snowflakes, so this will likely fail to resolve as a real mention"
    )

# Cached separately from the DB so building an alert never needs a DB round
# trip on the hot path; kept in sync with /api/station's PUT/DELETE.
station_cache: Optional[dict] = None


def parse_station(raw: Optional[str]) -> Optional[dict]:
    """{"lat": float, "lon": float} from the stored settings string, or None if
    it's unset, unparseable, or missing a coordinate. One implementation for
    all three callers (startup, the GET endpoint, the test simulator), which
    each had their own slightly different version of this try/except."""
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(data, dict):
        return None
    lat, lon = data.get("lat"), data.get("lon")
    if lat is None or lon is None:
        return None
    return {"lat": lat, "lon": lon}


def haversine_meters(lat1, lon1, lat2, lon2) -> float:
    R = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def meters_to_feet(m: float) -> float:
    return m * 3.28084


def mps_to_mph(mps: float) -> float:
    return mps * 2.23694


def build_new_drone_message(track: "DroneTrack") -> str:
    """Builds the alert text for a brand-new (non-test) detection. Uses
    imperial units throughout (feet/mph). Formatted as a Discord ## heading
    (bold, slightly larger) with the role ping (if configured) on that same
    line, followed by the details in a fenced code block on their own line."""
    name = track.nickname or "New Drone"
    role_ping = f" <@&{DISCORD_ROLE_ID}>" if DISCORD_ROLE_ID else ""
    header = f"## {name} Detected{role_ping}"

    has_pos = track.lat is not None and track.lon is not None
    if not has_pos:
        return f"{header}\n```No location information available```"

    parts = []
    if station_cache and station_cache.get("lat") is not None and station_cache.get("lon") is not None:
        dist_ft = meters_to_feet(
            haversine_meters(station_cache["lat"], station_cache["lon"], track.lat, track.lon)
        )
        parts.append(f"{dist_ft:,.0f}ft From Station")

    # op_status is the direct Remote ID signal for this ("Ground", "Airborne",
    # etc.); anything other than "Ground" (including unknown/missing) is
    # treated as flying, since a device actively broadcasting is more likely
    # airborne than not.
    is_landed = (track.op_status or "").strip().lower() == "ground"
    status_clause = "Currently landed" if is_landed else "Currently flying"
    if not is_landed and track.speed is not None:
        status_clause += f" at {mps_to_mph(track.speed):.0f}mph"
    if track.heading is not None:
        status_clause += f" and Heading {track.heading:.0f}\u00b0"
    parts.append(status_clause)

    has_op = track.op_lat is not None and track.op_lon is not None
    parts.append("Controller Located" if has_op else "Controller Not Located")

    body = " - ".join(parts)
    return f"{header}\n```{body}```"


def _post_discord_webhook_sync(url: str, content: str) -> tuple[bool, str]:
    """Returns (success, detail). Never raises — callers decide whether the
    detail matters (the fire-and-forget alert path just logs it; the test
    endpoint surfaces it to the UI)."""
    # allowed_mentions is deliberately locked down rather than left at
    # Discord's default (which parses and pings anything mention-shaped in
    # the content). Drone nicknames are user-editable text that lands
    # directly in this string — without this restriction, naming a drone
    # something like "@everyone" would actually ping the whole server. Only
    # the one specific configured role (if any) is ever allowed through.
    allowed_roles = [DISCORD_ROLE_ID] if DISCORD_ROLE_ID else []
    payload = json.dumps({
        "content": content,
        "allowed_mentions": {"parse": [], "roles": allowed_roles},
    }).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            # Discord sits behind Cloudflare, which blocks requests carrying
            # urllib's default User-Agent ("Python-urllib/3.x") as likely
            # bot/scraper traffic — this alone is the usual cause of an
            # otherwise-inexplicable 403 on an otherwise-correct webhook URL.
            "User-Agent": "DroneID-Frontend (https://github.com/Jerry-235/DroneID-Frontend, 1.0)",
        },
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=DISCORD_ALERT_TIMEOUT_S)
        return True, "ok"
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            pass
        log.exception("Failed to send Discord webhook alert (HTTP %s)", e.code)
        return False, f"HTTP {e.code}: {body or e.reason}"
    except Exception as e:
        log.exception("Failed to send Discord webhook alert")
        return False, str(e)


async def send_new_drone_alert(track: "DroneTrack"):
    if not discord_webhook_url:
        return
    message = build_new_drone_message(track)
    await asyncio.to_thread(_post_discord_webhook_sync, discord_webhook_url, message)


# ---- synthetic test drone, for exercising the real pipeline without RF hardware ----

TEST_DRONE_MAC = "aa:bb:cc:dd:ee:ff"
TEST_DRONE_SERIAL = "TEST1DRONE0000001"
TEST_DEFAULT_ORIGIN = (37.7749, -122.4194)  # used only if no station is configured

test_drone_running = False


def _test_drone_burst(t: float, origin_lat: float, origin_lon: float) -> list[dict]:
    """One synthetic decoded-message burst, shaped exactly like a real
    zmq_decoder.py burst (standalone {"MAC": ...} element + Basic ID +
    Location/Vector Message + System Message + Self ID), so it exercises the
    identical extraction/correlation path as real data — this is what makes
    it useful for finding real bugs rather than just a UI mockup. Orbits a
    center point (the configured station if there is one) with a slowly
    wandering operator position nearby."""
    angle = (t / 20.0) * 2 * math.pi  # ~20s per lap
    radius_deg = 0.003  # roughly ~300m
    lat = origin_lat + radius_deg * math.sin(angle)
    lon = origin_lon + radius_deg * math.cos(angle)
    heading = round((math.degrees(angle) + 90) % 360)
    speed = 8.0 + 2.0 * math.sin(t / 5.0)
    vert_speed = 0.3 * math.cos(t / 4.0)
    alt = 80.0 + 15.0 * math.sin(t / 7.0)
    height_agl = alt - 10.0
    op_lat = origin_lat + 0.0006 * math.sin(t / 13.0)
    op_lon = origin_lon + 0.0006 * math.cos(t / 13.0)

    return [
        {"MAC": TEST_DRONE_MAC},
        {"Basic ID": {
            "protocol_version": "F3411.22", "id_type": "Serial Number (ANSI/CTA-2063-A)",
            "id": TEST_DRONE_SERIAL, "ua_type": 2,
        }},
        {"Location/Vector Message": {
            "protocol_version": "F3411.22", "op_status": "Airborne", "height_type": "AGL",
            "direction": heading, "speed": f"{speed:.1f} m/s", "vert_speed": f"{vert_speed:.1f} m/s",
            "latitude": f"{lat:.7f}", "longitude": f"{lon:.7f}",
            "pressure_altitude": "Undefined", "geodetic_altitude": f"{alt:.1f} m",
            "height_agl": f"{height_agl:.1f} m",
            "vertical_accuracy": "<10 m", "horizontal_accuracy": "<1 m",
            "baro_accuracy": "<10 m", "speed_accuracy": "<0.3 m/s",
            "timestamp": "synthetic",
        }},
        {"System Message": {
            "operator_location_type": "Dynamic", "classification_type": "Test",
            "latitude": op_lat, "longitude": op_lon,
            "geodetic_altitude": "0.0 m", "protocol_version": "F3411.22",
        }},
        {"Self ID": {
            "protocol_version": "F3411.22", "text_type": "Text Description",
            "text": "SYNTHETIC TEST DRONE — for debugging only, not a real detection",
        }},
    ]


async def test_drone_simulator():
    global test_drone_running
    origin_lat, origin_lon = TEST_DEFAULT_ORIGIN
    station = parse_station(await db.get_setting("station"))
    if station:
        origin_lat, origin_lon = station["lat"], station["lon"]
    log.info("Test drone simulator started, orbiting (%s, %s)", origin_lat, origin_lon)
    t0 = time.time()
    try:
        while test_drone_running:
            messages = _test_drone_burst(time.time() - t0, origin_lat, origin_lon)
            mac, cleaned = _burst_from_message_list(messages)
            track = store.apply_burst(mac, cleaned)
            await persist_update(track)
            await manager.broadcast({"type": "update", "drone": track.to_dict()})
            await asyncio.sleep(1.5)
    finally:
        log.info("Test drone simulator stopped")


async def stale_sweeper():
    """Broadcast status transitions (live -> stale -> dropped) so the UI can
    fade/remove icons even without new packets arriving, and close out the DB
    flight record once a track is dropped.

    Only actual transitions go out. It used to re-send every track's status on
    every tick, so a quiet map still pushed a message per track every 5s, and
    each one made the browser rebuild its whole sidebar — for news it already
    had.

    The whole loop body is wrapped in try/except: without it, a single
    unexpected error here (e.g. a transient DB hiccup in db.end_flight)
    would silently kill this task for the rest of the process's life —
    meaning nothing would ever go stale or get dropped again until the
    server is restarted. Every other long-running loop in this file already
    follows this pattern; this one was missing it, which is the most likely
    explanation for drones outliving DROP_AFTER_S indefinitely rather than
    just late."""
    while True:
        try:
            await asyncio.sleep(5)
            now = time.time()
            drop_keys = []
            for key, track in list(store.tracks.items()):
                age = now - track.last_seen
                if age > DROP_AFTER_S:
                    drop_keys.append(key)
                    continue
                status = track.status()
                if last_status_sent.get(key) != status:
                    last_status_sent[key] = status
                    await manager.broadcast({"type": "status", "key": key, "status": status})
            for key in drop_keys:
                store.tracks.pop(key, None)
                last_status_sent.pop(key, None)
                flight_id = active_flights.pop(key, None)
                if flight_id is not None:
                    try:
                        await db.end_flight(flight_id, now)
                    except Exception:
                        # Don't let one flight's DB write take down the rest
                        # of this batch, and still tell clients it's gone —
                        # the flight just stays "in progress" in History
                        # until this can be reconciled, rather than leaving
                        # a marker stuck on the live map indefinitely.
                        log.exception(
                            "Failed to close out flight %s in DB for dropped track %s", flight_id, key
                        )
                await manager.broadcast({"type": "dropped", "key": key})
        except Exception:
            log.exception("Error in stale sweeper loop — will retry on next tick")


@asynccontextmanager
async def lifespan(_app: "FastAPI"):
    """Opens the DB, warms the caches, and starts the background listeners.

    This is the modern replacement for @app.on_event("startup"), which FastAPI
    has deprecated. Nothing is torn down on the way out: the process exits
    immediately afterwards, the listener tasks die with the loop, and SQLite in
    WAL mode needs no explicit close to stay consistent."""
    global station_cache
    db.init(os.environ.get("DRONEID_DB_PATH", os.path.join(os.path.dirname(__file__), "droneid.db")))
    catalog_nicknames.update(await db.get_catalog())
    station_cache = parse_station(await db.get_setting("station"))
    tasks = [
        asyncio.create_task(zmq_listener()),
        asyncio.create_task(sniffer_health_listener(BT_ZMQ_ADDR, "bluetooth")),
        asyncio.create_task(sniffer_health_listener(WIFI_ZMQ_ADDR, "wifi")),
        asyncio.create_task(health_broadcaster()),
        asyncio.create_task(stale_sweeper()),
    ]
    # Held only so they aren't garbage-collected mid-flight: asyncio keeps
    # nothing but a weak reference to a bare create_task().
    _app.state.background_tasks = tasks
    yield


app = FastAPI(title="DroneID Live Map", lifespan=lifespan)

# Compress HTTP responses. A long flight's point list is megabytes of JSON
# that repeats the same field names and strings on every row, so it shrinks
# about 20x (a 7,492-point flight: 4.3 MB -> 0.19 MB) — the difference
# between a second-plus and near-instant over WiFi or to a phone. Level 5
# gets almost all of level 9's saving in a fraction of the CPU time. Small
# responses (under 1 KB) are left alone, and the WebSocket is untouched: this
# middleware only handles plain HTTP.
app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=5)


class RenameBody(BaseModel):
    nickname: str


class StationBody(BaseModel):
    lat: float
    lon: float


@app.get("/api/station")
async def api_get_station():
    """The station/home-point location, stored server-side (not per-browser)
    so it's the same on every device that opens this app. None for both
    fields if it hasn't been set yet."""
    station = parse_station(await db.get_setting("station"))
    return station or {"lat": None, "lon": None}


@app.put("/api/station")
async def api_set_station(body: StationBody):
    if not (-90 <= body.lat <= 90) or not (-180 <= body.lon <= 180):
        return JSONResponse({"error": "lat/lon out of range"}, status_code=400)
    global station_cache
    await db.set_setting("station", json.dumps({"lat": body.lat, "lon": body.lon}))
    station_cache = {"lat": body.lat, "lon": body.lon}
    return {"ok": True, "lat": body.lat, "lon": body.lon}


@app.delete("/api/station")
async def api_delete_station():
    global station_cache
    await db.delete_setting("station")
    station_cache = None
    return {"ok": True}


@app.get("/api/discord_webhook")
async def api_get_discord_webhook():
    """Status only — deliberately never returns the actual URL. It's read
    from the DRONEID_DISCORD_WEBHOOK environment variable at startup, not
    stored anywhere the app itself manages, so there's no save/clear
    endpoint for it: changing it means changing the environment and
    restarting the process, same as any other env-configured setting here."""
    return {"configured": discord_webhook_url is not None}


@app.post("/api/discord_webhook/test")
async def api_test_discord_webhook():
    if not discord_webhook_url:
        return JSONResponse({"error": "No webhook configured."}, status_code=400)
    # Same role-ping logic as a real alert, so this test actually exercises
    # that path too — not just the webhook URL itself.
    role_ping = f" <@&{DISCORD_ROLE_ID}>" if DISCORD_ROLE_ID else ""
    test_message = f"## DroneID Test Alert{role_ping}\n```If you can see this, the webhook is working.```"
    ok, detail = await asyncio.to_thread(_post_discord_webhook_sync, discord_webhook_url, test_message)
    if not ok:
        return JSONResponse({"error": f"Discord rejected the request — {detail}"}, status_code=502)
    return {"ok": True}


@app.get("/api/drones")
async def get_drones():
    return {"drones": store.snapshot()}


@app.get("/api/health")
async def get_health():
    """Liveness of the ZMQ decoder feed and the two raw sniffers, each based
    on whether a message has arrived within the last HEALTH_TIMEOUT_S."""
    return {"health": compute_health()}


@app.post("/api/test/drone/start")
async def start_test_drone():
    """Starts a synthetic orbiting test drone (with an operator position)
    fed through the exact same apply_burst/persist_update/broadcast pipeline
    as real ZMQ data — for exercising the live map without RF hardware."""
    global test_drone_running
    if test_drone_running:
        return {"ok": True, "already_running": True}
    test_drone_running = True
    asyncio.create_task(test_drone_simulator())
    return {"ok": True}


@app.post("/api/test/drone/stop")
async def stop_test_drone():
    """Stops feeding new bursts. Left to decay naturally through the normal
    stale/drop timeouts rather than force-removed, so that lifecycle gets
    exercised too instead of skipped."""
    global test_drone_running
    test_drone_running = False
    return {"ok": True}


@app.get("/api/test/drone/status")
async def test_drone_status():
    return {"running": test_drone_running}


@app.get("/api/drones/catalog")
async def api_catalog():
    """All known nicknames, keyed by catalog_key (serial, or MAC if no
    serial has ever been seen for that drone)."""
    return {"catalog": catalog_nicknames}


@app.patch("/api/drones/{catalog_key}/name")
async def api_rename_drone(catalog_key: str, body: RenameBody):
    """Rename a drone. Applies retroactively: every past and future flight
    for this catalog_key will show the new name, since the name is looked
    up at query/display time rather than copied into flight rows."""
    nickname = body.nickname.strip() or None
    await db.rename_drone(catalog_key, nickname)
    if nickname:
        catalog_nicknames[catalog_key] = nickname
    else:
        catalog_nicknames.pop(catalog_key, None)
    if catalog_key in store.tracks:
        store.tracks[catalog_key].nickname = nickname
        await manager.broadcast({"type": "update", "drone": store.tracks[catalog_key].to_dict()})
    return {"ok": True, "catalog_key": catalog_key, "nickname": nickname}


MAX_FLIGHT_PAGE = 500


@app.get("/api/flights")
async def api_list_flights(limit: int = 50, offset: int = 0):
    """Past (and in-progress) flights, most recent first. limit is capped:
    without it, one mistyped query string could ask for the entire table and
    hold the single DB lock (and so all ingest) for as long as that took."""
    limit = max(1, min(int(limit), MAX_FLIGHT_PAGE))
    offset = max(0, int(offset))
    return {"flights": await db.list_flights(limit, offset)}


@app.get("/api/flights/{flight_id}")
async def api_get_flight(flight_id: int):
    """A single flight's metadata plus its full point-by-point path — this is
    what both the historical playback view and 'show this live drone's path
    so far' use (for an in-progress flight, points are returned up to now)."""
    flight = await db.get_flight(flight_id)
    if not flight:
        return JSONResponse({"error": "flight not found"}, status_code=404)
    points = await db.get_flight_points(flight_id)
    # Serialised here and returned as a finished Response, rather than
    # returning the dict for FastAPI to encode. FastAPI's default path first
    # walks every value through its own recursive encoder in pure Python —
    # ~190k values for a long flight — before it even starts serialising,
    # which is the slow part for a response this size. The data is already
    # plain JSON types straight from SQLite, so that pass has nothing to do.
    # Same serialisation settings as FastAPI's own JSONResponse, so the body
    # is exactly what it would have produced (SQLite can't store NaN, so
    # allow_nan=False can't trip).
    body = json.dumps(
        {"flight": flight, "points": points},
        ensure_ascii=False, allow_nan=False, separators=(",", ":"),
    )
    return Response(content=body, media_type="application/json")


MISMATCH_MESSAGE = "Cannot merge mismatched info"


def flights_are_same_aircraft(flights: list[dict]) -> bool:
    """True if every one of these flights can be taken to belong to the same
    aircraft, by the same standard the automatic merge uses: they all agree
    on a serial, or they all agree on a MAC.

    Either alone is enough, because each covers a case the other misses — a
    MAC can be re-randomized between flights while the serial holds, and a
    serial can be missing from an early flight that was only ever seen by
    MAC. What is *not* allowed is inferring a match from a field some of
    them don't have: a flight carrying neither a MAC nor a serial can't be
    shown to be the same aircraft as anything, so it never merges."""
    if len(flights) < 2:
        return False
    serials = [f.get("serial") for f in flights]
    macs = [f.get("mac") for f in flights]
    all_serials_agree = all(serials) and len(set(serials)) == 1
    all_macs_agree = all(macs) and len(set(macs)) == 1
    return all_serials_agree or all_macs_agree


class FlightIdsBody(BaseModel):
    flight_ids: list[int]


def _live_flight_ids() -> set[int]:
    """Flights a live track is still writing points into. Editing one out
    from under the ingest path would strand active_flights on a row that no
    longer exists (or has silently changed shape), so both merge and delete
    refuse to touch these."""
    return set(active_flights.values())


@app.post("/api/flights/merge")
async def api_merge_flights(body: FlightIdsBody):
    """Fold several past flights into one. Destructive and not reversible:
    the source rows are removed and their points repointed at the survivor,
    which is the oldest of them."""
    ids = list(dict.fromkeys(body.flight_ids))  # de-dupe, keep order
    if len(ids) < 2:
        return JSONResponse(
            {"error": "Select at least two flights to merge."}, status_code=400
        )
    flights = await db.get_flights_by_ids(ids)
    if len(flights) != len(ids):
        return JSONResponse({"error": "One or more flights no longer exist."}, status_code=404)
    live = _live_flight_ids().intersection(ids)
    if live:
        return JSONResponse(
            {"error": "That flight is still in progress — wait for it to close out first."},
            status_code=409,
        )
    if not flights_are_same_aircraft(flights):
        return JSONResponse({"error": MISMATCH_MESSAGE}, status_code=409)

    target_id = await db.merge_flights(ids)
    log.info("Merged flights %s into %s", ids, target_id)
    return {"ok": True, "flight_id": target_id, "merged": len(ids)}


@app.post("/api/flights/delete")
async def api_delete_flights(body: FlightIdsBody):
    """Delete past flights and their points. Also not reversible."""
    ids = list(dict.fromkeys(body.flight_ids))
    if not ids:
        return JSONResponse({"error": "No flights selected."}, status_code=400)
    live = _live_flight_ids().intersection(ids)
    if live:
        return JSONResponse(
            {"error": "That flight is still in progress — wait for it to close out first."},
            status_code=409,
        )
    deleted = await db.delete_flights(ids)
    log.info("Deleted flights %s (%s rows)", ids, deleted)
    return {"ok": True, "deleted": deleted}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        await websocket.send_text(json.dumps({
            "type": "snapshot", "drones": store.snapshot(), "health": compute_health(),
        }))
        while True:
            await websocket.receive_text()  # client doesn't send anything meaningful; just keep alive
    except WebSocketDisconnect:
        pass
    finally:
        # finally, not just the WebSocketDisconnect branch: any other error
        # here (a network reset surfacing as something else, a send failing
        # mid-snapshot) would otherwise leave a dead socket in the broadcast
        # set forever, and every broadcast from then on would try it and fail.
        manager.disconnect(websocket)


STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    """Browsers ask for /favicon.ico at the site root regardless of the <link>
    tags in the page — some bookmark and tab-restore paths only ever look
    there. Served from the real icon so that request isn't a 404."""
    return FileResponse(os.path.join(STATIC_DIR, "icons", "favicon.ico"))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=os.environ.get("HOST", "0.0.0.0"), port=int(os.environ.get("PORT", "8000")))