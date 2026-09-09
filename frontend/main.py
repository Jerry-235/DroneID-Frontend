"""
DroneID web frontend — backend.

Subscribes to the DroneID zmq_decoder.py output (default tcp://127.0.0.1:4224,
per bkerler/DroneID's README: `./zmq_decoder.py --zmqsetting 127.0.0.1:4224
--zmqclients 127.0.0.1:4222,127.0.0.1:4223`), maintains live in-memory track
state keyed by Approach A (UAS serial from Basic ID, falling back to MAC),
and serves:
  - GET  /api/drones     current snapshot of all known tracks
  - WS   /ws             live push of updates as they arrive
  - GET  /                the map UI (static/index.html)

No persistence yet (that's the next phase) — this is the live-view slice.
"""

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field, asdict
from typing import Optional

import zmq
import zmq.asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("droneid-frontend")

ZMQ_ADDR = os.environ.get("DRONEID_ZMQ_ADDR", "tcp://127.0.0.1:4224")
STALE_AFTER_S = float(os.environ.get("DRONEID_STALE_AFTER_S", "45"))
DROP_AFTER_S = float(os.environ.get("DRONEID_DROP_AFTER_S", "300"))

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
    source: Optional[str] = None  # "wifi" or "bt", whichever last reported
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
    notes: Optional[str] = None
    flight_id: Optional[int] = None  # current open flight row in the DB, if any

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
    def _get_loc(msg: dict) -> Optional[dict]:
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
                    return merged
                return loc
        return None

    @staticmethod
    def _get_system(msg: dict) -> Optional[dict]:
        for key in ("System Message", "System"):
            if key in msg:
                return msg[key]
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
            elif "Self ID" in msg:
                text = msg["Self ID"].get("text")
                if text:
                    track.description = text

            loc = self._get_loc(msg)
            if loc:
                track.source = "wifi/bt"
                lat = parse_latlon(loc.get("latitude"))
                lon = parse_latlon(loc.get("longitude"))
                if lat is not None and lon is not None and not (lat == 0 and lon == 0):
                    track.lat, track.lon = lat, lon

                alt = clean_float(loc.get("geodetic_altitude"))
                hagl = clean_float(loc.get("height_agl"))
                speed = clean_float(loc.get("speed"))
                is_raw_shape = isinstance(loc.get("geodetic_altitude"), int) and "coord" in msg.get(
                    "Location Vector", msg.get("Location/Vector Message", {})
                )
                if is_raw_shape:
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
                if sysm.get("protocol_version") and not track.protocol_version:
                    track.protocol_version = sysm.get("protocol_version")

        track.touch()
        return track

    def snapshot(self) -> list[dict]:
        return [t.to_dict() for t in self.tracks.values()]


store = TrackStore()
catalog_nicknames: dict[str, str] = {}   # catalog_key -> user-assigned nickname, cached from DB
active_flights: dict[str, int] = {}      # catalog_key -> currently-open flight row id


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
        for ws in self.active:
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()


def parse_message_payload(raw: bytes) -> Optional[dict]:
    """The decoder can emit either a bare JSON list of messages, or (in some
    forks/newer builds) a JSON object with a topic/MAC wrapper. Handle both,
    and also tolerate an optional 'TOPIC {json}' framing on the PUB socket."""
    text = raw.decode("utf-8", errors="replace").strip()
    for candidate in (text, text.split(" ", 1)[-1] if " " in text else text):
        try:
            data = json.loads(candidate)
            return data
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
    """Open a new flight the first time we see this catalog key in this
    process's lifetime, otherwise append a point to the already-open flight.
    Also refreshes the catalog row (mac/serial/registration/ua_type/protocol
    version) and applies any user-assigned nickname to the live track."""
    key = track.key
    # ensure the catalog row exists before a flight can reference it (FK)
    await db.upsert_catalog(
        key, track.mac, track.serial, track.registration_id,
        track.ua_type, track.protocol_version, track.last_seen,
    )
    if key not in active_flights:
        active_flights[key] = await db.start_flight(key, track.mac, track.serial, track.last_seen)
    track.flight_id = active_flights[key]
    point_fields = {c: getattr(track, c, None) for c in db.POINT_COLUMNS}
    await db.add_point(track.flight_id, track.last_seen, point_fields)
    track.nickname = catalog_nicknames.get(key)


async def zmq_listener():
    ctx = zmq.asyncio.Context()
    sock = ctx.socket(zmq.SUB)
    sock.connect(ZMQ_ADDR)
    sock.setsockopt(zmq.SUBSCRIBE, b"")
    log.info("Subscribed to DroneID decoder at %s", ZMQ_ADDR)
    while True:
        try:
            raw = await sock.recv()
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


async def stale_sweeper():
    """Periodically re-broadcast status transitions (live -> stale -> dropped)
    so the UI can fade/remove icons even without new packets arriving, and
    close out the DB flight record once a track is dropped."""
    while True:
        await asyncio.sleep(5)
        now = time.time()
        drop_keys = []
        for key, track in list(store.tracks.items()):
            age = now - track.last_seen
            if age > DROP_AFTER_S:
                drop_keys.append(key)
            else:
                await manager.broadcast({"type": "status", "key": key, "status": track.status()})
        for key in drop_keys:
            store.tracks.pop(key, None)
            flight_id = active_flights.pop(key, None)
            if flight_id is not None:
                await db.end_flight(flight_id, now)
            await manager.broadcast({"type": "dropped", "key": key})


app = FastAPI(title="DroneID Live Map")


class RenameBody(BaseModel):
    nickname: str


class StationBody(BaseModel):
    lat: float
    lon: float


@app.on_event("startup")
async def startup():
    db.init(os.environ.get("DRONEID_DB_PATH", os.path.join(os.path.dirname(__file__), "droneid.db")))
    catalog_nicknames.update(await db.get_catalog())
    asyncio.create_task(zmq_listener())
    asyncio.create_task(stale_sweeper())


@app.get("/api/station")
async def api_get_station():
    """The station/home-point location, stored server-side (not per-browser)
    so it's the same on every device that opens this app. None for both
    fields if it hasn't been set yet."""
    raw = await db.get_setting("station")
    if not raw:
        return {"lat": None, "lon": None}
    try:
        data = json.loads(raw)
        return {"lat": data.get("lat"), "lon": data.get("lon")}
    except (json.JSONDecodeError, AttributeError):
        return {"lat": None, "lon": None}


@app.put("/api/station")
async def api_set_station(body: StationBody):
    if not (-90 <= body.lat <= 90) or not (-180 <= body.lon <= 180):
        return JSONResponse({"error": "lat/lon out of range"}, status_code=400)
    await db.set_setting("station", json.dumps({"lat": body.lat, "lon": body.lon}))
    return {"ok": True, "lat": body.lat, "lon": body.lon}


@app.delete("/api/station")
async def api_delete_station():
    await db.delete_setting("station")
    return {"ok": True}


@app.get("/api/drones")
async def get_drones():
    return {"drones": store.snapshot()}


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


@app.get("/api/flights")
async def api_list_flights(limit: int = 50, offset: int = 0):
    """Past (and in-progress) flights, most recent first."""
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
    return {"flight": flight, "points": points}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        await websocket.send_text(json.dumps({"type": "snapshot", "drones": store.snapshot()}))
        while True:
            await websocket.receive_text()  # client doesn't send anything meaningful; just keep alive
    except WebSocketDisconnect:
        manager.disconnect(websocket)


app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")


@app.get("/")
async def index():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=os.environ.get("HOST", "0.0.0.0"), port=int(os.environ.get("PORT", "8000")))
