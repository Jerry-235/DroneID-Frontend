"""Multi-node support: which stations feed this server, where they are, and
how to tell when two of them saw the same broadcast.

A station is one sensor box. The master (this server's own decoder) is one;
any others are remote nodes whose decoder output is subscribed to over the
network (normally Tailscale). Every station sends the same burst format, so
TrackStore folds a drone seen by two stations into one track automatically —
this module only adds what the merge can't know by itself: which station a
point came from, and whether a point is a second copy of one another station
already reported.

Config lives in nodes.json next to main.py (path overridable with
DRONEID_NODES_PATH). Without that file the server behaves exactly as before:
a single master fed from DRONEID_ZMQ_ADDR.
"""
import json
import logging
import math
import os
import re
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger("droneid-frontend.nodes")

MASTER_ID = "master"
KINDS = ("master", "static", "roaming")
_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,31}$")
_ENDPOINT_RE = re.compile(r"^tcp://[^\s/]+:\d{1,5}$")

# A remote node's position report older than this is shown as last known
# rather than current.
STATUS_STALE_S = float(os.environ.get("DRONEID_NODE_STATUS_STALE_S", "60"))
# Two points from different stations with no Remote ID timestamp of their
# own count as the same broadcast if they're this close in time.
UNSTAMPED_DUPLICATE_WINDOW_S = 2.0
# How many recent points a track remembers for duplicate checks.
RECENT_POINTS_KEPT = 32


@dataclass(frozen=True)
class NodeConfig:
    id: str
    name: str
    kind: str              # "master", "static" or "roaming"
    bursts: str            # tcp endpoint of that node's decoder PUB socket
    status: Optional[str] = None   # tcp endpoint of node_agent PUB socket, if any
    lat: Optional[float] = None    # fixed position, for static nodes
    lon: Optional[float] = None


class ConfigError(ValueError):
    pass


def _check_endpoint(node_id: str, field_name: str, value: str) -> str:
    if not isinstance(value, str) or not _ENDPOINT_RE.match(value):
        raise ConfigError(f"node '{node_id}': {field_name} must look like tcp://host:port, got {value!r}")
    return value


def _check_coord(node_id: str, name: str, value, limit: float) -> float:
    try:
        f = float(value)
    except (TypeError, ValueError):
        raise ConfigError(f"node '{node_id}': {name} is not a number: {value!r}")
    if not math.isfinite(f) or abs(f) > limit:
        raise ConfigError(f"node '{node_id}': {name} out of range: {value!r}")
    return f


def parse_nodes(data, default_master_bursts: str) -> list[NodeConfig]:
    """Validates a nodes.json document. Raises ConfigError with a message
    that names the bad node and field, so the startup log says what to fix."""
    if not isinstance(data, dict) or not isinstance(data.get("nodes"), list):
        raise ConfigError('nodes.json must be an object with a "nodes" list')
    out: list[NodeConfig] = []
    seen: set[str] = set()
    for raw in data["nodes"]:
        if not isinstance(raw, dict):
            raise ConfigError("every entry in nodes.json must be an object")
        node_id = str(raw.get("id", "")).strip()
        if not _ID_RE.match(node_id):
            raise ConfigError(f"node id {node_id!r} must be lowercase letters, digits or dashes")
        if node_id in seen:
            raise ConfigError(f"node id {node_id!r} is listed more than once")
        seen.add(node_id)
        kind = raw.get("kind", "static")
        if kind not in KINDS:
            raise ConfigError(f"node '{node_id}': kind must be one of {KINDS}, got {kind!r}")
        if kind == "master" and node_id != MASTER_ID:
            raise ConfigError(f'the master node must be id "{MASTER_ID}"')
        bursts = raw.get("bursts") or (default_master_bursts if kind == "master" else None)
        if bursts is None:
            raise ConfigError(f"node '{node_id}': bursts endpoint is required")
        bursts = _check_endpoint(node_id, "bursts", bursts)
        status = raw.get("status")
        if status is not None:
            status = _check_endpoint(node_id, "status", status)
        lat = lon = None
        if kind == "static":
            if "lat" not in raw or "lon" not in raw:
                raise ConfigError(f"node '{node_id}': a static node needs lat and lon")
            lat = _check_coord(node_id, "lat", raw["lat"], 90)
            lon = _check_coord(node_id, "lon", raw["lon"], 180)
        out.append(NodeConfig(
            id=node_id,
            name=str(raw.get("name") or node_id),
            kind=kind,
            bursts=bursts,
            status=status,
            lat=lat,
            lon=lon,
        ))
    if not any(n.kind == "master" for n in out):
        out.insert(0, NodeConfig(MASTER_ID, "Master", "master", default_master_bursts))
    return out


def load_nodes(path: str, default_master_bursts: str) -> list[NodeConfig]:
    """Reads nodes.json. A missing file is not an error — it means a single
    master, exactly as before multi-node support existed."""
    if not os.path.exists(path):
        return [NodeConfig(MASTER_ID, "Master", "master", default_master_bursts)]
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    nodes = parse_nodes(data, default_master_bursts)
    log.info("Loaded %d station(s) from %s: %s", len(nodes), path, ", ".join(n.id for n in nodes))
    return nodes


def parse_status(raw) -> Optional[dict]:
    """A node_agent position report, validated. None for anything else, so a
    malformed or unrelated message can never half-update a station."""
    if not isinstance(raw, dict) or raw.get("type") != "node_status":
        return None
    node_id = raw.get("node")
    if not isinstance(node_id, str) or not _ID_RE.match(node_id):
        return None
    fix = raw.get("fix") is True
    lat = lon = None
    if fix:
        try:
            lat, lon = float(raw["lat"]), float(raw["lon"])
        except (KeyError, TypeError, ValueError):
            return None
        if not (math.isfinite(lat) and math.isfinite(lon)) or abs(lat) > 90 or abs(lon) > 180:
            return None
        if lat == 0 and lon == 0:
            # A receiver with no fix often reports 0,0. That is not a place.
            fix, lat, lon = False, None, None
    return {
        "node": node_id,
        "name": raw.get("name") if isinstance(raw.get("name"), str) else None,
        "fix": fix,
        "lat": lat,
        "lon": lon,
        "alt": _float_or_none(raw.get("alt")),
        "speed": _float_or_none(raw.get("speed")),
        "track": _float_or_none(raw.get("track")),
        "sats": _int_or_none(raw.get("sats")),
    }


def _float_or_none(v) -> Optional[float]:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _int_or_none(v) -> Optional[int]:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


@dataclass
class _NodeState:
    last_burst_at: Optional[float] = None
    last_status_at: Optional[float] = None
    fix: bool = False
    lat: Optional[float] = None      # roaming: last reported fix
    lon: Optional[float] = None
    alt: Optional[float] = None
    speed: Optional[float] = None
    track: Optional[float] = None
    sats: Optional[int] = None
    reported_name: Optional[str] = None


class NodeRegistry:
    """Current state of every configured station. Config is fixed at startup;
    only the live state (last burst, last position report) changes."""

    def __init__(self, nodes: list[NodeConfig]):
        self.config: dict[str, NodeConfig] = {n.id: n for n in nodes}
        self.state: dict[str, _NodeState] = {n.id: _NodeState() for n in nodes}

    def __contains__(self, node_id: str) -> bool:
        return node_id in self.config

    def ids(self) -> list[str]:
        return list(self.config)

    def note_burst(self, node_id: str, now: float):
        if node_id in self.state:
            self.state[node_id].last_burst_at = now

    def note_status(self, status: dict, now: float) -> bool:
        """Applies a parsed position report. Returns False (and changes
        nothing) for a node that isn't configured, so an unknown sender can't
        add itself to the map."""
        st = self.state.get(status["node"])
        cfg = self.config.get(status["node"])
        if st is None or cfg is None or cfg.kind != "roaming":
            return False
        st.last_status_at = now
        st.fix = status["fix"]
        if status["fix"]:
            st.lat, st.lon = status["lat"], status["lon"]
        st.alt, st.speed, st.track, st.sats = status["alt"], status["speed"], status["track"], status["sats"]
        if status.get("name"):
            st.reported_name = status["name"]
        return True

    def position(self, node_id: str) -> Optional[tuple[float, float]]:
        """Where the station is now: its fixed position if static, its last
        real fix if roaming, None if neither is known. The master's position
        comes from the home-point setting, so the caller supplies that."""
        cfg = self.config.get(node_id)
        if cfg is None:
            return None
        if cfg.kind == "static":
            return cfg.lat, cfg.lon
        if cfg.kind == "roaming":
            st = self.state[node_id]
            if st.lat is not None and st.lon is not None:
                return st.lat, st.lon
        return None

    def describe(self, node_id: str, now: float, link: str, master_position=None) -> dict:
        cfg = self.config[node_id]
        st = self.state[node_id]
        if cfg.kind == "master":
            pos = master_position
        else:
            pos = self.position(node_id)
        status_age = None if st.last_status_at is None else now - st.last_status_at
        return {
            "id": cfg.id,
            "name": cfg.name if cfg.name != cfg.id else (st.reported_name or cfg.name),
            "kind": cfg.kind,
            "lat": pos[0] if pos else None,
            "lon": pos[1] if pos else None,
            "fix": (st.fix if cfg.kind == "roaming" else pos is not None),
            "link": link,
            "status_age": None if status_age is None else round(status_age, 1),
            "stale": (cfg.kind == "roaming" and (status_age is None or status_age > STATUS_STALE_S)),
            "alt": st.alt if cfg.kind == "roaming" else None,
            "speed": st.speed if cfg.kind == "roaming" else None,
            "track": st.track if cfg.kind == "roaming" else None,
            "sats": st.sats if cfg.kind == "roaming" else None,
        }


@dataclass
class SeenPoint:
    station: str
    lat: float
    lon: float
    loc_ts: Optional[str]
    at: float


def is_cross_station_duplicate(recent: list, station: str, lat: float, lon: float,
                               loc_ts, now: float) -> bool:
    """True if another station already recorded this same position for this
    track. Same-station repeats are NOT duplicates here — a drone's beacon is
    re-sent several times a second with the same timestamp, and that's been
    stored as-is all along; changing it would alter existing flight data.

    A Remote ID location carries its own timestamp, and two stations hearing
    the same broadcast report the same one, so that match is exact. Without
    one, fall back to the same position within a short window."""
    loc_key = None if loc_ts is None else str(loc_ts)
    for p in recent:
        if p.station == station:
            continue
        if abs(p.lat - lat) > 1e-6 or abs(p.lon - lon) > 1e-6:
            continue
        if loc_key is not None and p.loc_ts is not None:
            if p.loc_ts == loc_key:
                return True
        elif abs(now - p.at) <= UNSTAMPED_DUPLICATE_WINDOW_S:
            return True
    return False


def remember_point(recent: list, station: str, lat: float, lon: float, loc_ts, now: float):
    recent.append(SeenPoint(station, lat, lon, None if loc_ts is None else str(loc_ts), now))
    del recent[:-RECENT_POINTS_KEPT]
