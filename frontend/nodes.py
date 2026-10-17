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
DRONEID_NODES_PATH). It lists only the external stations, by name and address.
The master is implied by the program running. Without that file the server
behaves exactly as before: a single master fed from DRONEID_ZMQ_ADDR.
"""
import json
import logging
import math
import os
import ipaddress
import re
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger("droneid-frontend.nodes")

MASTER_ID = "master"

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
    number: Optional[int] = None   # the ID number from nodes.json, None for the master


class ConfigError(ValueError):
    pass


def _check_coord(node_id: str, name: str, value, limit: float) -> float:
    try:
        f = float(value)
    except (TypeError, ValueError):
        raise ConfigError(f"node '{node_id}': {name} is not a number: {value!r}")
    if not math.isfinite(f) or abs(f) > limit:
        raise ConfigError(f"node '{node_id}': {name} out of range: {value!r}")
    return f


# Every external station runs the same two services on its address: the
# decoder's burst feed, and the node_agent beacon (position and sniffer health).
NODE_BURSTS_PORT = 4224
NODE_STATUS_PORT = 4230
MAX_NODE_NUMBER = 999
ALLOWED_FIELDS = {"id", "ip", "lat", "lon"}
_HOST_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.\-]{0,252}$")


def _valid_station_ip(ip: str) -> bool:
    """An IPv4 or IPv6 literal (Tailscale hands out both), or a plain host name."""
    try:
        ipaddress.ip_address(ip)
        return True
    except ValueError:
        return _HOST_RE.match(ip) is not None


def _endpoint_host(ip: str) -> str:
    """ZMQ endpoints need an IPv6 literal in brackets: tcp://[fd7a::1]:4224."""
    return f"[{ip}]" if ":" in ip else ip


def node_id_for_number(number: int) -> str:
    """The internal id of station number N. The admin page shows the number;
    the database and the code use this id."""
    return f"node-{number}"


def _number_of(raw: dict) -> Optional[int]:
    """The ID number of one entry: an integer, or a string of digits."""
    value = raw.get("id")
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and re.fullmatch(r"\d{1,3}", value.strip()):
        return int(value.strip())
    return None


def parse_nodes(data, default_master_bursts: str) -> list[NodeConfig]:
    """Validates a nodes.json document. The master is implied by the program
    running and is not listed. Each station has an id (a number from 1 to 999),
    an ip, and optionally a starting lat and lon. Anything else is an error, so
    an old file fails loudly instead of being half-read. Raises ConfigError
    naming the station."""
    if not isinstance(data, dict):
        raise ConfigError('nodes.json must be an object with a "stations" list')
    unknown_top = set(data) - {"stations"}
    if unknown_top:
        raise ConfigError(f"nodes.json: unknown top-level field {sorted(unknown_top)[0]!r}; the file has only \"stations\"")
    entries = data.get("stations", [])
    if not isinstance(entries, list):
        raise ConfigError('"stations" in nodes.json must be a list')
    out: list[NodeConfig] = [NodeConfig(MASTER_ID, "Station", "master", default_master_bursts)]
    seen_ids: set[str] = set()
    seen_ips: set[str] = set()
    for position, raw in enumerate(entries, start=1):
        if not isinstance(raw, dict):
            raise ConfigError(f"station at position {position}: each entry must be an object")
        if raw.get("id") == MASTER_ID or raw.get("kind") == "master":
            raise ConfigError("nodes.json lists the master, which is not allowed: the master is implied "
                              "by the program running. Remove that entry.")
        number = _number_of(raw)
        label = f"ID {raw.get('id')!r}" if raw.get("id") not in (None, "") else f"at position {position}"
        if number is None or not (1 <= number <= MAX_NODE_NUMBER):
            raise ConfigError(f"station {label}: id must be a number from 1 to {MAX_NODE_NUMBER}")
        extra = set(raw) - ALLOWED_FIELDS
        if extra:
            raise ConfigError(f"station {label}: unknown field {sorted(extra)[0]!r}. Only id, ip, "
                              "and optional lat and lon are allowed; names and locations are set in the admin page")
        ip = str(raw.get("ip") or "").strip()
        if not _valid_station_ip(ip):
            raise ConfigError(f"station {label}: ip must be an address such as 100.100.10.90, got {raw.get('ip')!r}")
        node_id = node_id_for_number(number)
        if node_id in seen_ids:
            raise ConfigError(f"station id {number} is listed more than once")
        if ip in seen_ips:
            raise ConfigError(f"station {label}: ip {ip} is listed more than once")
        seen_ids.add(node_id)
        seen_ips.add(ip)
        lat = lon = None
        if "lat" in raw or "lon" in raw:
            if "lat" not in raw or "lon" not in raw:
                raise ConfigError(f"station {label}: lat and lon must be given together")
            lat = _check_coord(node_id, "lat", raw["lat"], 90)
            lon = _check_coord(node_id, "lon", raw["lon"], 180)
        # With a starting position the station is "static"; without one it is
        # "roaming" and reports its position from its GPS.
        kind = "static" if lat is not None else "roaming"
        out.append(NodeConfig(
            id=node_id,
            name=f"Node {number}",
            kind=kind,
            bursts=f"tcp://{_endpoint_host(ip)}:{NODE_BURSTS_PORT}",
            status=f"tcp://{_endpoint_host(ip)}:{NODE_STATUS_PORT}",   # every station reports health here
            lat=lat,
            lon=lon,
            number=number,
        ))
    return out


def load_nodes(path: str, default_master_bursts: str) -> list[NodeConfig]:
    """Reads nodes.json. A missing file is not an error — it means a single
    master, exactly as before multi-node support existed."""
    if not os.path.exists(path):
        return [NodeConfig(MASTER_ID, "Station", "master", default_master_bursts)]
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
    health = None
    if isinstance(raw.get("health"), dict):
        h = raw["health"]
        health = {"wifi": _sniffer_state(h.get("wifi")), "bluetooth": _sniffer_state(h.get("bluetooth"))}
    return {
        "health": health,
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
    health: Optional[dict] = None    # {"wifi": "green"|"yellow"|"red", "bluetooth": ...} from node_agent
    last_health_at: Optional[float] = None


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

    def note_status(self, node_id: str, status: dict, now: float) -> bool:
        """Applies a report from a station's node_agent. Sniffer health is kept
        for every external station; position only for a roaming one, since a
        fixed station's position comes from its config. False, and nothing
        changes, for an unknown station."""
        st = self.state.get(node_id)
        cfg = self.config.get(node_id)
        if st is None or cfg is None or cfg.kind == "master":
            return False
        st.last_status_at = now
        if status.get("health") is not None:
            st.health = status["health"]
            st.last_health_at = now
        if cfg.kind != "roaming":
            return True
        st.fix = status["fix"]
        if status["fix"]:
            st.lat, st.lon = status["lat"], status["lon"]
        st.alt, st.speed, st.track, st.sats = status["alt"], status["speed"], status["track"], status["sats"]
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

    def gps_fix(self, node_id: str) -> Optional[tuple[float, float]]:
        """The live GPS fix of a roaming station, or None. Fixed and master
        stations have no GPS feed, so they always return None."""
        cfg = self.config.get(node_id)
        if cfg is None or cfg.kind != "roaming":
            return None
        st = self.state[node_id]
        if st.fix and st.lat is not None and st.lon is not None:
            return st.lat, st.lon
        return None

    def describe(self, node_id: str, now: float, link: str) -> dict:
        cfg = self.config[node_id]
        st = self.state[node_id]
        # The master has no node_agent position of its own; the server places it.
        pos = None if cfg.kind == "master" else self.position(node_id)
        status_age = None if st.last_status_at is None else now - st.last_status_at
        return {
            "id": cfg.id,
            "name": cfg.name,
            "number": cfg.number,
            "kind": cfg.kind,
            "wifi": _sniffer_color(st, now) if cfg.kind != "master" else None,
            "bluetooth": _sniffer_color(st, now, "bluetooth") if cfg.kind != "master" else None,
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


def _sniffer_state(value) -> str:
    """A sniffer's state as the agent reports it. Older agents sent true/false
    for "running"; those read as green/red."""
    if value is True:
        return "green"
    if value is False:
        return "red"
    return value if value in ("green", "yellow", "red") else "red"


def _sniffer_color(st: "_NodeState", now: float, key: str = "wifi") -> str:
    """The sniffer's state at the station's last report: green (sending), yellow
    (connected but quiet), or red (not connected). Red as well if no report has
    arrived lately, since the agent may be down."""
    if st.health is None or st.last_health_at is None or now - st.last_health_at > STATUS_STALE_S:
        return "red"
    return st.health.get(key) or "red"


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