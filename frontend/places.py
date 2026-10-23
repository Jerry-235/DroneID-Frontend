"""Place names for a drone's first detected position, for the Discord alert.

Two sources, checked in order:

1. landmarks.csv next to this file — hand-curated names ("Landmark A", "Landmark B",
   "Landmark C"). If the point falls inside a landmark's radius, the nearest
   such landmark wins. Always preferred, because OpenStreetMap doesn't use
   the site's internal names.
2. OpenStreetMap via Nominatim reverse geocoding — used only when no landmark
   covers the point. Nominatim's usage policy: at most one request per second,
   an identifying User-Agent, and caching. This module does all three.

Only the drone's own coordinates are ever sent out. The station's position
never is.

Results are cached in memory by a ~100 m grid cell, so a restart clears the
cache. Failed or timed-out lookups aren't cached, so they retry next time.
"""
import asyncio
import csv
import json
import logging
import math
import os
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger("droneid-frontend.places")

HERE = os.path.dirname(os.path.realpath(__file__))
LANDMARKS_PATH = os.environ.get("DRONEID_LANDMARKS_PATH") or os.path.join(HERE, "landmarks.csv")
# Radius used for landmarks whose CSV row leaves radius_m blank.
DEFAULT_RADIUS_M = float(os.environ.get("DRONEID_LANDMARK_RADIUS_M") or 150)
NOMINATIM_URL = os.environ.get("DRONEID_NOMINATIM_URL") or "https://nominatim.openstreetmap.org/reverse"
# Nominatim asks for a way to contact you if something goes wrong. Set this to
# an email address or a URL you control.
GEOCODER_CONTACT = (os.environ.get("DRONEID_GEOCODER_CONTACT") or "").strip()
# Hard ceiling on how long an alert waits for a lookup. Past this, the alert
# posts without a place name.
LOOKUP_TIMEOUT_S = float(os.environ.get("DRONEID_PLACE_TIMEOUT_S") or 2.0)
NOMINATIM_MIN_INTERVAL_S = 1.1  # the policy is 1/s; a little headroom

EARTH_R_M = 6371000.0


@dataclass(frozen=True)
class Landmark:
    name: str
    lat: float
    lon: float
    radius_m: float


@dataclass(frozen=True)
class Place:
    name: str


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = p2 - p1
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return EARTH_R_M * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def load_landmarks(path: str) -> list[Landmark]:
    """Reads the landmark CSV. Columns: name, lat, lon, and optionally
    radius_m. A missing file isn't an error — lookups just go to OSM. A bad
    row is skipped with a warning, so one typo doesn't drop the whole list."""
    if not os.path.exists(path):
        log.warning("No landmark file at %s — every alert will use OSM names", path)
        return []
    out: list[Landmark] = []
    # utf-8-sig: a spreadsheet's "CSV UTF-8" export starts with a BOM, which
    # would otherwise rename the first header and silently skip every row.
    with open(path, newline="", encoding="utf-8-sig") as f:
        for lineno, row in enumerate(csv.DictReader(f), start=2):
            try:
                name = (row.get("name") or "").strip()
                if not name:
                    raise ValueError("empty name")
                radius = (row.get("radius_m") or "").strip()
                out.append(Landmark(
                    name=name,
                    lat=float(row["lat"]),
                    lon=float(row["lon"]),
                    radius_m=float(radius) if radius else DEFAULT_RADIUS_M,
                ))
            except (KeyError, ValueError) as e:
                log.warning("Skipping landmark row %d in %s: %s", lineno, path, e)
    log.info("Loaded %d landmarks from %s", len(out), path)
    return out


_landmarks: list[Landmark] = load_landmarks(LANDMARKS_PATH)


def nearest_landmark(lat: float, lon: float, landmarks: Optional[list[Landmark]] = None) -> Optional[str]:
    """Name of the nearest landmark whose radius covers the point, or None."""
    best_name, best_d = None, None
    for lm in (_landmarks if landmarks is None else landmarks):
        d = haversine_m(lat, lon, lm.lat, lm.lon)
        if d <= lm.radius_m and (best_d is None or d < best_d):
            best_name, best_d = lm.name, d
    return best_name


# ---- OpenStreetMap fallback ----

_cache: dict[tuple[float, float], Optional[str]] = {}
_CACHE_MAX = 5000
_last_call = 0.0
_rate_lock: Optional[asyncio.Lock] = None
_warned_no_contact = False


def _cell(lat: float, lon: float) -> tuple[float, float]:
    # Three decimal places is roughly 100 m of latitude.
    return round(lat, 3), round(lon, 3)


def _user_agent() -> str:
    global _warned_no_contact
    if not GEOCODER_CONTACT and not _warned_no_contact:
        log.warning("DRONEID_GEOCODER_CONTACT is not set — Nominatim asks for one in the User-Agent")
        _warned_no_contact = True
    ua = "DroneID-Frontend/1.0"
    return f"{ua} ({GEOCODER_CONTACT})" if GEOCODER_CONTACT else ua


def _osm_name(data: dict) -> Optional[str]:
    """Most specific useful name in a Nominatim reverse result. The 'name'
    field is the object itself (a building, a POI, a road); the address parts
    are fallbacks for places with no name of their own."""
    name = (data.get("name") or "").strip()
    if name:
        return name
    addr = data.get("address") or {}
    for key in ("building", "amenity", "tourism", "road", "neighbourhood", "suburb",
                "village", "town", "city", "county"):
        if addr.get(key):
            return str(addr[key])
    return None


def _fetch_nominatim(lat: float, lon: float) -> dict:
    query = urllib.parse.urlencode({
        "lat": f"{lat:.6f}",
        "lon": f"{lon:.6f}",
        "zoom": 18,
        "format": "jsonv2",
        "addressdetails": 1,
    })
    req = urllib.request.Request(
        f"{NOMINATIM_URL}?{query}",
        headers={"User-Agent": _user_agent(), "Accept-Language": "en"},
    )
    with urllib.request.urlopen(req, timeout=LOOKUP_TIMEOUT_S) as resp:
        return json.loads(resp.read().decode("utf-8"))


async def _osm_lookup(lat: float, lon: float) -> Optional[str]:
    global _last_call, _rate_lock
    if _rate_lock is None:
        _rate_lock = asyncio.Lock()
    async with _rate_lock:
        wait = NOMINATIM_MIN_INTERVAL_S - (time.monotonic() - _last_call)
        if wait > 0:
            await asyncio.sleep(wait)
        try:
            data = await asyncio.to_thread(_fetch_nominatim, lat, lon)
        finally:
            _last_call = time.monotonic()
    return _osm_name(data)


async def resolve(lat: Optional[float], lon: Optional[float],
                  timeout: Optional[float] = None) -> Optional[Place]:
    """The place name for a position, or None. Never raises, and never takes
    longer than the timeout — callers can await it on the alert path without
    holding anything up."""
    if lat is None or lon is None:
        return None
    name = nearest_landmark(lat, lon)
    if name:
        return Place(name)

    key = _cell(lat, lon)
    if key in _cache:
        cached = _cache[key]
        return Place(cached) if cached else None
    try:
        name = await asyncio.wait_for(_osm_lookup(lat, lon), timeout or LOOKUP_TIMEOUT_S)
    except asyncio.TimeoutError:
        log.info("Place lookup timed out for %.5f,%.5f — posting without a name", lat, lon)
        return None
    except Exception:
        log.exception("Place lookup failed for %.5f,%.5f", lat, lon)
        return None
    if len(_cache) >= _CACHE_MAX:
        _cache.clear()  # bounded: a long-running station can't grow this without limit
    _cache[key] = name  # cache "nothing here" too, so an empty spot isn't re-asked
    return Place(name) if name else None