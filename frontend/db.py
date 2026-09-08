"""
Persistence layer for DroneID frontend.

Three tables:
  - drones        catalog: one row per canonical identity (catalog_key),
                   holds the user-assigned nickname. catalog_key is the same
                   "serial if known, else MAC" key Approach A already uses in
                   TrackStore, so renaming here instantly relabels every past
                   and future flight for that drone — nickname is joined in at
                   query time, never copied into flight/point rows.
  - flights       one row per contiguous detection session for a catalog_key.
  - track_points  timestamped samples belonging to a flight.

Plain sqlite3 (stdlib, no extra dependency), single connection, serialized
through an asyncio.Lock + to_thread since this is a single-operator, small-
scale tool — no need for a connection pool or an async DB driver here.
"""

import asyncio
import os
import sqlite3
import time
from typing import Optional

DB_PATH = os.environ.get("DRONEID_DB_PATH", "droneid.db")

_lock = asyncio.Lock()
_conn: Optional[sqlite3.Connection] = None


def _connect(path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS drones (
            catalog_key   TEXT PRIMARY KEY,
            nickname      TEXT,
            notes         TEXT,
            mac           TEXT,
            serial        TEXT,
            registration_id TEXT,
            ua_type       TEXT,
            first_seen    REAL,
            last_seen     REAL
        );

        CREATE TABLE IF NOT EXISTS flights (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            catalog_key   TEXT NOT NULL REFERENCES drones(catalog_key),
            mac           TEXT,
            serial        TEXT,
            started_at    REAL NOT NULL,
            ended_at      REAL,
            last_point_at REAL,
            point_count   INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_flights_catalog ON flights(catalog_key, started_at);

        CREATE TABLE IF NOT EXISTS track_points (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            flight_id   INTEGER NOT NULL REFERENCES flights(id),
            ts          REAL NOT NULL,
            lat REAL, lon REAL, alt REAL, height_agl REAL,
            heading REAL, speed REAL,
            op_lat REAL, op_lon REAL
        );
        CREATE INDEX IF NOT EXISTS idx_points_flight ON track_points(flight_id, ts);
        """
    )
    conn.commit()

    # migration: older DBs created before last_point_at existed
    cols = [r[1] for r in conn.execute("PRAGMA table_info(flights)").fetchall()]
    if "last_point_at" not in cols:
        conn.execute("ALTER TABLE flights ADD COLUMN last_point_at REAL")
        conn.execute("UPDATE flights SET last_point_at = ended_at WHERE last_point_at IS NULL")
        conn.commit()

    return conn


def init(path: str = DB_PATH):
    """Call once at startup. Safe to call again in tests with a fresh path."""
    global _conn, DB_PATH
    DB_PATH = path
    _conn = _connect(path)
    return _conn


def _require_conn() -> sqlite3.Connection:
    if _conn is None:
        return init()
    return _conn


async def run(fn, *args):
    async with _lock:
        return await asyncio.to_thread(fn, *args)


# ---- sync implementations (always called via run()/to_thread) ----

def _upsert_catalog(catalog_key, mac, serial, registration_id, ua_type, ts):
    conn = _require_conn()
    conn.execute(
        """
        INSERT INTO drones (catalog_key, mac, serial, registration_id, ua_type, first_seen, last_seen)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(catalog_key) DO UPDATE SET
            mac=excluded.mac,
            serial=COALESCE(excluded.serial, drones.serial),
            registration_id=COALESCE(excluded.registration_id, drones.registration_id),
            ua_type=COALESCE(excluded.ua_type, drones.ua_type),
            last_seen=excluded.last_seen
        """,
        (catalog_key, mac, serial, registration_id, str(ua_type) if ua_type is not None else None, ts, ts),
    )
    conn.commit()


def _rename_drone(catalog_key, nickname):
    conn = _require_conn()
    conn.execute(
        "INSERT INTO drones (catalog_key, nickname) VALUES (?, ?) "
        "ON CONFLICT(catalog_key) DO UPDATE SET nickname=excluded.nickname",
        (catalog_key, nickname),
    )
    conn.commit()


def _get_catalog() -> dict:
    conn = _require_conn()
    rows = conn.execute("SELECT catalog_key, nickname FROM drones WHERE nickname IS NOT NULL").fetchall()
    return {r["catalog_key"]: r["nickname"] for r in rows}


def _start_flight(catalog_key, mac, serial, ts) -> int:
    conn = _require_conn()
    cur = conn.execute(
        "INSERT INTO flights (catalog_key, mac, serial, started_at) VALUES (?, ?, ?, ?)",
        (catalog_key, mac, serial, ts),
    )
    conn.commit()
    return cur.lastrowid


def _add_point(flight_id, ts, lat, lon, alt, height_agl, heading, speed, op_lat, op_lon):
    conn = _require_conn()
    conn.execute(
        """INSERT INTO track_points (flight_id, ts, lat, lon, alt, height_agl, heading, speed, op_lat, op_lon)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (flight_id, ts, lat, lon, alt, height_agl, heading, speed, op_lat, op_lon),
    )
    # last_point_at reflects the actual span of received data; ended_at (set
    # separately, on drop-detection) can lag well behind it since a flight
    # isn't closed until DROP_AFTER_S of silence has passed.
    conn.execute(
        "UPDATE flights SET point_count = point_count + 1, last_point_at = ? WHERE id = ?",
        (ts, flight_id),
    )
    conn.commit()


def _end_flight(flight_id, ts):
    conn = _require_conn()
    conn.execute("UPDATE flights SET ended_at = ? WHERE id = ?", (ts, flight_id))
    conn.commit()


def _list_flights(limit, offset):
    conn = _require_conn()
    rows = conn.execute(
        """
        SELECT f.id, f.catalog_key, f.mac, f.serial, f.started_at, f.ended_at,
               f.last_point_at, f.point_count,
               COALESCE(d.nickname, f.serial, f.mac, f.catalog_key) AS display_name
        FROM flights f
        LEFT JOIN drones d ON d.catalog_key = f.catalog_key
        ORDER BY f.started_at DESC
        LIMIT ? OFFSET ?
        """,
        (limit, offset),
    ).fetchall()
    return [dict(r) for r in rows]


def _get_flight(flight_id):
    conn = _require_conn()
    row = conn.execute(
        """
        SELECT f.id, f.catalog_key, f.mac, f.serial, f.started_at, f.ended_at,
               f.last_point_at, f.point_count,
               COALESCE(d.nickname, f.serial, f.mac, f.catalog_key) AS display_name
        FROM flights f LEFT JOIN drones d ON d.catalog_key = f.catalog_key
        WHERE f.id = ?
        """,
        (flight_id,),
    ).fetchone()
    return dict(row) if row else None


def _get_flight_points(flight_id):
    conn = _require_conn()
    rows = conn.execute(
        """SELECT ts, lat, lon, alt, height_agl, heading, speed, op_lat, op_lon
           FROM track_points WHERE flight_id = ? ORDER BY ts ASC""",
        (flight_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def _search_flights_for_catalog_key(catalog_key, limit):
    conn = _require_conn()
    rows = conn.execute(
        "SELECT id FROM flights WHERE catalog_key = ? ORDER BY started_at DESC LIMIT ?",
        (catalog_key, limit),
    ).fetchall()
    return [r["id"] for r in rows]


# ---- async-facing API ----

async def upsert_catalog(catalog_key, mac, serial, registration_id, ua_type, ts=None):
    await run(_upsert_catalog, catalog_key, mac, serial, registration_id, ua_type, ts or time.time())


async def rename_drone(catalog_key, nickname):
    await run(_rename_drone, catalog_key, nickname)


async def get_catalog() -> dict:
    return await run(_get_catalog)


async def start_flight(catalog_key, mac, serial, ts=None) -> int:
    return await run(_start_flight, catalog_key, mac, serial, ts or time.time())


async def add_point(flight_id, ts, lat, lon, alt, height_agl, heading, speed, op_lat, op_lon):
    await run(_add_point, flight_id, ts, lat, lon, alt, height_agl, heading, speed, op_lat, op_lon)


async def end_flight(flight_id, ts=None):
    await run(_end_flight, flight_id, ts or time.time())


async def list_flights(limit=50, offset=0):
    return await run(_list_flights, limit, offset)


async def get_flight(flight_id):
    return await run(_get_flight, flight_id)


async def get_flight_points(flight_id):
    return await run(_get_flight_points, flight_id)


async def flights_for_catalog_key(catalog_key, limit=20):
    return await run(_search_flights_for_catalog_key, catalog_key, limit)
