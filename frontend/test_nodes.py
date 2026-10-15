"""Tests for multi-node support (nodes.py and the station-aware ingest path).

    python3 test_nodes.py

Needs the server's dependencies (pyzmq, fastapi). If they aren't installed,
point DRONEID_TEST_STUBS at a stubs.py that fakes them. Uses a throwaway
SQLite file; nothing touches droneid.db.
"""
import asyncio, json, os, sys, tempfile, time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
stubs_path = os.environ.get("DRONEID_TEST_STUBS")
if stubs_path:
    sys.path.insert(0, os.path.dirname(stubs_path))
    import stubs; stubs.install()

import db
import main
import nodes

fails = []
def check(label, cond, extra=""):
    print(f"  {'PASS' if cond else 'FAIL'}  {label}{('  ' + str(extra)) if extra and not cond else ''}")
    if not cond:
        fails.append(label)

def raises(fn, *args):
    try:
        fn(*args)
    except nodes.ConfigError as e:
        return str(e)
    return None

MASTER_ADDR = "tcp://127.0.0.1:4224"

print("\n[nodes.json parsing]")
check("no nodes.json -> a single master on the local decoder",
      [n.id for n in nodes.load_nodes("/nonexistent/nodes.json", MASTER_ADDR)] == ["master"])
m = nodes.parse_nodes({"nodes": [
    {"id": "north", "kind": "static", "bursts": "tcp://100.100.10.90:4224", "lat": 26.1, "lon": -97.2}]}, MASTER_ADDR)
check("master is added even when the file leaves it out", m[0].id == "master" and m[0].bursts == MASTER_ADDR)
check("static node keeps its position", (m[1].lat, m[1].lon) == (26.1, -97.2))
check("explicit master gets the local decoder by default",
      nodes.parse_nodes({"nodes": [{"id": "master", "kind": "master"}]}, MASTER_ADDR)[0].bursts == MASTER_ADDR)
check("bad id rejected", "lowercase" in (raises(nodes.parse_nodes, {"nodes": [{"id": "North Node", "kind": "roaming", "bursts": MASTER_ADDR}]}, MASTER_ADDR) or ""))
check("duplicate id rejected", "more than once" in (raises(nodes.parse_nodes, {"nodes": [
    {"id": "a", "kind": "roaming", "bursts": MASTER_ADDR}, {"id": "a", "kind": "roaming", "bursts": MASTER_ADDR}]}, MASTER_ADDR) or ""))
check("endpoint without tcp:// rejected", "tcp://" in (raises(nodes.parse_nodes, {"nodes": [
    {"id": "a", "kind": "roaming", "bursts": "100.100.10.90:4224"}]}, MASTER_ADDR) or ""))
check("static node without a position rejected", "needs lat and lon" in (raises(nodes.parse_nodes, {"nodes": [
    {"id": "a", "kind": "static", "bursts": MASTER_ADDR}]}, MASTER_ADDR) or ""))
check("latitude out of range rejected", "out of range" in (raises(nodes.parse_nodes, {"nodes": [
    {"id": "a", "kind": "static", "bursts": MASTER_ADDR, "lat": 95, "lon": 0}]}, MASTER_ADDR) or ""))
check("unknown kind rejected", "kind must be" in (raises(nodes.parse_nodes, {"nodes": [
    {"id": "a", "kind": "drone", "bursts": MASTER_ADDR}]}, MASTER_ADDR) or ""))

print("\n[the shipped example config]")
ex = nodes.parse_nodes(json.load(open(os.path.join(HERE, "nodes.example.json"))), MASTER_ADDR)
hw = {n.id: n for n in ex}.get("highway")
check("example includes the Highway station", hw is not None and hw.name == "Highway" and hw.kind == "static")
check("Highway sits at its surveyed position", hw is not None and (hw.lat, hw.lon) == (25.992678, -97.176886))
check("Highway reports from the Tailscale address", hw is not None and hw.bursts == "tcp://100.100.10.90:4224")

print("\n[node status reports]")
ok = nodes.parse_status({"type": "node_status", "node": "rover", "fix": True, "lat": 26.0, "lon": -97.1, "alt": 12.5})
check("valid fix parses", ok and ok["fix"] and ok["lat"] == 26.0, ok)
check("zero/zero is treated as no fix", nodes.parse_status(
    {"type": "node_status", "node": "rover", "fix": True, "lat": 0, "lon": 0})["fix"] is False)
check("wrong type ignored", nodes.parse_status({"type": "other", "node": "rover"}) is None)
check("bad node id ignored", nodes.parse_status({"type": "node_status", "node": "../etc"}) is None)
check("out-of-range fix ignored", nodes.parse_status(
    {"type": "node_status", "node": "rover", "fix": True, "lat": 200, "lon": 0}) is None)
check("non-dict ignored", nodes.parse_status("nonsense") is None)

print("\n[registry]")
reg = nodes.NodeRegistry(nodes.parse_nodes({"nodes": [
    {"id": "north", "kind": "static", "bursts": "tcp://100.100.10.90:4224", "lat": 26.1, "lon": -97.2},
    {"id": "rover", "kind": "roaming", "bursts": "tcp://100.100.10.91:4224", "status": "tcp://100.100.10.91:4230"},
]}, MASTER_ADDR))
now = 1000.0
check("static position comes from config", reg.position("north") == (26.1, -97.2))
check("roaming with no report has no position", reg.position("rover") is None)
check("unknown sender cannot add itself", reg.note_status(
    {"node": "stranger", "fix": True, "lat": 1, "lon": 1, "alt": None, "speed": None, "track": None, "sats": None}, now) is False)
check("a static node can't be moved by a report", reg.note_status(
    {"node": "north", "fix": True, "lat": 1, "lon": 1, "alt": None, "speed": None, "track": None, "sats": None}, now) is False)
reg.note_status(ok | {"alt": 12.5, "speed": 4.0, "track": 90.0, "sats": 9}, now)
check("roaming moves with its fix", reg.position("rover") == (26.0, -97.1))
d = reg.describe("rover", now + 10, "green")
check("fresh report is not stale", d["stale"] is False and d["status_age"] == 10.0, d)
d = reg.describe("rover", now + nodes.STATUS_STALE_S + 1, "green")
check("old report is stale but keeps last position", d["stale"] is True and d["lat"] == 26.0, d)
reg.note_status({"node": "rover", "fix": False, "lat": None, "lon": None, "alt": None, "speed": None, "track": None, "sats": 0}, now + 5)
check("losing the fix keeps the last known position", reg.position("rover") == (26.0, -97.1))
check("master position comes from the home point", reg.describe("north", now, "green", None)["lat"] == 26.1)

print("\n[cross-station duplicates]")
recent = []
nodes.remember_point(recent, "master", 26.0, -97.1, 555, now)
check("same broadcast from another station is a duplicate",
      nodes.is_cross_station_duplicate(recent, "north", 26.0, -97.1, 555, now + 0.3))
check("same station repeating itself is NOT a duplicate",
      not nodes.is_cross_station_duplicate(recent, "master", 26.0, -97.1, 555, now + 0.3))
check("a later Remote ID timestamp is a new point",
      not nodes.is_cross_station_duplicate(recent, "north", 26.0, -97.1, 556, now + 0.3))
check("a different position is a new point",
      not nodes.is_cross_station_duplicate(recent, "north", 26.001, -97.1, 555, now + 0.3))
recent2 = []
nodes.remember_point(recent2, "master", 26.0, -97.1, None, now)
check("unstamped point within the window is a duplicate",
      nodes.is_cross_station_duplicate(recent2, "north", 26.0, -97.1, None, now + 1.0))
check("unstamped point after the window is not",
      not nodes.is_cross_station_duplicate(recent2, "north", 26.0, -97.1, None, now + 10))

print("\n[node beacon: gpsd parsing and reports]")
import json, socket, threading
import node_agent as na
tpv_fix = json.dumps({"class": "TPV", "mode": 3, "lat": 26.01, "lon": -97.15, "altMSL": 12.0, "speed": 3.5, "track": 88.0})
tpv_nofix = json.dumps({"class": "TPV", "mode": 1})
sky = json.dumps({"class": "SKY", "uSat": 7})
check("3D fix parses", na.parse_gpsd_line(tpv_fix)["fix"] is True and na.parse_gpsd_line(tpv_fix)["lat"] == 26.01)
check("no-fix TPV has no position", na.parse_gpsd_line(tpv_nofix)["fix"] is False and na.parse_gpsd_line(tpv_nofix)["lat"] is None)
check("satellite count parses", na.parse_gpsd_line(sky) == {"kind": "SKY", "sats": 7})
check("other gpsd classes ignored", na.parse_gpsd_line(json.dumps({"class": "VERSION"})) is None)
check("garbage ignored", na.parse_gpsd_line("not json") is None)
rep_fix = na.build_status("rover", "Rover", fix=na.parse_gpsd_line(tpv_fix), sats=7, now=5.0)
check("roaming report with a fix", rep_fix["fix"] and rep_fix["lat"] == 26.01 and rep_fix["sats"] == 7, rep_fix)
rep_nofix = na.build_status("rover", "Rover", fix=na.parse_gpsd_line(tpv_nofix), now=5.0)
check("roaming report without a fix carries no position", rep_nofix["fix"] is False and rep_nofix["lat"] is None)
rep_static = na.build_status("north", "North", static=(26.1, -97.2), now=5.0)
check("fixed node always reports its set position", rep_static["fix"] and rep_static["lat"] == 26.1)
check("a report the master accepts", nodes.parse_status(rep_fix) is not None and nodes.parse_status(rep_nofix) is not None
      and nodes.parse_status(rep_static) is not None)

srv = socket.socket(); srv.bind(("127.0.0.1", 0)); srv.listen(1)
port = srv.getsockname()[1]
def fake_gpsd():
    conn, _ = srv.accept()
    conn.recv(256)                       # the ?WATCH line
    conn.sendall((tpv_fix + "\n" + sky + "\n").encode())
    time.sleep(2)
    conn.close()
threading.Thread(target=fake_gpsd, daemon=True).start()
client = na.GpsdClient("127.0.0.1", port, timeout=2.0)
fix, sats = client.poll(budget_s=1.0)
check("gpsd client reads the fix and satellite count", fix and fix["fix"] and sats == 7, (fix, sats))
client.close()
dead = na.GpsdClient("127.0.0.1", 1, timeout=0.5)
fix, sats = dead.poll(budget_s=0.5)
check("unreachable gpsd reports no fix instead of crashing", fix is None and sats is None)
srv.close()

print("\n[ingest: one drone heard by two stations]")
tmp = tempfile.mkdtemp()
db.init(os.path.join(tmp, "t.db"))
for d_ in (main.store.tracks, main.store.mac_to_key, main.active_flights, main.last_status_sent, main.catalog_nicknames):
    d_.clear()
main.pending_refile.clear()
main.manager.broadcast = lambda *_a, **_k: asyncio.sleep(0)   # no clients in tests
main.fleet = nodes.NodeRegistry(nodes.parse_nodes({"nodes": [
    {"id": "north", "kind": "static", "bursts": "tcp://100.100.10.90:4224", "lat": 26.1, "lon": -97.2}]}, MASTER_ADDR))
main.discord_webhook_url = None

MAC = "aa:bb:cc:00:00:42"
def burst(lat, lon, loc_ts):
    return [
        {"MAC": MAC},
        {"Basic ID": {"id": "SER-MULTI", "id_type": "Serial Number"}},
        {"Location/Vector Message": {"latitude": lat, "longitude": lon, "timestamp": loc_ts,
                                     "geodetic_altitude": "80 m"}},
    ]

async def hear(station, lat, lon, loc_ts):
    msgs = burst(lat, lon, loc_ts)
    mac, cleaned = main.extract_bursts(msgs)[0]
    track = main.store.apply_burst(mac, cleaned, station_id=station)
    await main.persist_update(track)
    return track

def points():
    conn = db._require_conn()
    return [dict(r) for r in conn.execute("SELECT station_id, lat, lon, loc_timestamp FROM track_points ORDER BY id")]

async def scenario():
    await hear("master", 26.0, -97.1, "1000")
    await hear("north", 26.0, -97.1, "1000")      # same broadcast, heard by north
    await hear("master", 26.0, -97.1, "1000")     # master repeats itself: stored, as before
    await hear("north", 26.0005, -97.1005, "1001")  # the drone has moved on
asyncio.run(scenario())
pts = points()
check("a second station's copy of a broadcast is skipped; a station's own repeats are kept",
      [p["station_id"] for p in pts] == ["master", "master", "north"], pts)
t = main.store.tracks["SER-MULTI"]
check("track remembers every station that heard it", t.seen_by == ["master", "north"], t.seen_by)
check("track's latest station is the last one to report", t.station_id == "north", t.station_id)
snap = t.to_dict()
check("recent_points stays out of what clients receive", "recent_points" not in snap)
check("seen_by reaches clients", snap["seen_by"] == ["master", "north"], snap.get("seen_by"))

print("\n[schema]")
cols = {r[1] for r in db._require_conn().execute("PRAGMA table_info(track_points)")}
check("track_points has station_id", "station_id" in cols)

print("\n[stations for the map]")
main.station_cache = {"lat": 26.0, "lon": -97.1}
snap = main.stations_snapshot(now=time.time())
by_id = {s["id"]: s for s in snap}
check("every configured station is listed", set(by_id) == {"master", "north"}, list(by_id))
check("master sits at the home point", by_id["master"]["lat"] == 26.0 and by_id["master"]["kind"] == "master")
check("static node sits at its configured point", by_id["north"]["lat"] == 26.1 and by_id["north"]["stale"] is False)
check("master link follows the local feed", by_id["master"]["link"] in ("red", "green", "yellow"))

print()
if fails:
    print(f"{len(fails)} FAILED: {fails}")
    sys.exit(1)
print("ALL PASS")
