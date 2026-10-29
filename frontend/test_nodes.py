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
m = nodes.parse_nodes({"stations": [{"id": 1, "ip": "100.100.10.90", "lat": 40.1, "lon": -100.2}]}, MASTER_ADDR)
check("master is implied and first", m[0].id == "master" and m[0].bursts == MASTER_ADDR and m[0].number is None)
check("a station with lat/lon is static", m[1].kind == "static" and (m[1].lat, m[1].lon) == (40.1, -100.2))
check("station id is node-N with number N", m[1].id == "node-1" and m[1].number == 1)
check("default name is Node N", m[1].name == "Node 1")
check("ports come from the ip", m[1].bursts == "tcp://100.100.10.90:4224" and m[1].status == "tcp://100.100.10.90:4230")
r2 = nodes.parse_nodes({"stations": [{"id": 2, "ip": "100.100.10.91"}]}, MASTER_ADDR)[1]
check("no position -> roaming with a GPS beacon", r2.kind == "roaming" and r2.status == "tcp://100.100.10.91:4230" and r2.lat is None)
check("id may be given as digits", nodes.parse_nodes({"stations": [{"id": "3", "ip": "100.100.10.92"}]}, MASTER_ADDR)[1].id == "node-3")
check("the master may not be listed", "is not allowed" in (raises(nodes.parse_nodes, {"stations": [
    {"id": "master", "ip": "100.100.10.90"}]}, MASTER_ADDR) or ""))
check("an old file's name field is refused", "unknown field 'name'" in (raises(nodes.parse_nodes, {"stations": [
    {"id": 1, "ip": "100.100.10.90", "name": "Station A"}]}, MASTER_ADDR) or ""))
check("an old file's kind field is refused", "unknown field 'kind'" in (raises(nodes.parse_nodes, {"stations": [
    {"id": 1, "ip": "100.100.10.90", "kind": "static"}]}, MASTER_ADDR) or ""))
check("an old file's bursts field is refused", "unknown field 'bursts'" in (raises(nodes.parse_nodes, {"stations": [
    {"id": 1, "ip": "100.100.10.90", "bursts": "tcp://100.100.10.90:4224"}]}, MASTER_ADDR) or ""))
check("the old nodes key is refused", "unknown top-level field" in (raises(nodes.parse_nodes, {"nodes": [
    {"id": 1, "ip": "100.100.10.90"}]}, MASTER_ADDR) or ""))
check("text ids are refused", "from 1 to 999" in (raises(nodes.parse_nodes, {"stations": [
    {"id": "highway", "ip": "100.100.10.90"}]}, MASTER_ADDR) or ""))
check("id 0 refused", "from 1 to 999" in (raises(nodes.parse_nodes, {"stations": [
    {"id": 0, "ip": "100.100.10.90"}]}, MASTER_ADDR) or ""))
check("id 1000 refused", "from 1 to 999" in (raises(nodes.parse_nodes, {"stations": [
    {"id": 1000, "ip": "100.100.10.90"}]}, MASTER_ADDR) or ""))
check("station without an id refused", "from 1 to 999" in (raises(nodes.parse_nodes, {"stations": [
    {"ip": "100.100.10.90"}]}, MASTER_ADDR) or ""))
check("station without an ip refused", "ip must be" in (raises(nodes.parse_nodes, {"stations": [
    {"id": 1}]}, MASTER_ADDR) or ""))
check("lat without lon refused", "given together" in (raises(nodes.parse_nodes, {"stations": [
    {"id": 1, "ip": "100.100.10.90", "lat": 40}]}, MASTER_ADDR) or ""))
check("latitude out of range refused", "out of range" in (raises(nodes.parse_nodes, {"stations": [
    {"id": 1, "ip": "100.100.10.90", "lat": 95, "lon": 0}]}, MASTER_ADDR) or ""))
check("duplicate id refused", "more than once" in (raises(nodes.parse_nodes, {"stations": [
    {"id": 1, "ip": "100.100.10.90"}, {"id": "1", "ip": "100.100.10.91"}]}, MASTER_ADDR) or ""))
check("duplicate ip refused", "more than once" in (raises(nodes.parse_nodes, {"stations": [
    {"id": 1, "ip": "100.100.10.90"}, {"id": 2, "ip": "100.100.10.90"}]}, MASTER_ADDR) or ""))
check("a bad ip refused", "ip must be" in (raises(nodes.parse_nodes, {"stations": [
    {"id": 1, "ip": "tcp://100.100.10.90:4224"}]}, MASTER_ADDR) or ""))

print("\n[the shipped example config]")
ex = nodes.parse_nodes(json.load(open(os.path.join(HERE, "nodes.example.json"))), MASTER_ADDR)
hw = {n.id: n for n in ex}.get("node-1")
check("example station 1 is static", hw is not None and hw.number == 1 and hw.kind == "static")
check("station 1 starting position", hw is not None and (hw.lat, hw.lon) == (40.0, -100.0))
check("station 1 reports from its address", hw is not None and hw.bursts == "tcp://100.64.0.10:4224")

print("\n[node status reports]")
ok = nodes.parse_status({"type": "node_status", "fix": True, "lat": 40.0, "lon": -100.1, "alt": 12.5})
check("valid fix parses", ok and ok["fix"] and ok["lat"] == 40.0, ok)
check("zero/zero is treated as no fix", nodes.parse_status(
    {"type": "node_status", "fix": True, "lat": 0, "lon": 0})["fix"] is False)
check("wrong type ignored", nodes.parse_status({"type": "other"}) is None)
check("out-of-range fix ignored", nodes.parse_status(
    {"type": "node_status", "fix": True, "lat": 200, "lon": 0}) is None)
check("non-dict ignored", nodes.parse_status("nonsense") is None)

print("\n[registry]")
reg = nodes.NodeRegistry(nodes.parse_nodes({"stations": [
    {"id": 1, "ip": "100.100.10.90", "lat": 40.1, "lon": -100.2},
    {"id": 2, "ip": "100.100.10.91"},
]}, MASTER_ADDR))
now = 1000.0
check("static position comes from config", reg.position("node-1") == (40.1, -100.2))
check("roaming with no report has no position", reg.position("node-2") is None)
check("unknown station cannot add itself", reg.note_status("node-9", ok | {"alt": None, "speed": None, "track": None, "sats": None}, now) is False)
reg.note_status("node-1", {"fix": True, "lat": 1, "lon": 1, "alt": None, "speed": None, "track": None, "sats": None}, now)
check("a static station can't be moved by a report", reg.position("node-1") == (40.1, -100.2))
reg.note_status("node-2", ok | {"alt": 12.5, "speed": 4.0, "track": 90.0, "sats": 9}, now)
check("roaming moves with its fix", reg.position("node-2") == (40.0, -100.1))
d = reg.describe("node-2", now + 10, "green")
check("fresh report is not stale", d["stale"] is False and d["status_age"] == 10.0, d)
d = reg.describe("node-2", now + nodes.STATUS_STALE_S + 1, "green")
check("old report is stale but keeps last position", d["stale"] is True and d["lat"] == 40.0, d)
reg.note_status("node-2", {"fix": False, "lat": None, "lon": None, "alt": None, "speed": None, "track": None, "sats": 0}, now + 5)
check("losing the fix keeps the last known position", reg.position("node-2") == (40.0, -100.1))
check("master position comes from the home point", reg.describe("node-1", now, "green")["lat"] == 40.1)

print("\n[sniffer health on the map]")
import node_agent as na
w = na.SnifferWatch("wifi", "127.0.0.1:1", timeout=12.0)
check("a sniffer not connected is red", w.status(100.0) == "red")
w.note_connected(True)
check("connected but never heard from is yellow", w.status(100.0) == "yellow")
w.note_message(100.0)
check("messages arriving is green", w.status(105.0) == "green")
check("quiet past the timeout is yellow", w.status(113.0) == "yellow")
w.note_connected(False)
check("connection lost is red even with recent messages", w.status(101.0) == "red")
bt = na.SnifferWatch("bluetooth", "127.0.0.1:2")
check("agent health covers both sniffers", na.sniffer_health({"wifi": w, "bluetooth": bt}, now=100.0)
      == {"wifi": "red", "bluetooth": "red"})
rep_h = na.build_status(fix=None, now=5.0, health={"wifi": "green", "bluetooth": "yellow"})
check("agent reports sniffer states", rep_h["health"] == {"wifi": "green", "bluetooth": "yellow"}, rep_h)
check("master reads the states from a report", nodes.parse_status(rep_h)["health"] == {"wifi": "green", "bluetooth": "yellow"})
check("an older agent's true/false still reads",
      nodes.parse_status(na.build_status(fix=None, now=5.0, health={"wifi": True, "bluetooth": False}))["health"]
      == {"wifi": "green", "bluetooth": "red"})
check("a report without health is accepted", nodes.parse_status(na.build_status(fix=None, now=5.0))["health"] is None)
hreg = nodes.NodeRegistry(nodes.parse_nodes({"stations": [
    {"id": 1, "ip": "100.100.10.90", "lat": 40.1, "lon": -100.2},
    {"id": 2, "ip": "100.100.10.91"}]}, MASTER_ADDR))
hreg.note_status("node-1", nodes.parse_status(rep_h), now)
d1 = hreg.describe("node-1", now + 1, "green")
check("a fixed station's health is kept", d1["wifi"] == "green" and d1["bluetooth"] == "yellow", d1)
check("fixed station still has a fixed position", hreg.position("node-1") == (40.1, -100.2))
check("the number is on the station", d1["number"] == 1, d1)
d2 = hreg.describe("node-2", now + 1, "green")
check("no report yet reads as red", d2["wifi"] == "red" and d2["bluetooth"] == "red", d2)
hreg.note_status("node-2", nodes.parse_status(na.build_status(fix=None, now=5.0, health={"wifi": "green", "bluetooth": "green"})), now)
check("a roaming station's health is kept", hreg.describe("node-2", now + 1, "green")["bluetooth"] == "green")
check("an old report stops counting", hreg.describe("node-2", now + nodes.STATUS_STALE_S + 5, "green")["wifi"] == "red")
check("the master has no sniffer lights", hreg.describe("master", now, "green")["wifi"] is None)

print("\n[cross-station duplicates]")
recent = []
nodes.remember_point(recent, "master", 40.0, -100.1, 555, now)
check("same broadcast from another station is a duplicate",
      nodes.is_cross_station_duplicate(recent, "node-1", 40.0, -100.1, 555, now + 0.3))
check("same station repeating a stamped broadcast is a duplicate",
      nodes.is_cross_station_duplicate(recent, "master", 40.0, -100.1, 555, now + 0.3))
check("a later Remote ID timestamp is a new point",
      not nodes.is_cross_station_duplicate(recent, "node-1", 40.0, -100.1, 556, now + 0.3))
check("a different position is a new point",
      not nodes.is_cross_station_duplicate(recent, "node-1", 40.001, -100.1, 555, now + 0.3))
recent2 = []
nodes.remember_point(recent2, "master", 40.0, -100.1, None, now)
check("unstamped point within the window is a duplicate",
      nodes.is_cross_station_duplicate(recent2, "node-1", 40.0, -100.1, None, now + 1.0))
check("unstamped point after the window is not",
      not nodes.is_cross_station_duplicate(recent2, "node-1", 40.0, -100.1, None, now + 10))

print("\n[node beacon: gpsd parsing and reports]")
import json, socket, threading
import node_agent as na
tpv_fix = json.dumps({"class": "TPV", "mode": 3, "lat": 40.01, "lon": -100.15, "altMSL": 12.0, "speed": 3.5, "track": 88.0})
tpv_nofix = json.dumps({"class": "TPV", "mode": 1})
sky = json.dumps({"class": "SKY", "uSat": 7})
check("3D fix parses", na.parse_gpsd_line(tpv_fix)["fix"] is True and na.parse_gpsd_line(tpv_fix)["lat"] == 40.01)
check("no-fix TPV has no position", na.parse_gpsd_line(tpv_nofix)["fix"] is False and na.parse_gpsd_line(tpv_nofix)["lat"] is None)
check("satellite count parses", na.parse_gpsd_line(sky) == {"kind": "SKY", "sats": 7})
check("other gpsd classes ignored", na.parse_gpsd_line(json.dumps({"class": "VERSION"})) is None)
check("garbage ignored", na.parse_gpsd_line("not json") is None)
rep_fix = na.build_status(fix=na.parse_gpsd_line(tpv_fix), sats=7, now=5.0)
check("roaming report with a fix", rep_fix["fix"] and rep_fix["lat"] == 40.01 and rep_fix["sats"] == 7, rep_fix)
rep_nofix = na.build_status(fix=na.parse_gpsd_line(tpv_nofix), now=5.0)
check("roaming report without a fix carries no position", rep_nofix["fix"] is False and rep_nofix["lat"] is None)
rep_static = na.build_status(static=(40.1, -100.2), now=5.0)
check("fixed node always reports its set position", rep_static["fix"] and rep_static["lat"] == 40.1)
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
main.fleet = nodes.NodeRegistry(nodes.parse_nodes({"stations": [
    {"id": 1, "ip": "100.100.10.90", "lat": 40.1, "lon": -100.2}]}, MASTER_ADDR))
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
    await hear("master", 40.0, -100.1, "1000")
    await hear("node-1", 40.0, -100.1, "1000")      # same broadcast, heard by north
    await hear("master", 40.0, -100.1, "1000")     # master repeats itself: not stored again
    await hear("node-1", 40.0005, -100.1005, "1001")  # the drone has moved on
asyncio.run(scenario())
pts = points()
check("each broadcast is stored once, whichever station or repeat it came from",
      [p["station_id"] for p in pts] == ["master", "node-1"], pts)
conn = db._require_conn()
first_stations = conn.execute("SELECT stations FROM track_points ORDER BY id LIMIT 1").fetchone()[0]
check("the stored point lists both stations that heard it",
      json.loads(first_stations) == ["master", "node-1"], first_stations)
t = main.store.tracks["SER-MULTI"]
check("track remembers every station that heard it", t.seen_by == ["master", "node-1"], t.seen_by)
check("track's latest station is the last one to report", t.station_id == "node-1", t.station_id)
snap = t.to_dict()
check("recent_points stays out of what clients receive", "recent_points" not in snap)
check("seen_by reaches clients", snap["seen_by"] == ["master", "node-1"], snap.get("seen_by"))

print("\n[schema]")
cols = {r[1] for r in db._require_conn().execute("PRAGMA table_info(track_points)")}
check("track_points has station_id", "station_id" in cols)

print("\n[stations for the map]")
main.station_cache = {"lat": 40.0, "lon": -100.1}
snap = main.stations_snapshot(now=time.time())
by_id = {s["id"]: s for s in snap}
check("every configured station is listed", set(by_id) == {"master", "node-1"}, list(by_id))
check("master sits at the home point", by_id["master"]["lat"] == 40.0 and by_id["master"]["kind"] == "master")
check("static node sits at its configured point", by_id["node-1"]["lat"] == 40.1 and by_id["node-1"]["stale"] is False)
check("master link follows the local feed", by_id["master"]["link"] in ("red", "green", "yellow"))

print("\n[health alerts]")
HN = {"master": "Master", "node-1": "Node 1"}
H_UP = {"decoder": True, "agent": True, "wifi": True, "bluetooth": True}
H_MUP = {"decoder": True, "wifi": True, "bluetooth": True}
H_OFF = {"decoder": False, "agent": False, "wifi": None, "bluetooth": None}

def health_run(timeline, until, grace=0):
    """Steps a tracker every 5 s like the monitor; returns (time, message) pairs."""
    tr = main.HealthTracker(60, 10)
    state = {"master": dict(H_MUP), "node-1": dict(H_UP)}
    out, tick = [], 0
    while tick <= until:
        for at, sid, change in timeline:
            if at == tick:
                state[sid].update(change)
        out += [(tick, m) for m in tr.step({k: dict(v) for k, v in state.items()}, HN, tick, tick >= grace)]
        tick += 5
    return out

check("a short blip sends nothing",
      health_run([(10, "node-1", {"wifi": False}), (30, "node-1", {"wifi": True})], 150) == [])
msgs = health_run([(10, "node-1", {"wifi": False}), (120, "node-1", {"wifi": True})], 160)
check("one sniffer down is a warning, then one recovery",
      [m for _, m in msgs] == [
          "## \u26a0\ufe0f Node 1 Wi-Fi Sniffer Down\n```Wi-Fi sniffer not reporting - Decoder, agent and Bluetooth sniffer still up```",
          "## \u2705 Node 1 Wi-Fi Sniffer Online\n```All components reporting```"], msgs)
check("the warning waits for the debounce", msgs[0][0] == 70, msgs)
msgs = health_run([(10, "node-1", {"decoder": False}), (20, "node-1", H_OFF),
                   (200, "node-1", {"decoder": True}), (300, "node-1", H_UP)], 340)
check("a whole node down is one offline message; a partial return then the rest",
      [m for _, m in msgs] == [
          "## \u274c Node 1 Offline\n```Decoder and agent not reporting```",
          "## \u2705 Node 1 Online\n```Decoder online - Agent offline```",
          "## \u2705 Node 1 Agent Online\n```All components reporting```"], msgs)
msgs = health_run([(10, "node-1", H_OFF), (200, "node-1", {"decoder": True, "agent": True, "wifi": True, "bluetooth": False}),
                   (400, "node-1", {"bluetooth": True})], 440)
check("a part named offline in a recovery gets only its own online message later",
      [m for _, m in msgs][1:] == [
          "## \u2705 Node 1 Online\n```Decoder and agent online - Bluetooth sniffer offline```",
          "## \u2705 Node 1 Bluetooth Sniffer Online\n```All components reporting```"], msgs)
msgs = health_run([(0, "node-1", H_OFF)], 150, grace=120)
check("nothing during the start-up grace; still-down stations reported after it",
      [t_ for t_, _ in msgs] == [120] and "Offline" in msgs[0][1], msgs)
check("the master's full outage names all three",
      health_run([(10, "master", {"decoder": False, "wifi": False, "bluetooth": False})], 90)[0][1]
      == "## \u274c Master Offline\n```Decoder, Wi-Fi and Bluetooth sniffers not reporting```")

hnow = time.time()
main.connection_state["zmq"] = main.CONNECTED
main.connection_state["node:node-1"] = main.DISCONNECTED
main.fleet.state["node-1"].last_status_at = None
h = main.station_health(hnow)
check("a remote station's sniffers are unknown while its agent is silent",
      h["node-1"] == {"decoder": False, "agent": False, "wifi": None, "bluetooth": None}, h["node-1"])
main.fleet.note_status("node-1", nodes.parse_status(na.build_status(
    fix=None, now=hnow, health={"wifi": "green", "bluetooth": "red"})), hnow)
main.connection_state["node:node-1"] = main.CONNECTED
h = main.station_health(hnow + 1)
check("a reporting agent gives real sniffer states",
      h["node-1"] == {"decoder": True, "agent": True, "wifi": True, "bluetooth": False}, h["node-1"])
check("the master has a decoder and two sniffers, no agent", set(h["master"]) == {"decoder", "wifi", "bluetooth"}, h["master"])

print()
if fails:
    print(f"{len(fails)} FAILED: {fails}")
    sys.exit(1)
print("ALL PASS")