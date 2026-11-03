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

print("\n[station positions on points]")
main.station_cache = {"lat": 40.0, "lon": -100.1}          # the master's home point
def burst_pos(loc_ts):
    return [{"MAC": "11:22:33:44:55:66"},
            {"Basic ID": {"id": "SER-POS", "id_type": "Serial Number"}},
            {"Location/Vector Message": {"latitude": 40.05, "longitude": -100.15, "timestamp": loc_ts,
                                         "geodetic_altitude": "80 m"}}]
async def hear_pos(station, loc_ts):
    mac, cleaned = main.extract_bursts(burst_pos(loc_ts))[0]
    await main.persist_update(main.store.apply_burst(mac, cleaned, station_id=station))
async def pos_scenario():
    await hear_pos("master", "2000")
    await hear_pos("node-1", "2000")          # same broadcast, second station
asyncio.run(pos_scenario())
pos_flight = main.store.tracks["SER-POS"].flight_id
first = asyncio.run(db.get_flight_points(pos_flight))[0]
check("a point stores where each station that heard it was",
      first["station_positions"] == {"master": [40.0, -100.1], "node-1": [40.1, -100.2]}, first.get("station_positions"))
main.station_cache = {"lat": 41.0, "lon": -101.0}          # the master is relocated afterwards
again = asyncio.run(db.get_flight_points(pos_flight))[0]
check("relocating a station doesn't change what an old point recorded",
      again["station_positions"]["master"] == [40.0, -100.1], again.get("station_positions"))
main.station_cache = {"lat": 40.0, "lon": -100.1}

print("\n[temperature]")
def fake_zones(zones):
    """A throwaway /sys/class/thermal with the given (type, millidegrees) zones."""
    root = tempfile.mkdtemp()
    for i, (kind, milli) in enumerate(zones):
        d = os.path.join(root, f"thermal_zone{i}")
        os.makedirs(d)
        open(os.path.join(d, "type"), "w").write(kind + "\n")
        open(os.path.join(d, "temp"), "w").write(f"{milli}\n")
    return root
NO_HWMON = os.path.join(tempfile.mkdtemp(), "no-hwmon")    # tests never read this machine's sensors
def fake_hwmon(devices):
    """A throwaway /sys/class/hwmon with the given (name, [millidegrees]) devices."""
    root = tempfile.mkdtemp()
    for i, (name, millis) in enumerate(devices):
        d = os.path.join(root, f"hwmon{i}")
        os.makedirs(d)
        open(os.path.join(d, "name"), "w").write(name + "\n")
        for j, milli in enumerate(millis, start=1):
            open(os.path.join(d, f"temp{j}_input"), "w").write(f"{milli}\n")
    return root
check("Intel: the CPU package zone wins over a hotter Wi-Fi zone",
      na.read_cpu_temp(fake_zones([("acpitz", 40000), ("iwlwifi_1", 70000), ("x86_pkg_temp", 55500)]), NO_HWMON) == 55.5)
check("Arm board: the SoC zone is used",
      na.read_cpu_temp(fake_zones([("gpu-thermal", 50000), ("soc-thermal", 61200)]), NO_HWMON) == 61.2)
check("unknown zone names: the hottest is used",
      na.read_cpu_temp(fake_zones([("board", 45000), ("other", 52000)]), NO_HWMON) == 52.0)
check("no thermal zones: None", na.read_cpu_temp(os.path.join(tempfile.mkdtemp(), "missing"), NO_HWMON) is None)
check("nonsense readings are ignored", na.read_cpu_temp(fake_zones([("x86_pkg_temp", 999000)]), NO_HWMON) is None)
check("your master: coretemp's cores are used, not the cool ACPI zone",
      na.read_cpu_temp(fake_zones([("acpitz", 26800), ("soc_dts0", 45000), ("soc_dts1", 47000)]),
                       fake_hwmon([("ath10k_hwmon", [42000]), ("coretemp", [49000, 47000, 49000, 49000])])) == 49.0)
check("Intel Atom without coretemp: the SoC zones are used, not ACPI",
      na.read_cpu_temp(fake_zones([("acpitz", 26800), ("soc_dts0", 45000), ("soc_dts1", 47000)]), NO_HWMON) == 47.0)
check("only non-CPU hwmon devices: falls back to the thermal zones",
      na.read_cpu_temp(fake_zones([("x86_pkg_temp", 55000)]), fake_hwmon([("nvme", [38000]), ("ath10k_hwmon", [60000])])) == 55.0)
check("AMD: k10temp is used",
      na.read_cpu_temp(fake_zones([("acpitz", 30000)]), fake_hwmon([("k10temp", [63500])])) == 63.5)
check("the report carries the temperature",
      nodes.parse_status(na.build_status(fix=None, now=1.0, temp_c=61.5))["temp_c"] == 61.5)
check("an older agent's report has no temperature",
      nodes.parse_status(na.build_status(fix=None, now=1.0))["temp_c"] is None)
treg = nodes.NodeRegistry(nodes.parse_nodes({"stations": [{"id": 2, "ip": "100.100.10.91"}]}, MASTER_ADDR))
tnow = time.time()
treg.note_status("node-2", nodes.parse_status(na.build_status(fix=None, now=tnow, temp_c=72.0)), tnow)
check("a fresh report's temperature is kept", treg.temperature("node-2", tnow + 1) == 72.0)
check("a silent agent's temperature is unknown", treg.temperature("node-2", tnow + nodes.STATUS_STALE_S + 1) is None)

def temp_run(readings, until):
    """readings: [(time, celsius)] for node-1; steps every 5 s like the monitor."""
    tr = main.TempTracker(90, 5, 60)
    out, tick, current = [], 0, None
    while tick <= until:
        for at, c in readings:
            if at == tick:
                current = c
        out += [(tick, m) for m in tr.step({"node-1": current}, {"node-1": "Node 1"}, tick)]
        tick += 5
    return out
check("a brief spike to 90 C sends nothing", temp_run([(0, 70), (10, 91), (30, 75)], 150) == [])
msgs = temp_run([(0, 70), (10, 92), (200, 80)], 240)
check("sustained heat sends one alert, then the all-clear",
      [m for _, m in msgs] == ["## \U0001F321\ufe0f Node 1 High Temperature\n```92\u00b0 C - limit 90\u00b0 C```",
                              "## \u2705 Node 1 Temperature Normal\n```80\u00b0 C```"], msgs)
check("the alert waits a minute", msgs[0][0] == 70, msgs)
msgs = temp_run([(0, 70), (10, 92), (100, 88), (120, 91), (140, 87), (300, 84)], 320)
check("hovering near the limit doesn't repeat; clears only 5 C below it",
      [t_ for t_, _ in msgs] == [70, 300], msgs)

print("\n[front LEDs]")
def fake_leds(names):
    """A throwaway /sys/class/leds with the given LED directories."""
    root = tempfile.mkdtemp()
    for n in names:
        d = os.path.join(root, n)
        os.makedirs(d)
        for f, v in (("brightness", "0"), ("max_brightness", "1"), ("trigger", "none")):
            open(os.path.join(d, f), "w").write(v)
    return root
led_file = lambda root, n, f: open(os.path.join(root, n, f)).read()
check("mainline LED names are found",
      na.find_leds(fake_leds(["green:lan", "green:wan", "green:wlan", "red:power", "mmc0::"]))
      == {"lan": "green:lan", "wan": "green:wan", "wl": "green:wlan", "sys": "red:power"})
check("FriendlyElec LED names are found",
      na.find_leds(fake_leds(["sys_led", "wan_led", "lan_led", "wlan_led"]))
      == {"sys": "sys_led", "wan": "wan_led", "lan": "lan_led", "wl": "wlan_led"})
check("another board's LEDs are left alone", na.find_leds(fake_leds(["ACT", "PWR", "mmc0::"])) == {})
node2 = fake_leds(["ath9k-phy0", "lan1_led", "lan2_led", "mmc2::", "sys_led", "wan_led"])
check("numbered port LEDs count (lan1_led is LAN); Wi-Fi card and storage LEDs are left alone",
      na.find_leds(node2) == {"lan": "lan1_led", "sys": "sys_led", "wan": "wan_led"})
check("--led-map assigns an LED the names don't identify",
      na.find_leds(node2, {"wl": "lan2_led"}) == {"wl": "lan2_led", "lan": "lan1_led", "sys": "sys_led", "wan": "wan_led"})
check("a --led-map name that doesn't exist is skipped", "wl" not in na.find_leds(node2, {"wl": "lan3_led"}))
check("SYS solid with the decoder running, heartbeat without it",
      na.led_patterns("yellow", False, [])["sys"] == "on" and na.led_patterns("red", False, [])["sys"] == "heartbeat")
check("WAN follows the master", na.led_patterns("red", True, [])["wan"] == "on"
      and na.led_patterns("red", False, [])["wan"] == "off")
check("LAN: both sniffers solid, one blinks slowly, none off",
      [na.led_patterns("red", False, s)["lan"] for s in (["green", "yellow"], ["green", "red"], ["red", "red"])]
      == ["on", na.LAN_BLINK, "off"])
check("WL blinks only after a detection",
      na.led_patterns("green", False, [])["wl"] == na.WL_BLINK and na.led_patterns("yellow", False, [])["wl"] == "off")
lroot = fake_leds(["green:lan", "green:wan", "green:wlan", "red:power"])
panel = na.LedPanel(na.find_leds(lroot), lroot)
panel.set({"sys": "on", "wan": "off", "lan": na.LAN_BLINK, "wl": "heartbeat"})
lan = []
for i in range(40):
    panel.tick(i * 0.05)
    lan.append(led_file(lroot, "green:lan", "brightness"))
check("an LED shows its pattern", led_file(lroot, "red:power", "brightness") == "1"
      and led_file(lroot, "green:wan", "brightness") == "0" and led_file(lroot, "green:wlan", "trigger") == "heartbeat")
check("a slow blink is a second on, a second off", "".join(lan) == "1" * 20 + "0" * 20, "".join(lan))
panel.reset()
check("stopping puts SYS back on the kernel heartbeat and the rest off",
      led_file(lroot, "red:power", "trigger") == "heartbeat"
      and [led_file(lroot, n, "brightness") for n in ("green:lan", "green:wan", "green:wlan")] == ["0", "0", "0"])
troot = fake_leds(["green:lan"])
open(os.path.join(troot, "green:lan", "trigger"), "w").write("none heartbeat [netdev] timer")   # following the port
tpanel = na.LedPanel(na.find_leds(troot), troot)
check("the active trigger is read from its brackets", tpanel.active_trigger("lan") == "netdev")
tpanel.set({"lan": "off"}); tpanel.tick(0.0)
check("an LED following a network port is taken over", tpanel.active_trigger("lan") == "none")
open(os.path.join(troot, "green:lan", "trigger"), "w").write("none heartbeat [netdev] timer")   # set back by something else
open(os.path.join(troot, "green:lan", "brightness"), "w").write("1")
tpanel.tick(0.5)
held = tpanel.active_trigger("lan")
tpanel.tick(na.LED_RECHECK_S + 0.1)
check("an LED set back by something else is taken back at the next check",
      held == "netdev" and tpanel.active_trigger("lan") == "none"
      and led_file(troot, "green:lan", "brightness") == "0")
broot = fake_leds(["green:lan"])
os.remove(os.path.join(broot, "green:lan", "brightness"))
os.makedirs(os.path.join(broot, "green:lan", "brightness"))     # can't be written
bpanel = na.LedPanel(na.find_leds(broot), broot)
bpanel.set({"lan": "on"}); bpanel.tick(0.0); bpanel.tick(0.1)
check("an LED that can't be written turns LED control off instead of failing", bpanel.ok is False)

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
          "## \u2705 Node 1 Online\n```Decoder, agent and Wi-Fi sniffer online - Bluetooth sniffer offline```",
          "## \u2705 Node 1 Bluetooth Sniffer Online\n```All components reporting```"], msgs)
msgs = health_run([(10, "node-1", H_OFF), (200, "node-1", {"decoder": True}),
                   (300, "node-1", {"agent": True, "wifi": True, "bluetooth": False}),
                   (400, "node-1", {"bluetooth": True})], 440)
check("a recovery names what came back and what's still offline",
      [m for _, m in msgs][1:] == [
          "## \u2705 Node 1 Online\n```Decoder online - Agent offline```",
          "## \u2705 Node 1 Agent Online\n```Agent and Wi-Fi sniffer online - Bluetooth sniffer offline```",
          "## \u2705 Node 1 Bluetooth Sniffer Online\n```All components reporting```"], msgs)
msgs = health_run([(10, "node-1", {"agent": False, "wifi": None, "bluetooth": None}), (150, "node-1", H_UP)], 190)
check("an agent outage names only the agent",
      msgs[0][1] == "## \u26a0\ufe0f Node 1 Agent Down\n```Agent not reporting - Decoder still up```", msgs)
msgs = health_run([(10, "node-1", {"agent": False, "wifi": None, "bluetooth": None}), (25, "node-1", H_UP),
                   (100, "node-1", {"wifi": False}), (220, "node-1", {"wifi": True})], 260)
check("a short agent blip doesn't leak into a later message",
      [m for _, m in msgs] == [
          "## \u26a0\ufe0f Node 1 Wi-Fi Sniffer Down\n```Wi-Fi sniffer not reporting - Decoder, agent and Bluetooth sniffer still up```",
          "## \u2705 Node 1 Wi-Fi Sniffer Online\n```All components reporting```"], msgs)
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

print("\n[drone models]")
import faa_models, urllib.error
FAKE_LIST = [
    {"trackingNumber": "RID000000001", "docType": "rid", "status": "accepted", "updatedAt": "a", "revNumber": 1},
    {"trackingNumber": "RID000000002", "docType": "rid", "status": "accepted", "updatedAt": "a", "revNumber": 1},
    {"trackingNumber": "RID000000003", "docType": "rid", "status": "accepted", "updatedAt": "a", "revNumber": 1},
    {"trackingNumber": "OOP000000004", "docType": "oop", "status": "accepted", "updatedAt": "a", "revNumber": 1},
    {"trackingNumber": "RID000000005", "docType": "rid", "status": "rescinded", "updatedAt": "a", "revNumber": 1},
]
FAKE_DOCS = {
    "RID000000001": ({"docRevId": 11, "makeName": "DJI", "modelName": "Mini 4 Pro", "certificationType": {"name": "Unmanned Aircraft"}},
                     "1581F6Z9000000000000-1581F6Z9ZZZZZZZZZZZZ\n"),
    "RID000000002": ({"docRevId": 12, "makeName": "Wingtra", "modelName": "WingtraOne Gen II", "certificationType": {"name": "Unmanned Aircraft"}},
                     "serialNumber\n17445AB123\n17445AB124\n"),
    "RID000000003": ({"docRevId": 13, "makeName": "Dronetag", "modelName": "Dronetag DRI", "certificationType": {"name": "Broadcast Module"}},
                     "1596A0000000000-1596AZZZZZZZZZZ\n"),
}
class FakeFaa:
    def __init__(self):
        self.calls, self.down, self.items = 0, False, [dict(i) for i in FAKE_LIST]
    def __call__(self, url, accept):
        self.calls += 1
        if self.down:
            raise urllib.error.URLError("unreachable")
        path = url.split("/api/v1", 1)[1]
        if path.startswith("/publicDOCRev?"):
            return json.dumps({"data": {"items": self.items, "totalItems": len(self.items)}})
        if path.startswith("/publicDOCRev/"):
            return json.dumps({"data": {"items": [{"publicRevision": FAKE_DOCS[path.rsplit("/", 1)[1]][0]}]}})
        rev = int(path.split("docRevIds%5B%5D=")[1].split("&")[0])
        return next(c for d, c in FAKE_DOCS.values() if d["docRevId"] == rev)

mdir = tempfile.mkdtemp()
overrides = os.path.join(mdir, "model_overrides.csv")
with open(overrides, "w") as f:
    f.write("prefix,model\n# my own entries\n1581FB34,DJI Lito X1\n")
faa = FakeFaa()
cat = faa_models.ModelCatalog(os.path.join(mdir, "m.db"), overrides, fetch=faa, pause_s=0)
M = lambda s: tuple((cat.lookup(s) or {}).get(k) for k in ("make", "model"))
check("an empty table needs a refresh", cat.needs_refresh())
check("the label puts make and model together without doubling",
      faa_models.label({"make": "DJI", "model": "Mini 4 Pro"}) == "DJI Mini 4 Pro"
      and faa_models.label({"make": "Percepto AirMax", "model": "Percepto AirMax OGI"}) == "Percepto AirMax OGI"
      and faa_models.label({"make": "DJI", "model": None}) == "DJI" and faa_models.label(None) is None)
check("a model filed with its make in front loses it",
      faa_models.make_and_model("DJI", "DJI Avata 360", "Unmanned Aircraft") == {"make": "DJI", "model": "Avata 360"})
summary = cat.refresh()
check("a refresh keeps only accepted Remote ID declarations", summary["fetched"] == 3 and faa.calls == 7, (summary, faa.calls))
check("a serial in a declared range gives make and model", M("1581F6Z9ABCDEF123456") == ("DJI", "Mini 4 Pro"))
check("a listed serial matches, and a model only loses the make as a whole word",
      M("17445AB123") == ("Wingtra", "WingtraOne Gen II"), M("17445AB123"))
check("a broadcast module says so", M("1596AABCDEFGHIJ") == ("Dronetag", "DRI (broadcast module)"), M("1596AABCDEFGHIJ"))
check("an override wins (a two-column line takes its first word as the make)",
      M("1581FB34C26380039ST4") == ("DJI", "Lito X1"))
check("an unknown serial from a known maker gives the make only", M("15815ABCDE") == ("DJI", None), M("15815ABCDE"))
check("an unknown maker gives nothing", cat.lookup("99995ABCDE") is None and cat.lookup("") is None)
check("lookups are case-insensitive", M("17445ab124") == ("Wingtra", "WingtraOne Gen II"))
check("a fresh table doesn't need a refresh", not cat.needs_refresh())
faa.calls = 0
check("an unchanged list re-fetches nothing", cat.refresh()["unchanged"] == 3 and faa.calls == 1)
faa.items[0]["updatedAt"] = "b"
faa.items.pop(1)
faa.calls = 0
summary = cat.refresh()
check("only a changed declaration is re-fetched, and a withdrawn one is dropped",
      summary["fetched"] == 1 and summary["removed"] == 1 and faa.calls == 3 and cat.lookup("17445AB123") is None, summary)
faa.down = True
summary = cat.refresh()
check("an unreachable FAA keeps the last copy and records the error",
      "error" in summary and M("1581F6Z9ABCDEF123456") == ("DJI", "Mini 4 Pro") and cat.status()["last_error"], summary)
with open(overrides, "a") as f:
    f.write("1581F6Z9ABC,DJI,Mini 4 Pro (mine)\n")
os.utime(overrides, (time.time() + 10, time.time() + 10))
cat._overrides_checked_at = float("-inf")
check("an edited overrides file is picked up, longest prefix first",
      M("1581F6Z9ABCDEF123456") == ("DJI", "Mini 4 Pro (mine)") and cat.status()["overrides"] == 2)

main.model_catalog = cat
t = main.DroneTrack(key="1581F6Z9000000000001", serial="1581F6Z9000000000001")
check("a live track carries its make and model",
      (t.to_dict()["make"], t.to_dict()["model"]) == ("DJI", "Mini 4 Pro"))
check("the alert heading uses the model when there's no nickname",
      main.build_new_drone_message(t).startswith("## DJI Mini 4 Pro Detected"), main.build_new_drone_message(t)[:60])
t.nickname = "Bob"
check("a nickname still wins in the heading", main.build_new_drone_message(t).startswith("## Bob Detected"))
t2 = main.DroneTrack(key="15815ABCDE", serial="15815ABCDE")
check("a maker-only match reads as '<maker> Drone' in the heading",
      main.build_new_drone_message(t2).startswith("## DJI Drone Detected"), main.build_new_drone_message(t2)[:60])
t3 = main.DroneTrack(key="aa:bb", mac="aa:bb")
check("no serial -> no model, and the heading stays New Drone",
      t3.to_dict()["make"] is None and t3.to_dict()["model"] is None and main.build_new_drone_message(t3).startswith("## New Drone Detected"))
check("history flights carry their model", main._with_model({"serial": "17445AB124"}) == {"serial": "17445AB124", "make": None, "model": None}
      and main._with_model({"serial": "1581F6Z9000000000001"})["model"] == "Mini 4 Pro")
check("model refresh is admin-only", main.route_needs_admin("POST", "/api/models/refresh")
      and not main.route_needs_admin("GET", "/api/models/status"))
print("\n[history search and filters]")
db.init(os.path.join(mdir, "history.db"))
T0 = 1_790_000_000.0
async def hist_setup():
    async def flight(serial, mac, start, n, nickname=None, station="master", transport="wifi", op=False,
                     location=None, ended=True, stations=None):
        key = serial or mac
        await db.upsert_catalog(key, mac, serial, None, None, None, start)
        if nickname:
            await db.rename_drone(key, nickname)
        fid = await db.start_flight(key, mac, serial, start)
        for i in range(n):
            await db.add_point(fid, start + i * 10, {"lat": 26.0, "lon": -97.2, "station_id": station,
                "transport": transport, "op_lat": 26.01 if op else None, "op_lon": -97.21 if op else None,
                "stations": json.dumps(stations) if stations else None})
        if location:
            await db.set_flight_location(fid, location)
        if ended:
            await db.end_flight(fid, start + n * 10)
        return fid
    return [
        await flight("1581F6Z9000000000001", "aa:01", T0, 30, nickname="Mini One", op=True, location="Port Isabel"),
        await flight("1581F6Z9000000000002", "aa:02", T0 + 86400, 3, location="South Padre Island"),
        await flight("1596AABCDEFGHIJ", "bb:01", T0 + 2 * 86400, 12, transport=None, station="node-2"),
        await flight(None, "cc:dd:ee:ff:00:11", T0 + 3 * 86400, 5, transport="bluetooth", ended=False,
                     stations=["master", "node-2"]),
    ]
hids = asyncio.run(hist_setup())
async def hist(**kw):
    args = dict(limit=50, offset=0, q=None, make=None, model=None, since=None, until=None, station=None,
                min_points=None, name=None, sort=None)
    args.update(kw)
    r = await main.api_list_flights(**args)
    return [f["id"] for f in r["flights"]], r["total"], r["all_total"]
H = lambda **kw: asyncio.run(hist(**kw))
check("no filters: every flight, newest first, with totals", H() == (hids[::-1], 4, 4), H())
check("search matches a nickname", H(q="mini one")[0] == [hids[0]])
check("search matches part of a serial, case-insensitive", H(q="1581f6z9")[0] == [hids[1], hids[0]])
check("search matches a MAC and a place name", H(q="cc:dd")[0] == [hids[3]] and H(q="padre")[0] == [hids[1]])
check("search matches make and model, every word must match",
      H(q="dji mini")[0] == [hids[1], hids[0]] and H(q="dronetag")[0] == [hids[2]] and H(q="dji dronetag")[0] == [])
check("search treats % and _ literally", H(q="%")[0] == [] and H(q="_")[0] == [])
check("make filter", H(make="DJI")[0] == [hids[1], hids[0]] and H(make="dronetag")[0] == [hids[2]])
check("make and model filter", H(make="Dronetag", model="DRI (broadcast module)")[0] == [hids[2]]
      and H(make="DJI", model="Air 3")[0] == [])
check("unknown make includes flights with no serial", H(make=main.UNKNOWN_FILTER)[0] == [hids[3]])
check("a make with no flights matches nothing", H(make="Parrot") == ([], 0, 4))
check("date range", H(since=T0 + 3600, until=T0 + 2 * 86400 + 1)[0] == [hids[2], hids[1]])
check("station filter, including a point another station also heard",
      H(station="node-2")[0] == [hids[3], hids[2]] and H(station="master")[0] == [hids[3], hids[1], hids[0]])
check("minimum points", H(min_points=10)[0] == [hids[2], hids[0]])
check("named and unnamed", H(name="named")[0] == [hids[0]] and hids[0] not in H(name="unnamed")[0]
      and len(H(name="unnamed")[0]) == 3 and len(H(name="anything")[0]) == 4)
check("sorts", H(sort="oldest")[0] == hids and H(sort="points")[0][0] == hids[0])
check("filters combine, and paging keeps the filtered total",
      H(make="DJI", min_points=1, q="padre") == ([hids[1]], 1, 4) and H(q="1581", limit=1, offset=1) == ([hids[0]], 2, 4))
async def more_flights():
    async def fl(serial, nickname, start):
        await db.upsert_catalog(serial, None, serial, None, None, None, start)
        await db.rename_drone(serial, nickname)
        fid = await db.start_flight(serial, None, serial, start)
        await db.add_point(fid, start, {"lat": 26.0, "lon": -97.2, "station_id": "master"})
        await db.end_flight(fid, start + 10)
        return fid
    return [await fl("1581F6Z90000000000A5", "SpaceX Mavic 4 Pro 5", T0 + 10 * 86400),
            await fl("1581F6Z90000000000A6", "SpaceX Mavic 4 Pro 50", T0 + 11 * 86400),
            await fl("1581F6Z90000000000A7", "SpaceX Mavic 4 Pro 6", T0 + 12 * 86400),
            await fl("1581F6Z90000000000A7", "SpaceX Mavic 4 Pro 6", T0 + 13 * 86400)]
more = asyncio.run(more_flights())
check("a search naming one drone exactly finds only that drone", H(q="SpaceX Mavic 4 Pro 5")[0] == [more[0]], H(q="SpaceX Mavic 4 Pro 5"))
check("a shared phrase finds every drone it names", set(H(q="spacex mavic 4 pro")[0]) == set(more))
check("a short number is a whole word: '5' doesn't match '50' or '512'",
      H(q="spacex 5")[0] == [more[0]] and H(q="mavic 50")[0] == [more[1]])
check("words can still come from different fields", H(q="mini padre")[0] == [hids[1]])
check("part of a serial from its middle", H(q="0000000a7")[0] == [more[3], more[2]])
def DR(**kw):
    args = dict(limit=50, offset=0, q=None, make=None, model=None, since=None, until=None, station=None,
                min_points=None, name=None, sort=None, group="drone")
    args.update(kw)
    return asyncio.run(main.api_list_flights(**args))
r = DR(q="spacex")
check("grouped by drone: each drone's flights together, newest first, with the group size",
      [f["id"] for f in r["flights"]] == [more[3], more[2], more[1], more[0]]
      and [f["drone_flights"] for f in r["flights"]] == [2, 2, 1, 1], r["flights"])
order = lambda r: list(dict.fromkeys(f["catalog_key"] for f in r["flights"]))
A5, A6, A7 = "1581F6Z90000000000A5", "1581F6Z90000000000A6", "1581F6Z90000000000A7"
check("drones by last seen, newest or oldest first",
      order(DR(q="spacex", sort="newest")) == [A7, A6, A5] and order(DR(q="spacex", sort="oldest")) == [A5, A6, A7])
check("drones by most or fewest flights", order(DR(q="spacex", sort="most")) == [A7, A6, A5]
      and order(DR(q="spacex", sort="fewest")) == [A6, A5, A7])
check("an unknown drone sort falls back to newest", order(DR(q="spacex", sort="longest")) == [A7, A6, A5])
r1, r2 = DR(limit=3, offset=0), DR(limit=3, offset=3)
keys1 = [f["catalog_key"] for f in r1["flights"]]
check("grouped by drone pages by drone, with every flight of each drone on its page",
      len(set(keys1)) == 3 and len(r1["flights"]) == 4 and r1["drones_total"] == 7
      and not set(keys1) & {f["catalog_key"] for f in r2["flights"]} and len(r2["flights"]) == 3, (r1, r2))
check("without grouping, flights don't carry a group size", all("drone_flights" not in f for f in DR(group=None)["flights"]))
check("longest, shortest and fewest points",
      H(sort="longest")[0][0] == hids[0] and H(sort="shortest")[0][-1] == hids[0]
      and H(sort="fewest")[0][-1] == hids[0] and H(sort="points")[0][0] == hids[0])

facets = asyncio.run(main.api_flight_facets())
fm_ = {m["make"]: m for m in facets["makes"]}
check("facets list makes and models with flight counts, unknown last",
      fm_["DJI"]["count"] == 6 and fm_["DJI"]["models"] == [{"model": "Mini 4 Pro", "count": 6}]
      and facets["makes"][-1]["make"] == main.UNKNOWN_FILTER and fm_[main.UNKNOWN_FILTER]["count"] == 1, facets)

main.model_catalog = None
check("no catalog -> lookups quietly return None", main.drone_model("1581F6Z9000000000001") == {"make": None, "model": None})

print()
if fails:
    print(f"{len(fails)} FAILED: {fails}")
    sys.exit(1)
print("ALL PASS")