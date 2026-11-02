#!/usr/bin/env python3
"""Position beacon for a remote DroneID node.

Runs on a remote node, alongside its decoder. Every few seconds it publishes a
small JSON report on a ZMQ PUB socket saying where the node is — from a GPS
receiver through gpsd, or a fixed position — and whether it has a fix. The
master server subscribes to that and moves the node's marker on the map.

It sends no detections. The node's decoder sends those on its own port.

    # roaming node with a GPS receiver (gpsd must be running)
    python3 node_agent.py --bind 100.100.10.91:4230 --gpsd 127.0.0.1:2947

    # fixed node: report a set position, no GPS needed
    python3 node_agent.py --bind 100.100.10.90:4230 --lat <latitude> --lon <longitude>

Bind to the node's Tailscale address, not 0.0.0.0, so only your tailnet can
reach it. Requires pyzmq.

It also watches the node's own Bluetooth and Wi-Fi sniffers over their ZMQ
ports and reports each one as green (sending), yellow (connected but quiet) or
red (not connected). That is the same check the master makes for its own
sniffers, so the map shows the same states for every station.

With --leds it also drives the board's front LEDs (a NanoPi R5C's SYS, WAN,
LAN and WL), so the node's state can be read at a glance:

    SYS  solid while this agent and the node's decoder both run; otherwise
         the kernel's normal heartbeat blink
    WAN  on while the master is connected and listening
    LAN  on with both sniffers running, slow blink with one, off with none
    WL   fast blink for 10 seconds after each drone detection

    python3 node_agent.py --bind 100.100.10.91:4230 --gpsd 127.0.0.1:2947 --leds
    python3 node_agent.py --reset-leds      # put the LEDs back (service stop hook)
"""
import argparse
import json
import logging
import math
import os
import re
import signal
import socket
import sys
import threading
import time
from typing import Optional

log = logging.getLogger("node-agent")


def parse_gpsd_line(line: str) -> Optional[dict]:
    """One line of gpsd's JSON stream -> a TPV (position) or SKY (satellites)
    record in a simple shape, or None for anything else."""
    try:
        msg = json.loads(line)
    except (ValueError, TypeError):
        return None
    if not isinstance(msg, dict):
        return None
    cls = msg.get("class")
    if cls == "TPV":
        mode = msg.get("mode", 0)
        lat, lon = msg.get("lat"), msg.get("lon")
        fix = isinstance(mode, int) and mode >= 2 and _finite(lat) and _finite(lon)
        return {
            "kind": "TPV",
            "fix": bool(fix),
            "lat": float(lat) if fix else None,
            "lon": float(lon) if fix else None,
            "alt": _finite_or_none(msg.get("altMSL", msg.get("alt"))),
            "speed": _finite_or_none(msg.get("speed")),
            "track": _finite_or_none(msg.get("track")),
        }
    if cls == "SKY":
        used = msg.get("uSat")
        if not isinstance(used, int):
            sats = msg.get("satellites") or []
            used = sum(1 for s in sats if isinstance(s, dict) and s.get("used")) if isinstance(sats, list) else None
        return {"kind": "SKY", "sats": used if isinstance(used, int) else None}
    return None


def _finite(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def _finite_or_none(v) -> Optional[float]:
    return float(v) if _finite(v) else None


# A sniffer whose messages stop for this long is yellow (connected but idle).
HEALTH_TIMEOUT_S = 12.0

# Where each sniffer publishes on this node. These match the defaults the
# decoder subscribes to; override with --bluetooth-zmq / --wifi-zmq if a sniffer
# was started with a different --zmqsetting.
SNIFFER_ZMQ = {"bluetooth": "127.0.0.1:4222", "wifi": "127.0.0.1:4223"}


class SnifferWatch:
    """Watches one sniffer's ZMQ output. Connected means the socket is up to the
    sniffer; green means its messages are arriving; yellow means it is connected
    but has been quiet for HEALTH_TIMEOUT_S; red means it isn't connected.

    The socket runs on a background thread and reconnects on its own, so a
    sniffer restart shows as red and then green again."""

    def __init__(self, name: str, addr: str, timeout: float = HEALTH_TIMEOUT_S):
        self.name, self.addr, self.timeout = name, addr, timeout
        self._lock = threading.Lock()
        self._connected = False
        self._last_message_at: Optional[float] = None

    # State changes, called from the reader thread (and directly by tests).
    def note_connected(self, connected: bool):
        with self._lock:
            self._connected = connected

    def note_message(self, now: float):
        with self._lock:
            self._last_message_at = now

    def status(self, now: float) -> str:
        with self._lock:
            connected, last = self._connected, self._last_message_at
        if not connected:
            return "red"
        if last is not None and now - last < self.timeout:
            return "green"
        return "yellow"

    def start(self):
        threading.Thread(target=self._run, name=f"sniffer-{self.name}", daemon=True).start()

    def _run(self):
        import zmq
        from zmq.utils.monitor import parse_monitor_message
        ctx = zmq.Context.instance()
        # Connected only once the sniffer completes the ZMQ handshake; a bare
        # TCP connect isn't proof it's running. Older pyzmq lacks the event.
        up_event = getattr(zmq, "EVENT_HANDSHAKE_SUCCEEDED", zmq.EVENT_CONNECTED)
        while True:
            sock = monitor = None
            try:
                sock = ctx.socket(zmq.SUB)
                sock.setsockopt(zmq.IPV6, 1)
                monitor = sock.get_monitor_socket()
                sock.connect(f"tcp://{self.addr}")
                sock.setsockopt(zmq.SUBSCRIBE, b"")
                poller = zmq.Poller()
                poller.register(sock, zmq.POLLIN)
                poller.register(monitor, zmq.POLLIN)
                while True:
                    ready = dict(poller.poll(500))
                    if monitor in ready:
                        ev = parse_monitor_message(monitor.recv_multipart())["event"]
                        if ev == up_event:
                            self.note_connected(True)
                        elif ev in (zmq.EVENT_DISCONNECTED, zmq.EVENT_CLOSED):
                            self.note_connected(False)
                    if sock in ready:
                        sock.recv()
                        self.note_message(time.time())
            except Exception:
                log.exception("%s sniffer watch failed at %s — retrying", self.name, self.addr)
                self.note_connected(False)
                time.sleep(1)
            finally:
                for h in (monitor, sock):
                    if h is not None:
                        h.close(linger=0)


def sniffer_health(watches: dict, now: Optional[float] = None) -> dict:
    """{"wifi": "green"|"yellow"|"red", "bluetooth": ...} from the watches."""
    now = now if now is not None else time.time()
    return {key: w.status(now) for key, w in watches.items()}


# Where the kernel exposes temperatures. Hardware-monitor devices (hwmon) carry
# the CPU's own sensors, such as Intel's coretemp; thermal zones are the
# fallback. Both report millidegrees.
HWMON_ROOT = "/sys/class/hwmon"
THERMAL_ROOT = "/sys/class/thermal"
# hwmon devices that are the CPU: Intel cores, AMD, and Arm SoCs.
CPU_HWMON_NAMES = ("coretemp", "k10temp", "zenpower",
                   "cpu_thermal", "cpu-thermal", "soc_thermal", "soc-thermal")
# Thermal zones that are the CPU: Intel package, Intel Atom SoC (soc_dts0,
# soc_dts1, ...), AMD, and the names Arm boards (such as the NanoPi R5C) use.
# acpitz isn't here on purpose: it's the board's ACPI zone, often an ambient
# reading far below the CPU's.
CPU_ZONE_TYPES = ("x86_pkg_temp", "soc_dts", "k10temp",
                  "cpu-thermal", "cpu_thermal", "soc-thermal", "soc_thermal", "cpu")


def _celsius(path: str) -> Optional[float]:
    """A millidegree reading as °C, or None if missing or outside -40 to 150."""
    try:
        with open(path) as f:
            celsius = int(f.read().strip()) / 1000.0
    except (OSError, ValueError):
        return None
    return celsius if -40.0 <= celsius <= 150.0 else None


def _read_text(path: str) -> Optional[str]:
    try:
        with open(path) as f:
            return f.read().strip()
    except OSError:
        return None


def _hwmon_cpu_temp(root: str) -> Optional[float]:
    """The hottest reading of a CPU hwmon device (every core, and the package
    when it's listed), or None if there's no such device."""
    try:
        devices = sorted(os.listdir(root))
    except OSError:
        return None
    for dev in devices:
        if _read_text(os.path.join(root, dev, "name")) not in CPU_HWMON_NAMES:
            continue
        try:
            files = os.listdir(os.path.join(root, dev))
        except OSError:
            continue
        readings = [c for f in files if f.startswith("temp") and f.endswith("_input")
                    for c in [_celsius(os.path.join(root, dev, f))] if c is not None]
        if readings:
            return round(max(readings), 1)
    return None


def _zone_cpu_temp(root: str) -> Optional[float]:
    """The CPU's thermal zone when one is recognised, otherwise the hottest
    zone, or None if there are no zones."""
    zones: dict = {}
    try:
        names = os.listdir(root)
    except OSError:
        return None
    for name in names:
        if not name.startswith("thermal_zone"):
            continue
        kind = _read_text(os.path.join(root, name, "type"))
        celsius = _celsius(os.path.join(root, name, "temp"))
        if kind is not None and celsius is not None:
            zones.setdefault(kind, []).append(celsius)
    for wanted in CPU_ZONE_TYPES:
        matches = [c for kind, cs in zones.items() if kind.startswith(wanted) for c in cs]
        if matches:
            return round(max(matches), 1)
    readings = [c for cs in zones.values() for c in cs]
    return round(max(readings), 1) if readings else None


def read_cpu_temp(root: str = THERMAL_ROOT, hwmon_root: str = HWMON_ROOT) -> Optional[float]:
    """The CPU temperature in °C, or None if this machine doesn't expose one.
    Uses the CPU's own sensor (coretemp and the like) first, then the CPU's
    thermal zone, then the hottest thermal zone."""
    temp = _hwmon_cpu_temp(hwmon_root)
    return temp if temp is not None else _zone_cpu_temp(root)


def build_status(*, fix: Optional[dict] = None,
                 static: Optional[tuple[float, float]] = None,
                 sats: Optional[int] = None, now: Optional[float] = None,
                 health: Optional[dict] = None, gps_feed: Optional[bool] = None,
                 temp_c: Optional[float] = None) -> dict:
    """The report the master expects. A fixed node always reports its set
    position; a roaming node reports its GPS fix, or no position at all when
    it has none (the master then keeps the last known one)."""
    base = {"type": "node_status", "health": health,
            "ts": now if now is not None else time.time(), "sats": sats}
    if gps_feed is not None:
        base["gps_feed"] = gps_feed   # whether gpsd answers: tells no feed apart from no fix
    if temp_c is not None:
        base["temp_c"] = temp_c       # CPU temperature, °C
    if static is not None:
        return base | {"fix": True, "lat": static[0], "lon": static[1],
                       "alt": None, "speed": None, "track": None, "source": "static"}
    if fix and fix.get("fix"):
        return base | {"fix": True, "lat": fix["lat"], "lon": fix["lon"], "alt": fix.get("alt"),
                       "speed": fix.get("speed"), "track": fix.get("track"), "source": "gps"}
    return base | {"fix": False, "lat": None, "lon": None, "alt": None,
                   "speed": None, "track": None, "source": "gps"}


# A fix older than this is reported as no fix. gpsd sends a position about once
# a second, so anything older means the receiver has stopped reporting.
FIX_MAX_AGE_S = 10.0


class GpsdClient:
    """Minimal client for gpsd's JSON stream. Reconnects on its own, so a
    gpsd restart or a brief USB dropout doesn't stop the beacon."""

    def __init__(self, host: str, port: int, timeout: float = 2.0):
        self.host, self.port, self.timeout = host, port, timeout
        self._sock = None
        self._buf = b""            # bytes read from gpsd but not yet split into lines
        self._sats: Optional[int] = None
        self._latest: Optional[dict] = None
        self._latest_at: float = 0.0
        self._warned_down = False  # one warning per outage, not one per poll
        self.feed_up = False       # True while the receiver is delivering; reported in each status
        self._tpv_at = float("-inf")   # when gpsd last sent a position record of any mode

    def _connect(self):
        self.close()
        sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        sock.sendall(b'?WATCH={"enable":true,"json":true}\n')
        self._sock = sock
        self._buf = b""
        self._warned_down = False

    def close(self):
        try:
            if self._sock is not None:
                self._sock.close()
        except OSError:
            pass
        self._sock = None
        self._buf = b""

    def _readline(self) -> str:
        """One line from gpsd. Reads straight from the socket and keeps its own
        buffer: a timeout here just raises socket.timeout and leaves the buffer
        intact, so the next read carries on. (A socket.makefile() reader can't
        be used after a timeout; it fails for good with "cannot read from timed
        out object".)"""
        while b"\n" not in self._buf:
            chunk = self._sock.recv(4096)
            if not chunk:
                raise OSError("gpsd closed the connection")
            self._buf += chunk
        line, _, self._buf = self._buf.partition(b"\n")
        return line.decode("utf-8", "replace")

    def poll(self, budget_s: float = 1.5) -> tuple[Optional[dict], Optional[int]]:
        """Reads whatever gpsd has sent within budget_s and returns the newest
        (position, satellites). Position is None if gpsd is unreachable or
        has no fix — the caller reports that as "no fix"."""
        deadline = time.monotonic() + budget_s
        try:
            if self._sock is None:
                self._connect()
            # Short socket timeout so a quiet gpsd can't hold the loop past the
            # deadline. A timeout just means "nothing new this round".
            self._sock.settimeout(max(0.05, min(0.5, budget_s)))
            while time.monotonic() < deadline:
                try:
                    line = self._readline()
                except socket.timeout:
                    break
                rec = parse_gpsd_line(line)
                if rec is None:
                    continue
                if rec["kind"] == "TPV":
                    self._latest = rec
                    self._latest_at = time.monotonic()
                    self._tpv_at = self._latest_at
                elif rec["kind"] == "SKY" and rec["sats"] is not None:
                    self._sats = rec["sats"]
        except OSError as e:
            if not self._warned_down:
                log.warning("gpsd unavailable (%s) — reporting no fix until it returns", e)
                self._warned_down = True
            self.close()
            self._latest = None
        if self._latest is not None and time.monotonic() - self._latest_at > FIX_MAX_AGE_S:
            self._latest = None   # the receiver has gone quiet; don't report where it was
        # The feed is up when the receiver is delivering position records, even
        # with no fix. gpsd answers on its socket even with no receiver behind
        # it, so a connection alone doesn't prove the feed.
        self.feed_up = (self._sock is not None
                        and time.monotonic() - self._tpv_at <= FIX_MAX_AGE_S)
        return self._latest, self._sats


# ---- front-panel LEDs ----
# Each LED is a directory under /sys/class/leds with a "trigger" file (what
# drives it: "none" for manual control, or a kernel pattern such as
# "heartbeat") and a "brightness" file. Blinking is done here, by switching
# brightness, rather than with the kernel's timer trigger, so only those two
# files need to be writable (see the README's udev rule).
LEDS_ROOT = "/sys/class/leds"
DETECTION_LED_S = 10.0          # WL blinks this long after a detection
LAN_BLINK = ("blink", 1.0, 1.0)  # one sniffer running
WL_BLINK = ("blink", 0.1, 0.1)   # a drone was just detected
LED_TICK_S = 0.05
# How often each LED's trigger is checked, in case something else on the board
# (a vendor script, or the LED following a network port) has set it back.
LED_RECHECK_S = 2.0


def find_leds(root: str = LEDS_ROOT) -> dict:
    """{"sys" | "wan" | "lan" | "wl": LED directory name} for the LEDs found.
    Matches both the mainline kernel's names (red:power, green:wan, green:lan,
    green:wlan) and FriendlyElec's (sys_led, wan_led, lan_led, wlan_led)."""
    found: dict = {}
    try:
        names = sorted(os.listdir(root))
    except OSError:
        return found
    for name in names:
        words = set(re.split(r"[:_\-.\s]+", name.lower()))
        if words & {"wlan", "wl", "wifi"}:
            role = "wl"
        elif "wan" in words:
            role = "wan"
        elif "lan" in words:
            role = "lan"
        elif words & {"sys", "power", "status"}:
            role = "sys"
        else:
            continue
        found.setdefault(role, name)
    return found


def led_patterns(decoder: str, master_listening: bool, sniffers: list) -> dict:
    """What each LED should show. decoder is the decoder watch's state: green
    after a detection within DETECTION_LED_S, yellow when running but quiet,
    red when not running. sniffers are the sniffer states the same way."""
    running = [s for s in sniffers if s in ("green", "yellow")]
    return {
        "sys": "on" if decoder in ("green", "yellow") else "heartbeat",
        "wan": "on" if master_listening else "off",
        "lan": "on" if len(running) >= 2 else (LAN_BLINK if running else "off"),
        "wl": WL_BLINK if decoder == "green" else "off",
    }


class LedPanel:
    """Shows a pattern on each LED: "on", "off", "heartbeat" (the kernel's
    own blink) or ("blink", on seconds, off seconds). The main loop sets the
    patterns; tick() applies them, from a thread while running. If an LED
    can't be written (no permission), LED control is turned off with one
    warning rather than failing every tick."""

    def __init__(self, leds: dict, root: str = LEDS_ROOT):
        self.root, self.leds = root, dict(leds)
        self.ok = bool(leds)
        self._want: dict = {}
        self._mode: dict = {}       # role -> "heartbeat" or "manual", as last written
        self._lit: dict = {}        # role -> last brightness written (True/False)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._max = {role: self._max_brightness(name) for role, name in leds.items()}
        self._checked_at = float("-inf")
        self._warned_taken: set = set()     # roles already reported as taken over

    def _max_brightness(self, name: str) -> int:
        try:
            with open(os.path.join(self.root, name, "max_brightness")) as f:
                return max(1, int(f.read().strip()))
        except (OSError, ValueError):
            return 1

    def _write(self, role: str, attr: str, value: str, quiet: bool = False) -> bool:
        path = os.path.join(self.root, self.leds[role], attr)
        try:
            with open(path, "w") as f:
                f.write(value)
            return True
        except OSError as e:
            if not quiet and self.ok:
                log.warning("LED control turned off: can't write %s (%s). "
                            "See the README for the udev rule that allows it.", path, e)
                self.ok = False
            return False

    def active_trigger(self, role: str) -> Optional[str]:
        """The trigger the kernel is using for an LED, shown in [brackets] in
        its trigger file, e.g. "none heartbeat [netdev] timer" -> "netdev"."""
        try:
            with open(os.path.join(self.root, self.leds[role], "trigger")) as f:
                text = f.read()
        except OSError:
            return None
        match = re.search(r"\[([^\]]+)\]", text)
        return match.group(1) if match else text.strip() or None

    def _recheck(self):
        """Takes back any LED whose trigger something else has changed."""
        for role, mode in list(self._mode.items()):
            expected = "heartbeat" if mode == "heartbeat" else "none"
            active = self.active_trigger(role)
            if active is not None and active != expected:
                if role not in self._warned_taken:
                    log.warning("The %s LED was set to trigger %r by something else on the "
                                "board; taking it back.", self.leds[role], active)
                    self._warned_taken.add(role)
                self._mode.pop(role, None)      # rewritten on this tick
                self._lit.pop(role, None)

    def set(self, patterns: dict):
        with self._lock:
            self._want.update({r: p for r, p in patterns.items() if r in self.leds})

    def tick(self, now: float):
        with self._lock:
            want = dict(self._want)
        if now - self._checked_at >= LED_RECHECK_S:
            self._checked_at = now
            self._recheck()
        for role, pattern in want.items():
            if not self.ok:
                return
            if pattern == "heartbeat":
                if self._mode.get(role) != "heartbeat" and self._write(role, "trigger", "heartbeat"):
                    self._mode[role] = "heartbeat"
                    self._lit.pop(role, None)
                continue
            if self._mode.get(role) != "manual":
                if not self._write(role, "trigger", "none"):
                    return
                self._mode[role] = "manual"
                self._lit.pop(role, None)
            if pattern == "on":
                lit = True
            elif pattern == "off":
                lit = False
            else:
                _, on_s, off_s = pattern
                lit = (now % (on_s + off_s)) < on_s
            if self._lit.get(role) != lit and self._write(role, "brightness",
                                                           str(self._max[role] if lit else 0)):
                self._lit[role] = lit

    def start(self):
        def run():
            while not self._stop.is_set() and self.ok:
                self.tick(time.monotonic())
                self._stop.wait(LED_TICK_S)
        threading.Thread(target=run, name="leds", daemon=True).start()

    def reset(self):
        """Back to the board's defaults: SYS on the kernel's heartbeat, the rest off."""
        self._stop.set()
        for role in self.leds:
            if role == "sys":
                self._write(role, "trigger", "heartbeat", quiet=True)
            else:
                self._write(role, "trigger", "none", quiet=True)
                self._write(role, "brightness", "0", quiet=True)


def drain_subscriptions(sock, listening: bool) -> bool:
    """Reads the subscribe / unsubscribe messages the master's connection
    sends to this agent's XPUB socket and returns whether the master is now
    listening. A subscribe arrives when it connects; an unsubscribe when it
    disconnects, including after a power cut, once heartbeats notice."""
    import zmq
    while True:
        try:
            msg = sock.recv(zmq.NOBLOCK)
        except zmq.Again:
            return listening
        if msg[:1] == b"\x01":
            listening = True
        elif msg[:1] == b"\x00":
            listening = False


def open_publisher(bind: str):
    import zmq  # imported here so the parsing above works without pyzmq
    if bind.startswith("0.0.0.0") or bind.startswith("*"):
        log.warning("Binding to %s exposes this beacon on every interface. "
                    "Bind to the node's Tailscale address instead.", bind)
    # XPUB rather than PUB: the same to the master, but it also passes on the
    # master's subscription, which is how the WAN LED knows the master is
    # listening. Heartbeats make a master that lost power drop off within
    # about 8 seconds instead of looking connected forever.
    sock = zmq.Context.instance().socket(zmq.XPUB)
    sock.setsockopt(zmq.IPV6, 1)  # the Tailscale address may be IPv6
    sock.setsockopt(zmq.HEARTBEAT_IVL, 2000)
    sock.setsockopt(zmq.HEARTBEAT_TIMEOUT, 6000)
    sock.bind(f"tcp://{bind}")
    return sock


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bind", help="host:port to publish on, e.g. 100.100.10.91:4230 (required)")
    ap.add_argument("--gpsd", help="host:port of gpsd, e.g. 127.0.0.1:2947 (roaming nodes)")
    ap.add_argument("--lat", type=float, help="fixed latitude (fixed nodes)")
    ap.add_argument("--lon", type=float, help="fixed longitude (fixed nodes)")
    ap.add_argument("--interval", type=float, default=2.0, help="seconds between reports [2]")
    ap.add_argument("--bluetooth-zmq", default=SNIFFER_ZMQ["bluetooth"],
                    help="host:port of the bluetooth sniffer's ZMQ output [127.0.0.1:4222]")
    ap.add_argument("--wifi-zmq", default=SNIFFER_ZMQ["wifi"],
                    help="host:port of the wifi sniffer's ZMQ output [127.0.0.1:4223]")
    ap.add_argument("--leds", action="store_true",
                    help="drive the board's front LEDs (SYS, WAN, LAN, WL) from the node's state")
    ap.add_argument("--decoder-zmq",
                    help="host:port of this node's decoder output, for the LEDs "
                         "[the --bind address, port 4224]")
    ap.add_argument("--reset-leds", action="store_true",
                    help="put the LEDs back to the board's defaults and exit")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.reset_leds:
        LedPanel(find_leds()).reset()
        return 0
    if not args.bind:
        ap.error("--bind is required")
    if (args.lat is None) != (args.lon is None):
        ap.error("give both --lat and --lon, or neither")
    # Position is optional: the master places a station from nodes.json or the
    # web UI, so a node may run with just --bind to report its health alone.
    static = (args.lat, args.lon) if args.lat is not None else None
    gps = None
    if args.gpsd:
        host, _, port = args.gpsd.rpartition(":")
        gps = GpsdClient(host or "127.0.0.1", int(port))

    watches = {"bluetooth": SnifferWatch("bluetooth", args.bluetooth_zmq),
               "wifi": SnifferWatch("wifi", args.wifi_zmq)}
    for w in watches.values():
        w.start()
        log.info("Watching %s sniffer at %s", w.name, w.addr)

    panel, decoder = None, None
    if args.leds:
        leds = find_leds()
        if leds:
            panel = LedPanel(leds)
            panel.start()
            decoder_addr = args.decoder_zmq or f"{args.bind.rpartition(':')[0]}:4224"
            decoder = SnifferWatch("decoder", decoder_addr, timeout=DETECTION_LED_S)
            decoder.start()
            log.info("Driving LEDs %s; watching the decoder at %s",
                     ", ".join(f"{r}={n}" for r, n in sorted(leds.items())), decoder_addr)
        else:
            log.warning("--leds given, but no SYS/WAN/LAN/WL LEDs found under %s", LEDS_ROOT)

    # systemd stops a service with SIGTERM; turn it into a normal exit so the
    # LEDs are put back below.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    pub = open_publisher(args.bind)
    log.info("Publishing position on %s every %.1fs", args.bind, args.interval)
    master_listening = False
    try:
        while True:
            fix, sats = (gps.poll(budget_s=max(0.2, args.interval - 0.5)) if gps else (None, None))
            if gps is None:
                time.sleep(args.interval)
            health = sniffer_health(watches)
            master_listening = drain_subscriptions(pub, master_listening)
            if panel is not None:
                panel.set(led_patterns(decoder.status(time.time()), master_listening,
                                       list(health.values())))
            pub.send_string(json.dumps(build_status(fix=fix, static=static, sats=sats,
                                                    health=health,
                                                    gps_feed=gps.feed_up if gps else False,
                                                    temp_c=read_cpu_temp())))
    except KeyboardInterrupt:
        return 0
    finally:
        if panel is not None:
            panel.reset()


if __name__ == "__main__":
    sys.exit(main())