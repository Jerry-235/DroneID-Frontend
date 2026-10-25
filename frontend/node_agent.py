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
"""
import argparse
import json
import logging
import math
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
                        if ev == zmq.EVENT_CONNECTED:
                            self.note_connected(True)
                        elif ev in (zmq.EVENT_DISCONNECTED, zmq.EVENT_CLOSED, zmq.EVENT_CONNECT_RETRIED):
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


def build_status(*, fix: Optional[dict] = None,
                 static: Optional[tuple[float, float]] = None,
                 sats: Optional[int] = None, now: Optional[float] = None,
                 health: Optional[dict] = None, gps_feed: Optional[bool] = None) -> dict:
    """The report the master expects. A fixed node always reports its set
    position; a roaming node reports its GPS fix, or no position at all when
    it has none (the master then keeps the last known one)."""
    base = {"type": "node_status", "health": health,
            "ts": now if now is not None else time.time(), "sats": sats}
    if gps_feed is not None:
        base["gps_feed"] = gps_feed   # whether gpsd answers: tells no feed apart from no fix
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
        self._file = None
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
        self._warned_down = False
        self._file = sock.makefile("r", encoding="utf-8", errors="replace")

    def close(self):
        for h in (self._file, self._sock):
            try:
                if h is not None:
                    h.close()
            except OSError:
                pass
        self._file = self._sock = None

    def poll(self, budget_s: float = 1.5) -> tuple[Optional[dict], Optional[int]]:
        """Reads whatever gpsd has sent within budget_s and returns the newest
        (position, satellites). Position is None if gpsd is unreachable or
        has no fix — the caller reports that as "no fix"."""
        deadline = time.monotonic() + budget_s
        try:
            if self._file is None:
                self._connect()
            # Short socket timeout so a quiet gpsd can't hold the loop past the
            # deadline. A timeout just means "nothing new this round".
            self._sock.settimeout(max(0.05, min(0.5, budget_s)))
            while time.monotonic() < deadline:
                try:
                    line = self._file.readline()
                except socket.timeout:
                    break
                if not line:
                    raise OSError("gpsd closed the connection")
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
        self.feed_up = (self._file is not None
                        and time.monotonic() - self._tpv_at <= FIX_MAX_AGE_S)
        return self._latest, self._sats


def open_publisher(bind: str):
    import zmq  # imported here so the parsing above works without pyzmq
    if bind.startswith("0.0.0.0") or bind.startswith("*"):
        log.warning("Binding to %s exposes this beacon on every interface. "
                    "Bind to the node's Tailscale address instead.", bind)
    sock = zmq.Context.instance().socket(zmq.PUB)
    sock.setsockopt(zmq.IPV6, 1)  # the Tailscale address may be IPv6
    sock.bind(f"tcp://{bind}")
    return sock


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bind", required=True, help="host:port to publish on, e.g. 100.100.10.91:4230")
    ap.add_argument("--gpsd", help="host:port of gpsd, e.g. 127.0.0.1:2947 (roaming nodes)")
    ap.add_argument("--lat", type=float, help="fixed latitude (fixed nodes)")
    ap.add_argument("--lon", type=float, help="fixed longitude (fixed nodes)")
    ap.add_argument("--interval", type=float, default=2.0, help="seconds between reports [2]")
    ap.add_argument("--bluetooth-zmq", default=SNIFFER_ZMQ["bluetooth"],
                    help="host:port of the bluetooth sniffer's ZMQ output [127.0.0.1:4222]")
    ap.add_argument("--wifi-zmq", default=SNIFFER_ZMQ["wifi"],
                    help="host:port of the wifi sniffer's ZMQ output [127.0.0.1:4223]")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
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

    pub = open_publisher(args.bind)
    log.info("Publishing position on %s every %.1fs", args.bind, args.interval)
    try:
        while True:
            fix, sats = (gps.poll(budget_s=max(0.2, args.interval - 0.5)) if gps else (None, None))
            if gps is None:
                time.sleep(args.interval)
            pub.send_string(json.dumps(build_status(fix=fix, static=static, sats=sats,
                                                    health=sniffer_health(watches),
                                                    gps_feed=gps.feed_up if gps else None)))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())