"""Drone make and model from a Remote ID serial number.

Remote ID broadcasts each aircraft's serial (ANSI/CTA-2063-A) in its Basic ID
message. Every model sold for US Remote ID has an FAA Declaration of Compliance
(DOC) that names its make and model and lists the serials it covers, as
individual serials or as ranges. This module keeps a local copy of the
accepted Remote ID declarations and answers lookups from it, so identifying a
drone needs no internet at detection time.

A lookup tries, in order:

    model_overrides.csv   your own entries: "prefix,make,model" lines, for
                          models with no FAA declaration (e.g.
                          1581FB34,DJI,Lito X1). The longest matching prefix
                          wins; a full serial works as a prefix too.
    faa_models.db         the local copy of the FAA's declarations. Generated
                          from the FAA's public API (uasdoc.faa.gov/api/v1);
                          not committed.
    manufacturer code     the serial's first four characters name its maker,
                          so an unmatched serial still gives its make (with no
                          model) when that code appears in the declarations.

The copy is refreshed about once a week (DRONEID_MODELS_REFRESH_DAYS; 0 means
only when asked). A refresh fetches the list of
declarations in one call, then re-downloads only the declarations whose
revision changed since the last refresh, pausing between requests. If the
FAA can't be reached, the last good copy keeps being used.

    python3 faa_models.py --refresh            refresh now
    python3 faa_models.py --lookup SERIAL ...   look serials up
    python3 faa_models.py --status             what the copy holds
"""
import argparse
import csv
import json
import logging
import os
import re
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from typing import Callable, Optional

log = logging.getLogger("faa_models")

HERE = os.path.dirname(os.path.abspath(__file__))
FAA_API = "https://uasdoc.faa.gov/api/v1"
MODELS_DB_PATH = os.environ.get("DRONEID_MODELS_DB", os.path.join(HERE, "faa_models.db"))
OVERRIDES_PATH = os.environ.get("DRONEID_MODEL_OVERRIDES", os.path.join(HERE, "model_overrides.csv"))
REFRESH_EVERY_S = float(os.environ.get("DRONEID_MODELS_REFRESH_DAYS", "7")) * 86400
REQUEST_PAUSE_S = 1.0          # between FAA requests, to be polite
REQUEST_TIMEOUT_S = 60.0       # a large serial list can take a while
USER_AGENT = "DroneID-Frontend (https://github.com/Jerry-235/DroneID-Frontend)"
OVERRIDES_RECHECK_S = 5.0
# A serial value is letters and digits; a range is two of them joined by "-".
_SERIAL_VALUE = re.compile(r"^[A-Z0-9]+(?:-[A-Z0-9]+)?$")

SCHEMA = """
CREATE TABLE IF NOT EXISTS docs (
    id         INTEGER PRIMARY KEY,
    tracking   TEXT UNIQUE NOT NULL,   -- e.g. RID000000713
    rev_id     INTEGER,                -- the public revision the serials came from
    make       TEXT,
    model      TEXT,
    series     TEXT,
    cert_type  TEXT,                   -- "Unmanned Aircraft", or a broadcast module
    change_key TEXT                    -- the listing's updatedAt + revNumber
);
CREATE TABLE IF NOT EXISTS ranges (
    doc   INTEGER NOT NULL,
    start TEXT NOT NULL,
    end   TEXT NOT NULL,
    len   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ranges ON ranges(len, start);
CREATE TABLE IF NOT EXISTS serials (
    serial TEXT NOT NULL,
    doc    INTEGER NOT NULL,
    PRIMARY KEY (serial, doc)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS makers (code TEXT PRIMARY KEY, make TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
"""


def make_and_model(make: Optional[str], model: Optional[str], cert_type: Optional[str] = None) -> dict:
    """{"make": "DJI", "model": "Mini 4 Pro"} from a declaration. A model filed
    with the make in front ("DJI Avata 360" by DJI) loses it, but only as a
    whole word ("WingtraOne" by Wingtra stays). An add-on broadcast module is
    marked as one in the model."""
    make, model = (make or "").strip(), (model or "").strip()
    if make and model.lower().startswith(make.lower() + " "):
        model = model[len(make) + 1:].strip()
    if cert_type and "module" in cert_type.lower():
        model = f"{model} (broadcast module)".strip()
    return {"make": make or None, "model": model or None}


def label(info: Optional[dict]) -> Optional[str]:
    """"DJI Mini 4 Pro" from a lookup result; just the make when the model isn't
    known; the model alone when it already names the make."""
    if not info:
        return None
    make, model = info.get("make") or "", info.get("model") or ""
    if make and model.lower().startswith(make.lower()):
        return model
    return f"{make} {model}".strip() or None


def manufacturer_code(serial: str) -> Optional[str]:
    """The four-character manufacturer code of an ANSI/CTA-2063-A serial, or
    None if the serial isn't in that format. The fifth character gives the
    length of the rest (1-9, A-F for 10-15), which is what tells a real serial
    from a manufacturer's own numbering."""
    s = serial.strip().upper()
    if len(s) < 6 or not s[:4].isalnum() or s[4] not in "123456789ABCDEF":
        return None
    return s[:4] if int(s[4], 16) == len(s) - 5 else None


def parse_serial_values(text: str) -> tuple[list, list]:
    """(ranges, singles) from the FAA's serial CSV: one value per line, either a
    serial or "start-end". Anything else (a header, blank lines) is skipped. A
    "range" whose ends differ in length can't be compared, so it's kept as two
    single serials."""
    ranges, singles = [], []
    for line in text.splitlines():
        value = line.split(",")[0].strip().strip('"').upper()
        if not value or not _SERIAL_VALUE.match(value):
            continue
        if "-" in value:
            start, end = value.split("-", 1)
            if len(start) == len(end):
                ranges.append((min(start, end), max(start, end)))
            else:
                singles += [start, end]
        else:
            singles.append(value)
    return ranges, singles


def _http_get(url: str, accept: str) -> str:
    req = urllib.request.Request(url, headers={"Accept": accept, "User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_S) as resp:
        return resp.read().decode("utf-8", errors="replace")


class ModelCatalog:
    """Lookups and refreshes for the local copy. Lookups are cached; a refresh
    runs in whatever thread calls it, on its own database connection, and
    clears the cache when it's done."""

    def __init__(self, db_path: str = MODELS_DB_PATH, overrides_path: str = OVERRIDES_PATH,
                 fetch: Callable[[str, str], str] = _http_get, pause_s: float = REQUEST_PAUSE_S):
        self.db_path, self.overrides_path = db_path, overrides_path
        self.fetch, self.pause_s = fetch, pause_s
        self._cache: dict = {}
        self._lock = threading.Lock()
        self._refreshing = threading.Lock()
        self._overrides: list = []
        self._overrides_mtime: Optional[float] = None
        self._overrides_checked_at = float("-inf")
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    # ---- overrides ----

    def _load_overrides(self) -> list:
        """[(prefix, make, model)], longest prefix first. A two-column line
        ("prefix,DJI Lito X1") takes its first word as the make. Re-read when the file
        changes, checked at most every OVERRIDES_RECHECK_S."""
        now = time.monotonic()
        if now - self._overrides_checked_at < OVERRIDES_RECHECK_S:
            return self._overrides
        self._overrides_checked_at = now
        try:
            mtime = os.path.getmtime(self.overrides_path)
        except OSError:
            if self._overrides:
                with self._lock:
                    self._cache.clear()
            self._overrides, self._overrides_mtime = [], None
            return self._overrides
        if mtime != self._overrides_mtime:
            entries = []
            try:
                with open(self.overrides_path, newline="", encoding="utf-8") as f:
                    for row in csv.reader(f):
                        if len(row) < 2 or not row[0].strip() or row[0].lstrip().startswith("#"):
                            continue
                        prefix = row[0].strip().upper()
                        if prefix == "PREFIX":
                            continue      # the header line
                        if len(row) >= 3:
                            make, model = row[1].strip(), row[2].strip()
                        else:
                            make, _, model = row[1].strip().partition(" ")
                        if make or model:
                            entries.append((prefix, make or None, model.strip() or None))
            except OSError as e:
                log.warning("Could not read %s: %s", self.overrides_path, e)
            entries.sort(key=lambda e: len(e[0]), reverse=True)
            self._overrides, self._overrides_mtime = entries, mtime
            with self._lock:
                self._cache.clear()
        return self._overrides

    # ---- lookups ----

    def lookup(self, serial: Optional[str]) -> Optional[dict]:
        """{"make": ..., "model": ...} for a serial, model None when only its
        manufacturer code is known, or None when nothing is. The returned dict
        is shared through the cache: don't modify it."""
        if not serial:
            return None
        s = serial.strip().upper()
        overrides = self._load_overrides()
        with self._lock:
            if s in self._cache:
                return self._cache[s]
        info = next(({"make": make, "model": model} for prefix, make, model in overrides
                     if s.startswith(prefix)), None)
        if info is None:
            info = self._lookup_faa(s)
        with self._lock:
            self._cache[s] = info
        return info

    def _lookup_faa(self, s: str) -> Optional[dict]:
        with self._lock:
            row = self._conn.execute(
                "SELECT d.make, d.model, d.cert_type FROM serials x JOIN docs d ON d.id = x.doc "
                "WHERE x.serial = ? ORDER BY d.tracking LIMIT 1", (s,)).fetchone()
            if row is None:
                row = self._conn.execute(
                    "SELECT d.make, d.model, d.cert_type FROM ranges r JOIN docs d ON d.id = r.doc "
                    "WHERE r.len = ? AND r.start <= ? AND r.end >= ? ORDER BY d.tracking LIMIT 1",
                    (len(s), s, s)).fetchone()
            if row is not None:
                return make_and_model(*row)
            code = manufacturer_code(s)
            maker = code and self._conn.execute("SELECT make FROM makers WHERE code = ?", (code,)).fetchone()
        return {"make": maker[0], "model": None} if maker else None

    def status(self) -> dict:
        with self._lock:
            meta = dict(self._conn.execute("SELECT key, value FROM meta").fetchall())
            docs = self._conn.execute("SELECT COUNT(*) FROM docs").fetchone()[0]
        return {
            "declarations": docs,
            "last_refresh": float(meta["last_refresh"]) if meta.get("last_refresh") else None,
            "last_error": meta.get("last_error") or None,
            "refreshing": self._refreshing.locked(),
            "overrides": len(self._load_overrides()),
        }

    def needs_refresh(self, now: Optional[float] = None) -> bool:
        """True when the copy is empty or older than REFRESH_EVERY_S. Always
        False when DRONEID_MODELS_REFRESH_DAYS is 0 (refresh by hand only)."""
        if REFRESH_EVERY_S <= 0:
            return False
        last = self.status()["last_refresh"]
        now = now if now is not None else time.time()
        return last is None or now - last >= REFRESH_EVERY_S

    # ---- refresh ----

    def _get(self, path: str, accept: str = "application/json") -> str:
        text = self.fetch(f"{FAA_API}{path}", accept)
        if self.pause_s:
            time.sleep(self.pause_s)
        return text

    def _listing(self) -> list:
        """Every declaration in the FAA's list, paging if it ever needs to."""
        items, page = [], 0
        while True:
            data = json.loads(self._get(f"/publicDOCRev?itemsPerPage=1000&pageIndex={page}"))["data"]
            batch = data.get("items") or []
            items += batch
            total = int(data.get("totalItems") or 0)
            if not batch or len(items) >= total:
                return items
            page += 1

    def refresh(self) -> dict:
        """Brings the local copy up to date. Returns a short summary. Never
        raises for network trouble: the last good copy stays in place and the
        error is recorded for the status display."""
        if not self._refreshing.acquire(blocking=False):
            return {"skipped": "a refresh is already running"}
        conn = sqlite3.connect(self.db_path)
        summary = {"fetched": 0, "unchanged": 0, "removed": 0, "failed": 0}
        try:
            items = self._listing()
            wanted = {i["trackingNumber"]: i for i in items
                      if i.get("docType") == "rid" and i.get("status") == "accepted"}
            known = {t: (doc_id, key) for doc_id, t, key in
                     conn.execute("SELECT id, tracking, change_key FROM docs").fetchall()}
            for tracking, item in sorted(wanted.items()):
                change_key = f"{item.get('updatedAt')}|{item.get('revNumber')}"
                if tracking in known and known[tracking][1] == change_key:
                    summary["unchanged"] += 1
                    continue
                try:
                    self._refresh_doc(conn, tracking, known.get(tracking, (None,))[0], change_key)
                    summary["fetched"] += 1
                except (urllib.error.URLError, OSError, ValueError, KeyError, IndexError, TypeError) as e:
                    summary["failed"] += 1
                    log.warning("FAA declaration %s not refreshed: %s", tracking, e)
            for tracking in set(known) - set(wanted):     # rescinded or withdrawn
                doc_id = known[tracking][0]
                with conn:
                    conn.execute("DELETE FROM ranges WHERE doc = ?", (doc_id,))
                    conn.execute("DELETE FROM serials WHERE doc = ?", (doc_id,))
                    conn.execute("DELETE FROM docs WHERE id = ?", (doc_id,))
                summary["removed"] += 1
            self._rebuild_makers(conn)
            with conn:
                conn.execute("INSERT OR REPLACE INTO meta VALUES ('last_refresh', ?)", (str(time.time()),))
                conn.execute("INSERT OR REPLACE INTO meta VALUES ('last_error', ?)",
                             (f"{summary['failed']} declarations could not be fetched" if summary["failed"] else "",))
            log.info("FAA model table refreshed: %s", summary)
        except (urllib.error.URLError, OSError, ValueError, KeyError, TypeError) as e:
            log.warning("FAA model table not refreshed, keeping the last copy: %s", e)
            with conn:
                conn.execute("INSERT OR REPLACE INTO meta VALUES ('last_error', ?)", (str(e)[:300],))
            summary["error"] = str(e)
        finally:
            conn.close()
            self._refreshing.release()
            with self._lock:
                self._cache.clear()
        return summary

    def _refresh_doc(self, conn: sqlite3.Connection, tracking: str, doc_id: Optional[int], change_key: str):
        detail = json.loads(self._get(f"/publicDOCRev/{tracking}"))["data"]["items"][0]
        rev = detail["publicRevision"]
        rev_id = rev["docRevId"]
        csv_text = self._get(f"/serialNumbers?findBy=docRevIds&docRevIds%5B%5D={rev_id}&limit=-1", "text/csv")
        ranges, singles = parse_serial_values(csv_text)
        start = (rev.get("serialNumberStart") or "").strip().upper()
        end = (rev.get("serialNumberEnd") or "").strip().upper()
        if start and end and len(start) == len(end):
            ranges.append((min(start, end), max(start, end)))
        fields = (rev_id, rev.get("makeName"), rev.get("modelName"), rev.get("series"),
                  (rev.get("certificationType") or {}).get("name"), change_key)
        with conn:      # one transaction per declaration: readers never see it half-written
            if doc_id is None:
                doc_id = conn.execute(
                    "INSERT INTO docs (tracking, rev_id, make, model, series, cert_type, change_key) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)", (tracking,) + fields).lastrowid
            else:
                conn.execute("DELETE FROM ranges WHERE doc = ?", (doc_id,))
                conn.execute("DELETE FROM serials WHERE doc = ?", (doc_id,))
                conn.execute("UPDATE docs SET rev_id = ?, make = ?, model = ?, series = ?, cert_type = ?, "
                             "change_key = ? WHERE id = ?", fields + (doc_id,))
            conn.executemany("INSERT INTO ranges VALUES (?, ?, ?, ?)",
                             [(doc_id, a, b, len(a)) for a, b in ranges])
            conn.executemany("INSERT OR IGNORE INTO serials VALUES (?, ?)", [(s, doc_id) for s in singles])

    def _rebuild_makers(self, conn: sqlite3.Connection):
        """Manufacturer code -> maker, from one example serial per declaration
        (range starts, and the first listed serial). Where declarations by
        different makers share a code, the maker with the most declarations wins."""
        votes: dict = defaultdict(Counter)
        makes = dict(conn.execute("SELECT id, make FROM docs").fetchall())
        examples = conn.execute("SELECT doc, start FROM ranges").fetchall()
        examples += conn.execute("SELECT doc, MIN(serial) FROM serials GROUP BY doc").fetchall()
        seen = set()
        for doc_id, value in examples:
            code = value and manufacturer_code(value)
            if code and doc_id in makes and (doc_id, code) not in seen:
                seen.add((doc_id, code))
                votes[code][makes[doc_id]] += 1
        with conn:
            conn.execute("DELETE FROM makers")
            conn.executemany("INSERT INTO makers VALUES (?, ?)",
                             [(code, c.most_common(1)[0][0]) for code, c in votes.items()])


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--refresh", action="store_true", help="refresh the local copy from the FAA now")
    ap.add_argument("--lookup", nargs="+", metavar="SERIAL", help="look serials up")
    ap.add_argument("--status", action="store_true", help="show what the local copy holds")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    catalog = ModelCatalog()
    if args.refresh:
        print(json.dumps(catalog.refresh()))
    for serial in args.lookup or []:
        info = catalog.lookup(serial)
        print(f"{serial}\t{(info or {}).get('make') or '-'}\t{(info or {}).get('model') or '-'}")
    if args.status or not (args.refresh or args.lookup):
        print(json.dumps(catalog.status(), indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())