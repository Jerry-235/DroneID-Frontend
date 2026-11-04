"""One-time backfill: give every flight that has no place name the name of its
first GPS fix, the same name the Discord alert uses. New flights are named as
they start, so this only needs running once, against the history recorded
before that change.

Lookups run one at a time. places.py holds OpenStreetMap to one request per
second, so a long history takes a while. Landmark names are instant. Each name
is saved as soon as it's found, so Ctrl-C is safe and re-running only retries
flights that are still unnamed.

Stop the frontend server while this runs.

Usage:  python backfill_flight_locations.py
        python backfill_flight_locations.py --landmarks

--landmarks re-reads landmarks.csv and renames any flight whose first fix falls
inside a landmark, so edited landmark names reach History. It makes no network
calls, and flights with no landmark keep the name they already have.
"""
import argparse
import asyncio

import db
import places


async def backfill():
    db.init()
    ids = db._flights_without_location()
    print(f"{len(ids)} flights to name", flush=True)
    named = no_fix = not_found = 0
    for n, flight_id in enumerate(ids, 1):
        lat, lon = db._flight_origin(flight_id)
        if lat is None:
            # Never had a GPS fix, so there is nothing to name. Marked so it
            # isn't looked at again.
            db._set_flight_location(flight_id, "")
            no_fix += 1
            status = "no GPS fix"
        else:
            place = await places.resolve(lat, lon, timeout=15)
            if place:
                db._set_flight_location(flight_id, place.name)
                named += 1
                status = place.name
            else:
                not_found += 1
                status = "no name found (re-run to retry)"
        print(f"[{n}/{len(ids)}] flight {flight_id}: {status}", flush=True)
    print(f"done: {named} named, {no_fix} without GPS, {not_found} not found", flush=True)


def relabel_from_landmarks():
    """Set each flight's name to the landmark covering its first fix, where one
    does. Landmarks win over OpenStreetMap names, the same rule the Discord alert
    uses, so this only changes flights whose stored name differs."""
    db.init()
    conn = db._require_conn()
    rows = conn.execute("SELECT id, location FROM flights").fetchall()
    changed = 0
    for flight_id, current in rows:
        lat, lon = db._flight_origin(flight_id)
        if lat is None:
            continue
        name = places.nearest_landmark(lat, lon)
        if name and name != current:
            db._set_flight_location(flight_id, name)
            changed += 1
            print(f"flight {flight_id}: {current!r} -> {name!r}", flush=True)
    print(f"done: {changed} of {len(rows)} flights renamed from landmarks.csv", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--landmarks", action="store_true",
                        help="rename flights from landmarks.csv (no network calls)")
    args = parser.parse_args()
    try:
        if args.landmarks:
            relabel_from_landmarks()
        else:
            asyncio.run(backfill())
    except KeyboardInterrupt:
        print("\nstopped; progress is saved, re-run to continue", flush=True)
