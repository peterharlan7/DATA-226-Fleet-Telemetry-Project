"""
Turn a recording (ingestion/record_stream.py output) into input files for the
streaming detectors, keeping the ORIGINAL event timestamps.

Writes the vehicle-position events, in the order they arrived, as numbered
JSON-lines files into a folder. Run a detector on that folder with
--max-files 1: Spark then reads one file per micro-batch, so event time moves
forward the way it did live and windows close in order.

A final "flush" event one hour after the last report pushes the watermark past
every window, so the last windows close too. It has its own source ("flush")
and never produces an alert.

Run from the repo root:
    python streaming/replay_recording.py                                  # data/recording_24h.jsonl -> data/replay_in
    python streaming/detect_bunching.py --input data/replay_in --max-files 1 --trigger 1 --quiet \\
        --output data/alerts/bunching_replay --checkpoint data/checkpoints/bunching_replay
"""

import argparse
import json
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.schema import SCHEMA_VERSION  # noqa: E402


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--recording", default="data/recording_24h.jsonl")
    p.add_argument("--out", default="data/replay_in")
    p.add_argument("--per-file", type=int, default=5000,
                   help="events per file (5,000 is about 10 minutes of Muni + VTA)")
    args = p.parse_args()

    if os.path.isdir(args.out):
        shutil.rmtree(args.out)                     # start clean: old files would be re-read
    os.makedirs(args.out)

    buf, n_files, n_events, skipped, last_ts, first_ts = [], 0, 0, 0, 0, None

    def flush_file():
        nonlocal buf, n_files
        if buf:
            with open(os.path.join(args.out, f"part_{n_files:05d}.json"), "w") as f:
                f.writelines(buf)
            n_files += 1
            buf = []

    with open(args.recording) as f:
        for line in f:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1                        # half-written last line
                continue
            ev = rec.get("event", rec)
            if "lat" not in ev or ev.get("event_ts") is None:
                continue                            # trip updates
            buf.append(json.dumps(ev) + "\n")
            n_events += 1
            last_ts = max(last_ts, ev["event_ts"])
            first_ts = ev["event_ts"] if first_ts is None else min(first_ts, ev["event_ts"])
            if len(buf) >= args.per_file:
                flush_file()
    flush_file()

    t = last_ts + 3_600_000
    buf = [json.dumps({"event_id": f"flush:flush:{t}", "schema_version": SCHEMA_VERSION,
                       "source": "flush", "vehicle_id": "flush", "route_id": "flush",
                       "trip_id": None, "direction_id": 0, "event_ts": t, "ingest_ts": t,
                       "lat": 0.0, "lon": 0.0, "bearing": None, "speed_mps": None,
                       "current_status": None, "stop_id": None}) + "\n"]
    flush_file()

    import datetime
    hours = (last_ts - first_ts) / 3_600_000 if first_ts else 0
    utc = lambda ms: datetime.datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M")   # local time
    print(f"Wrote {n_events:,} vehicle events covering {hours:.1f} h into {n_files} files in {args.out} "
          f"(unreadable lines skipped: {skipped})")
    if first_ts:
        print(f"Event time (local): {utc(first_ts)} to {utc(last_ts)}. When a detector's progress reaches "
              f"{utc(last_ts)} and nothing new prints for 30 seconds, press Ctrl+C.")


if __name__ == "__main__":
    main()
