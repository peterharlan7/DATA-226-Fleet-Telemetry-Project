"""
Download a few Argoverse 2 motion-forecasting scenarios and combine them into one
replay file for ingestion/av_replay.py (B19).

Argoverse 2 is public on Amazon S3 (no account needed). Each scenario is about
11 seconds of a self-driving car's view at 10 Hz: every tracked object's position
in local city coordinates (meters). We keep vehicles and buses and place the
scenarios one after another (scenario i starts 11 s after scenario i-1).

Run from the repo root:
    python ingestion/prepare_av2.py                     # 20 scenarios -> data/av2/av2_20_scenarios.parquet
    python ingestion/prepare_av2.py --n 50
Then replay it (see av_replay.py), e.g. at 10x speed:
    python ingestion/av_replay.py --parquet data/av2/av2_20_scenarios.parquet --source ARGO \\
        --id-cols scenario_id,track_id --x-col position_x --y-col position_y --time-col t_sec --speed 10
"""

import argparse
import glob
import os
import re
import urllib.request

import pandas as pd

BUCKET = "https://argoverse.s3.amazonaws.com"
PREFIX = "datasets/av2/motion-forecasting/val/"


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--n", type=int, default=20, help="number of scenarios")
    p.add_argument("--dir", default="data/av2")
    args = p.parse_args()

    os.makedirs(args.dir, exist_ok=True)
    listing = urllib.request.urlopen(f"{BUCKET}/?list-type=2&prefix={PREFIX}&max-keys={args.n * 2}").read().decode()
    keys = [k for k in re.findall(r"<Key>([^<]+)</Key>", listing) if k.endswith(".parquet")][:args.n]
    for k in keys:
        dest = os.path.join(args.dir, os.path.basename(k))
        if not os.path.exists(dest):
            urllib.request.urlretrieve(f"{BUCKET}/{k}", dest)
    print(f"Scenarios in {args.dir}: {len(keys)}")

    parts = []
    for i, f in enumerate(sorted(glob.glob(os.path.join(args.dir, "scenario_*.parquet")))[:args.n]):
        df = pd.read_parquet(f, columns=["scenario_id", "track_id", "object_type", "timestep",
                                         "position_x", "position_y", "city"])
        df = df[df.object_type.isin(["vehicle", "bus"])].copy()
        df["t_sec"] = i * 11.0 + df.timestep * 0.1
        parts.append(df)
    out = pd.concat(parts, ignore_index=True)
    path = os.path.join(args.dir, f"av2_{len(parts)}_scenarios.parquet")
    out.to_parquet(path, index=False)
    print(f"{len(out):,} vehicle readings | {out.groupby(['scenario_id', 'track_id']).ngroups:,} vehicles | "
          f"{out.t_sec.max() / 60:.1f} min | {out.city.value_counts().to_dict()} -> {path}")


if __name__ == "__main__":
    main()
