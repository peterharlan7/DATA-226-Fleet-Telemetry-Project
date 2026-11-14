"""
Similar-trajectory search with MinHash LSH (B21).

Question: which trips followed (nearly) the same path? Useful to group trips by
the street corridor they use, find detours, and spot overlapping routes.

How it works:
  1. Each trip (source, vehicle, trip_id) becomes the SET of ~200 m grid cells
     its position reports fall in. Two trips on the same streets share most cells.
  2. Similarity of two trips = Jaccard similarity of their cell sets
     (shared cells / all cells of either). Exact Jaccard for every pair is
     O(n^2): with 20,000 trips that is 200 million comparisons.
  3. MinHash LSH (Spark MLlib MinHashLSH) hashes each set so that similar sets
     probably land in the same bucket; only trips sharing a bucket are compared.
     More hash tables = higher recall, more work.
  4. Evaluation: on a random sample, exact Jaccard of ALL pairs is the ground
     truth. For 1, 2, 4, 8, 16 hash tables we measure recall (share of truly similar
     pairs LSH found) and time. LSH verifies its candidates with the exact
     distance, so precision is 100% by construction.
  5. Speed on the full day: LSH self-join time vs brute-force time (measured per
     pair on the sample and scaled to all pairs).
  6. Demo query: the 5 trips most similar to one trip.

Run from the repo root (input = the replay files of the 24 h recording):
    python algorithms/lsh_trajectories.py
    python algorithms/lsh_trajectories.py --threshold 0.6 --sample 1500
Writes benchmarks/results/lsh_trajectories.json and lsh_recall_vs_tables.csv.
"""

import argparse
import itertools
import json
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.schema import VEHICLE_EVENT_FIELDS, spark_schema  # noqa: E402

NUM_FEATURES = 1 << 20          # cell ids are hashed into this many slots (collisions negligible)


def jaccard(a, b):
    return len(a & b) / len(a | b)


def exact_pairs(trips, threshold):
    """Brute force: every pair. Returns (set of similar pairs, seconds, pairs compared)."""
    t0 = time.time()
    found, n = set(), 0
    for (ka, a), (kb, b) in itertools.combinations(trips.items(), 2):
        n += 1
        if jaccard(a, b) >= threshold:
            found.add((ka, kb) if ka < kb else (kb, ka))
    return found, time.time() - t0, n


def build_spark():
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.appName("fleet-lsh").master("local[*]")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "16")
             .config("spark.driver.memory", "4g")
             .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def load_trips(spark, args):
    """One row per trip: key, source, route, cells (sorted feature indices)."""
    from pyspark.sql import functions as F
    ev = (spark.read.schema(spark_schema(VEHICLE_EVENT_FIELDS)).json(args.input)
          .where((F.col("source") != "flush") & F.col("trip_id").isNotNull()
                 & F.col("lat").isNotNull() & F.col("lon").isNotNull()))
    g = args.grid
    cell = F.concat_ws("_", F.floor(F.col("lat") / g), F.floor(F.col("lon") / g))
    ev = ev.withColumn("cell", F.pmod(F.xxhash64(cell), F.lit(NUM_FEATURES)).cast("int"))
    trips = (ev.groupBy("source", "vehicle_id", "trip_id")
               .agg(F.first("route_id", ignorenulls=True).alias("route_id"),
                    F.array_sort(F.collect_set("cell")).alias("cells"),
                    F.count("*").alias("reports"))
               .where(F.size("cells") >= args.min_cells)
               .withColumn("key", F.concat_ws("|", "source", "vehicle_id", "trip_id")))
    return trips


def to_vectors(df):
    from pyspark.ml.linalg import SparseVector, VectorUDT
    from pyspark.sql.functions import col, udf
    as_vec = udf(lambda cells: SparseVector(NUM_FEATURES, cells, [1.0] * len(cells)), VectorUDT())
    return df.withColumn("features", as_vec(col("cells")))


def lsh_pairs_df(df, tables, threshold, seed):
    """Self-join with MinHash LSH: a DataFrame of similar pairs (a, b, source, route_a, route_b)."""
    from pyspark.ml.feature import MinHashLSH
    from pyspark.sql.functions import col
    model = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=tables, seed=seed).fit(df)
    pairs = (model.approxSimilarityJoin(df, df, 1 - threshold + 1e-9, distCol="dist")  # Spark uses dist < t; we want <=
             .where(col("datasetA.key") < col("datasetB.key"))
             .select(col("datasetA.key").alias("a"), col("datasetB.key").alias("b"),
                     col("datasetA.source").alias("source"),
                     col("datasetA.route_id").alias("route_a"), col("datasetB.route_id").alias("route_b")))
    return pairs, model


def lsh_join(df, tables, threshold, seed):
    """Self-join on a small set, collected. Returns (set of similar pairs, seconds, model)."""
    t0 = time.time()
    pairs, model = lsh_pairs_df(df, tables, threshold, seed)
    found = {(r["a"], r["b"]) for r in pairs.select("a", "b").collect()}
    return found, time.time() - t0, model


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input", default="data/replay_in", help="folder of vehicle-event JSON files")
    p.add_argument("--grid", type=float, default=0.002, help="grid cell size in degrees (~200 m)")
    p.add_argument("--min-cells", type=int, default=5, help="ignore trips covering fewer cells")
    p.add_argument("--threshold", type=float, default=0.5, help="similar = Jaccard >= this")
    p.add_argument("--sample", type=int, default=2000, help="trips in the exact-vs-LSH accuracy test")
    p.add_argument("--tables", default="1,2,4,8,16", help="numbers of hash tables to compare")
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--out", default="benchmarks/results")
    args = p.parse_args()
    tables_list = [int(t) for t in args.tables.split(",")]

    spark = build_spark()
    trips = to_vectors(load_trips(spark, args)).cache()
    n = trips.count()
    per_src = {r["source"]: r["count"] for r in trips.groupBy("source").count().collect()}
    print(f"Trips: {n:,} ({', '.join(f'{k} {v:,}' for k, v in sorted(per_src.items()))}) | "
          f"grid {args.grid} deg | similar = Jaccard >= {args.threshold}")

    # ---- Accuracy on a sample: exact all-pairs vs LSH --------------------------------
    rows = trips.select("key", "route_id", "cells").collect()
    rng = random.Random(args.seed)
    sample_rows = rng.sample(rows, min(args.sample, len(rows)))
    sample = {r["key"]: set(r["cells"]) for r in sample_rows}
    truth, exact_s, n_pairs = exact_pairs(sample, args.threshold)
    per_pair_s = exact_s / max(n_pairs, 1)
    print(f"\n== Accuracy on {len(sample):,} sampled trips ({n_pairs:,} pairs)")
    print(f"Exact (brute force): {len(truth):,} similar pairs in {exact_s:.1f} s")

    sample_df = trips.where(trips.key.isin(list(sample))).cache()
    sample_df.count()
    curve = []
    for t in tables_list:
        found, secs, _ = lsh_join(sample_df, t, args.threshold, args.seed)
        recall = len(found & truth) / len(truth) if truth else 1.0
        wrong = len(found - truth)
        curve.append({"hash_tables": t, "recall": round(recall, 4), "pairs_found": len(found),
                      "false_pairs": wrong, "seconds": round(secs, 2)})
        print(f"  LSH {t} table(s): recall {recall:6.1%} ({len(found & truth):,}/{len(truth):,}), "
              f"false pairs {wrong}, {secs:.1f} s")

    # ---- Full day: LSH self-join vs brute force estimate ---------------------------------
    from pyspark.sql import functions as F
    best = max(tables_list)
    t0 = time.time()
    pairs, model = lsh_pairs_df(trips, best, args.threshold, args.seed)
    pairs = pairs.withColumn("same_route", F.col("route_a") == F.col("route_b")).cache()
    n_found = pairs.count()                       # forces the full join
    full_s = time.time() - t0
    same_route = pairs.where("same_route").count()
    top_cross = [((r["source"], r["r1"], r["r2"]), r["count"]) for r in
                 pairs.where(~F.col("same_route"))
                      .select("source", F.least("route_a", "route_b").alias("r1"),
                              F.greatest("route_a", "route_b").alias("r2"))
                      .groupBy("source", "r1", "r2").count().orderBy(F.desc("count")).limit(8).collect()]
    all_pairs = n * (n - 1) // 2
    brute_est = all_pairs * per_pair_s
    print(f"\n== Full day: {n:,} trips = {all_pairs:,} pairs")
    print(f"LSH ({best} tables): {n_found:,} similar pairs in {full_s:.1f} s")
    print(f"Brute force (estimated from the sample's {per_pair_s * 1e6:.1f} us per pair): "
          f"{brute_est:,.0f} s -> LSH is about {brute_est / full_s:,.1f}x faster")
    if n_found:
        print(f"Similar pairs on the same route: {same_route / n_found:.0%}; "
              f"different routes (shared corridors): {n_found - same_route:,}")
        for (src, ra, rb), c in top_cross:
            print(f"  {src} routes {ra} & {rb}: {c:,} similar trip pairs")

    # ---- Demo query --------------------------------------------------------------------
    q = rng.choice([r for r in sample_rows if len(r["cells"]) >= 15] or sample_rows)
    qvec = trips.where(trips.key == q["key"]).first()["features"]
    nn = model.approxNearestNeighbors(trips, qvec, 6, distCol="dist").collect()
    print(f"\n== Query: 5 trips most similar to a route {q['route_id']} trip")
    demo = []
    for r in nn:
        if r["key"] == q["key"]:
            continue
        demo.append({"route_id": r["route_id"], "jaccard": round(1 - r["dist"], 3)})
        print(f"  route {r['route_id']:<8} Jaccard {1 - r['dist']:.2f}")

    os.makedirs(args.out, exist_ok=True)
    result = {"settings": vars(args), "trips": n, "trips_per_source": per_src,
              "sample": {"trips": len(sample), "pairs": n_pairs, "similar_pairs": len(truth),
                         "exact_seconds": round(exact_s, 2)},
              "recall_vs_tables": curve,
              "full_day": {"pairs": all_pairs, "lsh_tables": best, "lsh_similar_pairs": n_found,
                           "lsh_seconds": round(full_s, 1), "brute_force_seconds_est": round(brute_est),
                           "speedup_est": round(brute_est / full_s, 1),
                           "same_route_share": round(same_route / n_found, 3) if n_found else None,
                           "top_cross_route": [{"source": s, "routes": [a, b], "pairs": c}
                                               for (s, a, b), c in top_cross]},
              "query": {"route_id": q["route_id"], "neighbors": demo}}
    with open(os.path.join(args.out, "lsh_trajectories.json"), "w") as f:
        json.dump(result, f, indent=2)
    with open(os.path.join(args.out, "lsh_recall_vs_tables.csv"), "w") as f:
        f.write("hash_tables,recall,pairs_found,false_pairs,seconds\n")
        for c in curve:
            f.write(f"{c['hash_tables']},{c['recall']},{c['pairs_found']},{c['false_pairs']},{c['seconds']}\n")
    print(f"\nSaved {args.out}/lsh_trajectories.json and lsh_recall_vs_tables.csv")


if __name__ == "__main__":
    main()
