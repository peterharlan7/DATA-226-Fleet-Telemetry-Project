"""Tests for B17/B23 streaming algorithms (run from the repo root: python -m pytest algorithms/test_stream_algos.py)."""

import random

import pytest

from algorithms.stream_algos import BloomFilter, FlajoletMartin, ReservoirSampler


# ---- Bloom filter ----------------------------------------------------------

def test_bloom_never_misses_a_duplicate():
    bf = BloomFilter(expected_items=5_000, false_positive_rate=0.01)
    items = [f"SC:v{i}:{i * 1000}" for i in range(5_000)]
    for x in items:
        bf.add(x)
    assert all(x in bf for x in items)                 # no false negatives, ever
    assert all(bf.add(x) for x in items[:500])          # re-adding is always flagged as seen


def test_bloom_false_positive_rate_near_target():
    bf = BloomFilter(expected_items=10_000, false_positive_rate=0.01)
    for i in range(10_000):
        bf.add(f"in-{i}")
    fp = sum(f"out-{i}" in bf for i in range(20_000)) / 20_000
    assert fp < 0.02                                    # about 1%, allow noise


def test_bloom_add_reports_new_items_as_new():
    bf = BloomFilter(expected_items=100)
    assert bf.add("a") is False
    assert bf.add("a") is True


def test_bloom_bad_arguments():
    with pytest.raises(ValueError):
        BloomFilter(expected_items=0)
    with pytest.raises(ValueError):
        BloomFilter(expected_items=10, false_positive_rate=1.5)


# ---- Reservoir sampling ----------------------------------------------------

def test_reservoir_keeps_everything_while_stream_is_short():
    rs = ReservoirSampler(k=10, seed=1)
    for x in range(7):
        rs.add(x)
    assert sorted(rs.sample) == list(range(7))


def test_reservoir_size_stays_k():
    rs = ReservoirSampler(k=10, seed=1)
    for x in range(10_000):
        rs.add(x)
    assert len(rs.sample) == 10 and rs.seen == 10_000


def test_reservoir_is_uniform():
    hits = [0] * 10
    for trial in range(3_000):
        rs = ReservoirSampler(k=10, seed=trial)
        for x in range(100):
            rs.add(x)
        for x in rs.sample:
            hits[x // 10] += 1
    expected = 3_000 * 10 / 10                          # each tenth of the stream: 3,000 picks
    assert all(abs(h - expected) / expected < 0.1 for h in hits)


# ---- Flajolet-Martin -------------------------------------------------------

@pytest.mark.parametrize("n", [100, 1_000, 10_000])
def test_fm_estimate_within_25_percent(n):
    fm = FlajoletMartin()
    for i in range(n):
        fm.add(f"vehicle-{i}")
    assert abs(fm.estimate() - n) / n < 0.25


def test_fm_ignores_repeats():
    once, many = FlajoletMartin(), FlajoletMartin()
    for i in range(1_000):
        once.add(f"v{i}")
        for _ in range(5):
            many.add(f"v{i}")
    assert once.estimate() == many.estimate()           # same distinct items -> same state


def test_fm_memory_is_tiny():
    fm = FlajoletMartin()
    for i in range(50_000):
        fm.add(i)
    assert fm.memory_bytes() == 512
