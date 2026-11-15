"""
Streaming algorithms from the course, implemented from scratch (B17 + B23).

Each one reads a stream ONE item at a time, uses a small fixed amount of memory,
and never stores the whole stream. That is the point of streaming algorithms:
the stream may be endless, so we can't keep everything.

  BloomFilter      "Have I seen this event_id before?"  -> duplicate detection (B17)
                   Fixed memory, never misses a real duplicate, but can wrongly say
                   "seen" for a new item (false positive) at a rate we choose.
  ReservoirSampler A uniform random sample of k items from a stream of unknown length (B23)
                   Every item seen so far has the same chance k/n of being in the sample.
  FlajoletMartin   "How many DISTINCT vehicles?" estimated from a few small numbers (B23)
                   Uses trailing zeros of hashes; error shrinks as more hashes are combined.

No Spark here: plain Python, so the logic can be tested on its own.
streaming/stream_algorithms.py applies these to the live stream.

Run the self-test:  python algorithms/stream_algos.py
"""

import hashlib
import math
import random
import statistics


def _hash64(item, salt=b""):
    """A 64-bit hash of any item. Different salts give independent hash functions."""
    digest = hashlib.blake2b(str(item).encode("utf-8"), digest_size=8, salt=salt[:16]).digest()
    return int.from_bytes(digest, "big")


# ---------------------------------------------------------------------------
# Bloom filter (B17)
# ---------------------------------------------------------------------------

class BloomFilter:
    """Set membership with fixed memory and no false negatives.

    Sized from the expected number of items n and the false-positive rate p:
        bits      m = -n ln(p) / (ln 2)^2
        hashes    k = (m / n) ln 2
    """

    def __init__(self, expected_items, false_positive_rate=0.01):
        if expected_items < 1 or not 0 < false_positive_rate < 1:
            raise ValueError("need expected_items >= 1 and 0 < false_positive_rate < 1")
        self.n_expected = expected_items
        self.p_target = false_positive_rate
        self.m = max(8, math.ceil(-expected_items * math.log(false_positive_rate) / math.log(2) ** 2))
        self.k = max(1, round(self.m / expected_items * math.log(2)))
        self.bits = bytearray((self.m + 7) // 8)
        self.added = 0

    def _positions(self, item):
        # Double hashing: k positions from two hashes (Kirsch-Mitzenmacher).
        h1 = _hash64(item, b"bloom-1")
        h2 = _hash64(item, b"bloom-2") | 1
        return [(h1 + i * h2) % self.m for i in range(self.k)]

    def __contains__(self, item):
        return all(self.bits[p >> 3] & (1 << (p & 7)) for p in self._positions(item))

    def add(self, item):
        """Add item. Returns True if it was (probably) already there, i.e. a duplicate."""
        seen = True
        for p in self._positions(item):
            byte, bit = p >> 3, 1 << (p & 7)
            if not self.bits[byte] & bit:
                seen = False
                self.bits[byte] |= bit
        if not seen:
            self.added += 1
        return seen

    def memory_bytes(self):
        return len(self.bits)

    def expected_fp_rate(self):
        """False-positive rate predicted for the number of items added so far."""
        return (1 - math.exp(-self.k * self.added / self.m)) ** self.k


# ---------------------------------------------------------------------------
# Reservoir sampling (B23)
# ---------------------------------------------------------------------------

class ReservoirSampler:
    """Uniform random sample of k items from a stream (Algorithm R)."""

    def __init__(self, k, seed=None):
        if k < 1:
            raise ValueError("k must be at least 1")
        self.k = k
        self.sample = []
        self.seen = 0
        self.rng = random.Random(seed)

    def add(self, item):
        self.seen += 1
        if len(self.sample) < self.k:
            self.sample.append(item)              # fill the reservoir first
        else:
            j = self.rng.randint(1, self.seen)    # keep the new item with probability k/n
            if j <= self.k:
                self.sample[j - 1] = item


# ---------------------------------------------------------------------------
# Flajolet-Martin distinct counting (B23)
# ---------------------------------------------------------------------------

class FlajoletMartin:
    """Estimate the number of distinct items.

    For each hash function, keep a small bitmap: when an item's hash ends in r
    zeros, set bit r. With n distinct items, bits 0, 1, 2, ... up to about
    log2(n) all get set. R = the lowest bit still unset, so 2^R / PHI estimates n.
    One hash is noisy, so we use many: average R within groups, then take the
    median across groups ("median of means"), which resists unlucky hashes.
    """

    PHI = 0.77351   # Flajolet-Martin correction factor

    def __init__(self, num_hashes=64, group_size=8):
        if num_hashes % group_size:
            raise ValueError("num_hashes must be a multiple of group_size")
        self.num_hashes = num_hashes
        self.group_size = group_size
        self.salts = [f"fm-{i}".encode() for i in range(num_hashes)]
        self.bitmaps = [0] * num_hashes

    @staticmethod
    def _trailing_zeros(x):
        if x == 0:
            return 63
        return (x & -x).bit_length() - 1

    @staticmethod
    def _lowest_unset_bit(bitmap):
        r = 0
        while bitmap & (1 << r):
            r += 1
        return r

    def add(self, item):
        for i, salt in enumerate(self.salts):
            self.bitmaps[i] |= 1 << self._trailing_zeros(_hash64(item, salt))

    def estimate(self):
        rs = [self._lowest_unset_bit(b) for b in self.bitmaps]
        groups = [rs[i:i + self.group_size] for i in range(0, self.num_hashes, self.group_size)]
        return statistics.median(2 ** statistics.mean(g) / self.PHI for g in groups)

    def memory_bytes(self):
        return self.num_hashes * 8   # one 64-bit bitmap per hash function


if __name__ == "__main__":
    rng = random.Random(42)

    print("== Bloom filter: 20,000 events, 5% sent twice")
    events = [f"SC:bus{rng.randint(1, 500)}:{i}" for i in range(20_000)]
    stream = events + rng.sample(events, 1_000)          # 1,000 true duplicates
    rng.shuffle(stream)
    bf = BloomFilter(expected_items=25_000, false_positive_rate=0.01)
    exact, true_dupes, flagged, false_pos = set(), 0, 0, 0
    for e in stream:
        is_dup = e in exact
        said_dup = bf.add(e)
        exact.add(e)
        true_dupes += is_dup
        flagged += said_dup
        false_pos += said_dup and not is_dup
    print(f"  bits m={bf.m:,}  hashes k={bf.k}  memory {bf.memory_bytes():,} bytes "
          f"(an exact set of these ids uses ~{sum(len(x) for x in exact) + 50 * len(exact):,} bytes)")
    print(f"  true duplicates {true_dupes} | flagged {flagged} | missed {true_dupes - (flagged - false_pos)} "
          f"| false positives {false_pos} ({false_pos / len(exact):.2%} of new items, target 1%)")

    print("== Reservoir sampling: is the sample uniform?")
    hits = [0] * 10
    for trial in range(2_000):
        rs = ReservoirSampler(k=10, seed=trial)
        for x in range(100):
            rs.add(x)
        for x in rs.sample:
            hits[x // 10] += 1
    print("  how often each tenth of the stream ended up in the sample (expect ~2000 each):", hits)

    print("== Flajolet-Martin: distinct vehicles")
    for n in (100, 1_000, 10_000):
        fm = FlajoletMartin()
        for i in range(n):
            for _ in range(3):                        # each vehicle reports several times
                fm.add(f"vehicle-{i}")
        est = fm.estimate()
        print(f"  true {n:>6,} | estimate {est:>8,.0f} | error {abs(est - n) / n:.0%} | memory {fm.memory_bytes()} bytes")
