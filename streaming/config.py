"""
Detector settings in one place (B12 and later detectors).

Change a number here, not inside the detector code, so experiments (B25) can
compare settings without editing logic. Every value can also be overridden
on the command line, e.g. --max-distance 150.
"""

BUNCHING = {
    # Two buses on the same route and direction closer than this are "bunched".
    "max_distance_m": 200,
    # Only compare two position reports taken within this many seconds of each
    # other, so a bus is not compared with where another bus was a minute ago.
    "max_time_gap_s": 60,
    # The "current minute" each window reports on. With the refinement on (min_move_m > 0)
    # windows are sliding: lookback_s + window_s long, moving forward window_s at a time.
    # With min_move_m = 0 they are tumbling windows of window_s. Bigger = fewer, later alerts.
    "window_s": 60,
    # How long to wait for late reports before closing a window (watermark).
    # Bigger = fewer late reports missed, but alerts arrive later.
    "watermark_s": 30,
    # Refinement after the live-data analysis (streaming/analyze_bunching.py):
    # how far back to look to decide whether each vehicle is in service or parked.
    "lookback_s": 300,
    # A vehicle that moved less than this in the look-back is parked (GPS drift ~30 m).
    # Pairs with a parked vehicle are layovers, not bunching. 0 = off (raw detector).
    "min_move_m": 50,
    # Need at least this much history (seconds between first and last report) to judge movement.
    "min_history_s": 30,
    # Always closer than this at every shared report time (3+ times) -> "tandem" bunching.
    "tandem_m": 40,
}

STALLED = {
    # B16: an in-service vehicle that stays within still_m for stall_minutes, while NOT
    # reporting STOPPED_AT (a vehicle waiting at a stop or terminal reports STOPPED_AT).
    "stall_minutes": 10,
    "still_m": 50,
    "watermark_s": 30,
    # Need at least this many reports while stationary (feeds poll about every 2 minutes).
    "min_reports": 3,
}
