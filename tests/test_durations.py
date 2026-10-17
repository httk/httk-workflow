"""The Slurm ``--time`` spelling of the reserved time resources."""

import pytest

from httk.workflow._durations import (
    cap_maxtime,
    effective_time_limit,
    format_duration,
    parse_slurm_duration,
    time_limit_bound,
)


@pytest.mark.parametrize(
    ("text", "seconds"),
    [
        ("0", 0),
        ("30", 1800),
        ("30:00", 1800),
        ("1:05", 65),
        ("1:30:00", 5400),
        ("100:00:00", 360000),
        ("2-12", 216000),
        ("1-0:30", 88200),
        ("1-00:00:05", 86405),
        ("0-23:59:59", 86399),
    ],
)
def test_accepted_spellings(text: str, seconds: int) -> None:
    assert parse_slurm_duration(text) == seconds


@pytest.mark.parametrize(
    "text",
    ["", "1:60", "1:00:60", "1:60:00", "1-24", "-5", " 5", "5 ", "1.5", "1:2:3:4", "1-", "UNLIMITED", "INFINITE", "٣"],
)
def test_rejected_spellings(text: str) -> None:
    with pytest.raises(ValueError, match="M:S, H:M:S"):
        parse_slurm_duration(text)


@pytest.mark.parametrize(("seconds", "text"), [(0, "00:00:00"), (5400, "01:30:00"), (86405, "1-00:00:05")])
def test_format_duration_round_trips(seconds: int, text: str) -> None:
    assert format_duration(seconds) == text
    assert parse_slurm_duration(text) == seconds


def test_cap_maxtime_clamps_within_each_mapping_only() -> None:
    assert cap_maxtime({"mintime": 7200}, {"s": {"maxtime": 7200, "mintime": 7200}}, 3600) == (
        {"maxtime": 3600, "mintime": 3600},
        {"s": {"maxtime": 3600, "mintime": 3600}},
    )
    # A step naming only mintime is left as is; the scheduler clamps it on resolution.
    assert cap_maxtime({}, {"s": {"mintime": 7200}}, 3600) == ({"maxtime": 3600}, {"s": {"mintime": 7200}})
    assert cap_maxtime({"procs": 1}, {}, None) == ({"procs": 1}, {})


def test_time_limit_bound_prefers_the_maximum_and_reads_integer_minutes() -> None:
    assert time_limit_bound({"slurm.time_limit": "1:00:00", "slurm.time_limit_max": "2-0"}) == 172800
    assert time_limit_bound({"slurm.time_limit": 120}) == 7200
    assert time_limit_bound({}) is None
    with pytest.raises(ValueError, match="slurm.time_limit_max"):
        time_limit_bound({"slurm.time_limit_max": "soon"})
    with pytest.raises(ValueError, match="slurm.time_limit"):
        time_limit_bound({"slurm.time_limit": True})


def test_effective_time_limit_normalizes_and_bounds_a_request() -> None:
    configured = {"slurm.time_limit": "2:00:00"}
    assert effective_time_limit(configured, None) == "02:00:00"
    assert effective_time_limit({"slurm.time_limit": 90}, None) == "01:30:00"
    assert effective_time_limit({}, None) is None
    assert effective_time_limit(configured, "90") == "01:30:00"
    assert effective_time_limit(configured, "120") == "02:00:00"
    assert effective_time_limit({**configured, "slurm.time_limit_max": "1-0"}, "1-0") == "1-00:00:00"
    for requested in ("121", "0", "soon"):
        with pytest.raises(ValueError):
            effective_time_limit(configured, requested)
    with pytest.raises(ValueError):
        effective_time_limit({**configured, "slurm.time_limit_max": "3:00:00"}, "3:00:01")
    with pytest.raises(ValueError):
        effective_time_limit({}, "10")
    # Slurm also takes spellings the duration grammar does not, and reads 0 as no limit: without a request
    # they pass verbatim, and they bound no request.
    for unbounded in ("UNLIMITED", "0"):
        assert effective_time_limit({"slurm.time_limit": unbounded}, None) == unbounded
        with pytest.raises(ValueError):
            effective_time_limit({"slurm.time_limit": unbounded}, "10")
    with pytest.raises(ValueError, match="slurm.time_limit"):
        time_limit_bound({"slurm.time_limit": 0})
