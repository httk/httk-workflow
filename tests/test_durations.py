"""The Slurm ``--time`` spelling of the reserved time resources."""

import pytest

from httk.workflow._durations import cap_maxtime, format_duration, parse_slurm_duration


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
