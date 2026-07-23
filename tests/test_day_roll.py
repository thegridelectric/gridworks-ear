"""The day folder must roll at the UTC day boundary — the object-key
`<world>/eventstore/<YYYYMMDD>` date is load-bearing for every reader."""

import time

import pytest
from gear.config import EarSettings
from gear.ear import Ear, day_folder_from_unix_s

# 2026-07-23 23:59:50 UTC and 10s later, across midnight.
BEFORE_MIDNIGHT = 1784851190
AFTER_MIDNIGHT = 1784851200


def test_day_folder_from_unix_s_boundary() -> None:
    assert day_folder_from_unix_s(BEFORE_MIDNIGHT) == "20260723"
    assert day_folder_from_unix_s(AFTER_MIDNIGHT) == "20260724"


def test_periodic_tick_rolls_day_folder(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_now = BEFORE_MIDNIGHT
    monkeypatch.setattr(time, "time", lambda: fake_now)
    ear = Ear(EarSettings(service_alias="d1.tap1"), use_s3=False)
    assert ear.output_folder_root.endswith("/20260723")

    fake_now = AFTER_MIDNIGHT
    ear.periodic_tick()
    assert ear.output_folder_root.endswith("/20260724")
