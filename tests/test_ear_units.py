"""Unit coverage for the ear's storage contract — no broker, fake S3.

The object key is the ear's public contract; the fallback/retry pair is
its no-message-silently-dropped guarantee; the `_unparsed_` path is the
lossless-witness behavior."""

import re
import time
from pathlib import Path

import gear.ear
import pytest
from botocore.exceptions import ClientError
from gear.config import EarSettings
from gear.ear import Ear
from gwbase.transport_encoding import parse_routing_key

OK_RESULT = {"ResponseMetadata": {"HTTPStatusCode": 200}}


class FakeObject:
    def __init__(self, store: "FakeS3", bucket: str, path: str) -> None:
        self.store = store
        self.bucket = bucket
        self.path = path

    def put(self, *, Body: bytes) -> dict:  # noqa: N803 — boto3's spelling
        if not self.store.ok:
            raise ClientError({"Error": {"Code": "boom"}}, "PutObject")
        self.store.puts.append((self.bucket, self.path, Body))
        return OK_RESULT


class FakeS3:
    def __init__(self, *, ok: bool = True) -> None:
        self.ok = ok
        self.puts: list[tuple[str, str, bytes]] = []

    def Object(self, bucket: str, path: str) -> FakeObject:  # noqa: N802 — boto3's spelling
        return FakeObject(self, bucket, path)


@pytest.fixture()
def ear(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Ear:
    monkeypatch.setattr(gear.ear, "OUTPUT_DIRECTORY", tmp_path / "output")
    monkeypatch.setattr(gear.ear, "STATE_DIRECTORY", tmp_path / "state")
    e = Ear(EarSettings(service_alias="d1.tap1"), use_s3=True)
    e.s3_resource = FakeS3()
    return e


def fake_s3(e: Ear) -> FakeS3:
    assert isinstance(e.s3_resource, FakeS3)
    return e.s3_resource


def test_object_key_grammar(ear: Ear) -> None:
    envelope = parse_routing_key("rjb.d1-tap9.super.hb-a")
    ear.dispatch_message(envelope=envelope, body=b'{"TypeName": "hb.a"}')
    (bucket, path, body) = fake_s3(ear).puts[-1]
    assert bucket == "gwdev"
    assert re.fullmatch(
        r"d1__1/eventstore/\d{8}/d1\.tap9-hb\.a-\d{13}-d1\.tap1\.json", path
    )
    assert body == b'{"TypeName": "hb.a"}'


def test_unparsed_routing_key_is_stored_verbatim(ear: Ear) -> None:
    ear.on_routing_key_parse_error(
        routing_key="broadcast.glitch", body=b"raw-bytes", error=ValueError("bad")
    )
    (_, path, body) = fake_s3(ear).puts[-1]
    assert "_unparsed_broadcast.glitch-" in path
    assert path.endswith("-d1.tap1.txt")
    assert body == b"raw-bytes"
    assert ear.messages_heard_total == 1


def test_failed_put_falls_back_to_local_cache(ear: Ear) -> None:
    fake_s3(ear).ok = False
    envelope = parse_routing_key("rjb.d1-tap9.super.hb-a")
    ear.dispatch_message(envelope=envelope, body=b"precious")
    assert ear.s3_put_works is False
    cached = list(ear.local_cache_dir.iterdir())
    assert len(cached) == 1
    assert cached[0].read_bytes() == b"precious"


def test_cache_retry_uploads_and_unlinks(ear: Ear) -> None:
    (ear.local_cache_dir / "stuck.json").write_bytes(b"stuck")
    ear.try_to_empty_cache()
    assert list(ear.local_cache_dir.iterdir()) == []
    assert fake_s3(ear).puts[-1][2] == b"stuck"


def test_cache_retry_leaves_file_on_failure(ear: Ear) -> None:
    (ear.local_cache_dir / "stuck.json").write_bytes(b"stuck")
    fake_s3(ear).ok = False
    ear.try_to_empty_cache()
    assert [p.name for p in ear.local_cache_dir.iterdir()] == ["stuck.json"]


def test_probe_recovers_s3_put_works_and_drains_cache(ear: Ear) -> None:
    (ear.local_cache_dir / "stuck.json").write_bytes(b"stuck")
    ear.s3_put_works = False
    ear.update_s3_put_works()
    assert ear.s3_put_works is True
    assert list(ear.local_cache_dir.iterdir()) == []
    assert fake_s3(ear).puts[-1][2] == b"stuck"


def test_healthy_minute_tick_writes_no_heartbeat(ear: Ear) -> None:
    ear._last_minute_s = int(time.time()) - 120  # noqa: SLF001
    before = len(fake_s3(ear).puts)
    ear.periodic_tick()
    assert len(fake_s3(ear).puts) == before


def test_hourly_tick_warns_on_silence_and_resets_counter(
    ear: Ear, caplog: pytest.LogCaptureFixture
) -> None:
    ear._last_hour_s = int(time.time()) - 3700  # noqa: SLF001
    ear.messages_heard_this_hour = 0
    with caplog.at_level("WARNING", logger="ear.state"):
        ear.periodic_tick()
    assert any("heard 0 messages" in r.message for r in caplog.records)

    ear._last_hour_s = int(time.time()) - 3700  # noqa: SLF001
    ear.messages_heard_this_hour = 5
    with caplog.at_level("WARNING", logger="ear.state"):
        caplog.clear()
        ear.periodic_tick()
    assert not any("heard 0 messages" in r.message for r in caplog.records)
    assert ear.messages_heard_this_hour == 0
