"""Layer-1 liveness: publish one message through a real broker; the ear
(running without S3) must hear it and count it. Needs the gwbase dev broker
(`gw-dev-rabbit`, topology baked); self-skips in CI, whose bare broker has
no gwbase topology."""

import contextlib
import os
import time
import uuid
from collections.abc import Callable

import pika
from gear.config import EarSettings
from gear.ear import Ear

# rjb.<from-alias-lrh>.<from-rc>.<type-lrh> — a broadcast shape; the dev
# topology fans amq.topic into ear_tx, which the ear binds with `#`.
TEST_ROUTING_KEY = "rjb.d1-super1.super.hb-a"


def wait_for(f: "Callable[[], bool]", timeout: float, tag: str) -> None:
    start = time.time()
    while time.time() - start < timeout:
        if f():
            return
        time.sleep(0.05)
    msg = f"timed out after {timeout}s: {tag}"
    raise AssertionError(msg)


def test_start_one_message() -> None:
    if "GITHUB_ACTIONS" in os.environ:
        print("CI has a bare broker (no gwbase topology); skipping.")
        return
    settings = EarSettings(service_alias=f"d1.tap{uuid.uuid4().hex[:4]}")
    ear = Ear(settings, use_s3=False)
    ear.start()
    try:
        wait_for(lambda: ear._consuming, timeout=10.0, tag="ear consuming")  # noqa: SLF001
        heard_start = ear.messages_heard_total
        conn = pika.BlockingConnection(
            pika.URLParameters(settings.rabbit.url.get_secret_value())
        )
        ch = conn.channel()
        ch.basic_publish(
            exchange="amq.topic",
            routing_key=TEST_ROUTING_KEY,
            body=b'{"TypeName": "hb.a", "MyHex": "0"}',
        )
        conn.close()
        wait_for(
            lambda: ear.messages_heard_total > heard_start,
            timeout=5.0,
            tag="ear hears the published message",
        )
    finally:
        with contextlib.suppress(Exception):
            ear.stop()
