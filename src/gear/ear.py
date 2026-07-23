"""The ear: a passive audit witness on one exchange of a GridWorks broker.

A modern-gwbase `ActorBase` tap: the framework parses each routing key into
a `RoutingEnvelope` (who spoke, what type they claimed) and hands over the
raw bytes; the ear stores them verbatim in an S3-compatible object store.
It validates nothing — meaning lives in Sema, and the store's job is to
preserve what was actually said. Messages whose routing keys do not parse
are stored too (`_unparsed_` keys): the malformed utterances are exactly
what an audit wants.
"""

import functools
import os
import time
from datetime import UTC, datetime
from http import HTTPStatus
from pathlib import Path
from typing import no_type_check

import boto3
from botocore.exceptions import ClientError, EndpointConnectionError
from gwbase import ActorBase
from gwbase.transport_encoding import RoutingEnvelope
from mypy_boto3_s3 import S3ServiceResource

from gear.config import EarSettings
from gear.utils import EAR_MESSAGE_LOGGER as LGMSG
from gear.utils import EAR_STATE_LOGGER as LGST
from gear.utils import OUTPUT_DIRECTORY, STATE_DIRECTORY


def day_folder_from_unix_s(time_unix_s: int) -> str:
    return datetime.fromtimestamp(time_unix_s, tz=UTC).strftime("%Y%m%d")


class Ear(ActorBase):
    """Consume one exchange with `#`; one message becomes one object."""

    def __init__(self, settings: EarSettings, *, use_s3: bool = True) -> None:
        super().__init__(settings=settings)
        self.settings: EarSettings = settings
        self._consume_exchange = settings.consume_exchange
        self.use_s3 = use_s3
        self.s3_put_works: bool = use_s3
        self.s3_resource: S3ServiceResource | None = None
        self.messages_heard_this_hour = 0
        self.messages_heard_total = 0

        self.local_cache_dir = OUTPUT_DIRECTORY / (
            f"need_to_put/{self.settings.world_instance_alias}"
        )
        self.local_cache_dir.mkdir(exist_ok=True, parents=True)
        STATE_DIRECTORY.mkdir(exist_ok=True, parents=True)

        now = int(time.time())
        self._day_folder = day_folder_from_unix_s(now)
        self._last_minute_s = now - (now % 60)
        self._last_hour_s = now - (now % 3600)
        self._last_day_s = now - (now % 86400)

    # ------------------------------------------------------------------
    # Queue binding — the whole slice: `#` on the consume exchange
    # ------------------------------------------------------------------

    @no_type_check
    def bind_queue(self) -> None:
        """Bind `#` on the configured consume exchange, then QoS (via
        `on_direct_message_bindok`, matching the framework's bind flow)."""
        LGST.info("Binding %s to %s with #", self.queue_name, self._consume_exchange)
        cb = functools.partial(self.on_direct_message_bindok, binding="#")
        self._single_channel.queue_bind(
            self.queue_name,
            self._consume_exchange,
            routing_key="#",
            callback=cb,
        )

    # ------------------------------------------------------------------
    # Dispatch — store verbatim
    # ------------------------------------------------------------------

    def dispatch_message(self, *, envelope: RoutingEnvelope, body: bytes) -> None:
        self.messages_heard_this_hour += 1
        self.messages_heard_total += 1
        file_name = (
            f"{envelope.from_alias}-{envelope.type_name}"
            f"-{int(time.time() * 1000)}-{self.alias}.json"
        )
        self._store(file_name, body)

    def on_routing_key_parse_error(
        self, *, routing_key: str, body: bytes, error: ValueError
    ) -> None:
        """A witness drops nothing: an unparseable routing key is stored
        under an `_unparsed_` key carrying the offending routing key, body
        verbatim as always."""
        self.messages_heard_this_hour += 1
        self.messages_heard_total += 1
        LGST.warning(f"Unparseable routing key {routing_key!r}: {error}")
        file_name = (
            f"_unparsed_{routing_key}-{int(time.time() * 1000)}-{self.alias}.txt"
        )
        self._store(file_name, body)

    def _store(self, file_name: str, body: bytes) -> None:
        put_worked = False
        if self.use_s3 and self.s3_put_works:
            put_worked = self.put_in_s3(file_name, body)
        if not put_worked:
            self.store_locally(file_name, body)

    # ------------------------------------------------------------------
    # S3
    # ------------------------------------------------------------------

    @property
    def output_folder_root(self) -> str:
        """Objects live under `<world_instance_alias>/eventstore/<YYYYMMDD>`;
        the day folder rolls at the UTC day boundary (daily chore)."""
        return f"{self.settings.world_instance_alias}/eventstore/{self._day_folder}"

    def possibly_update_s3_folder(self) -> bool:
        old = self._day_folder
        self._day_folder = day_folder_from_unix_s(int(time.time()))
        changed = old != self._day_folder
        if changed:
            LGST.info(f"S3 day folder rolled: {old} -> {self._day_folder}")
        return changed

    def put_in_s3(self, file_name: str, payload: bytes) -> bool:
        """Put one object; on any failure flag the store as down (the
        heartbeat probe flips it back) and return False so the caller
        caches locally."""
        path_name = f"{self.output_folder_root}/{file_name}"
        if self.s3_resource is None:
            self.s3_resource = boto3.Session(
                region_name=self.settings.s3.region_name,
                profile_name=self.settings.s3.profile_name,
            ).resource(
                "s3",
                endpoint_url=self.settings.s3.endpoint_url or None,
            )
        s3_object = self.s3_resource.Object(self.settings.s3.bucket_name, path_name)
        log_note = ""
        s3_put_result = None
        try:
            s3_put_result = s3_object.put(Body=payload)
        except ClientError as e:
            log_note = f"botocore.exceptions.ClientError: {e}"
        except EndpointConnectionError as e:
            log_note = f"botocore.exceptions.EndpointConnectionError: {e}"
        except Exception as e:  # noqa: BLE001
            log_note = f"unknown error type {e}"

        if (
            s3_put_result is not None
            and s3_put_result.get("ResponseMetadata", {}).get("HTTPStatusCode")
            == HTTPStatus.OK
        ):
            LGMSG.info("Wrote to S3: %s", path_name)
            self.s3_put_works = True
            return True
        LGST.warning(log_note)
        self.s3_put_works = False
        return False

    def update_s3_put_works(self) -> None:
        """Probe: write a tiny self-heartbeat object so a downed store is
        noticed (and `s3_put_works` restored) within a minute."""
        self.put_in_s3(
            file_name=f"{self.alias}-hb-{self.alias}.json",
            payload=b'{"TypeName": "ear.hb"}',
        )

    # ------------------------------------------------------------------
    # Local cache — failed puts park here; the hourly chore retries
    # ------------------------------------------------------------------

    def store_locally(self, file_name: str, payload: bytes) -> None:
        with Path(f"{self.local_cache_dir}/{file_name}").open("wb") as outfile:
            outfile.write(payload)
        LGST.info(f"wrote to {self.local_cache_dir}/{file_name}")

    def try_to_empty_cache(self) -> None:
        for file_name in os.listdir(self.local_cache_dir):
            local_copy = Path(f"{self.local_cache_dir}/{file_name}")
            with local_copy.open("rb") as read_file:
                payload = read_file.read()
            if self.put_in_s3(file_name=file_name, payload=payload):
                local_copy.unlink()
                LGST.info(f"Put cached {file_name} in S3 and deleted locally")

    # ------------------------------------------------------------------
    # Periodic chores — in-process, driven by the CLI loop via
    # periodic_tick() (nothing here is system cron; the chores mutate
    # live actor state and must run inside the process)
    # ------------------------------------------------------------------

    def periodic_tick(self) -> None:
        now = time.time()
        if now >= self._last_minute_s - (self._last_minute_s % 60) + 60:
            if self.use_s3:
                self.update_s3_put_works()
            self._last_minute_s = int(now)
        if now >= self._last_hour_s - (self._last_hour_s % 3600) + 3600:
            if self.messages_heard_this_hour == 0:
                LGST.warning(f"Ear service {self.alias} heard 0 messages last hour")
            self.messages_heard_this_hour = 0
            if self.s3_put_works:
                self.try_to_empty_cache()
            self._last_hour_s = int(now)
        if now >= self._last_day_s - (self._last_day_s % 86400) + 86400:
            self.possibly_update_s3_folder()
            self._last_day_s = int(now)
