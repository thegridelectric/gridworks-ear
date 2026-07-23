from __future__ import annotations

import contextlib
import functools
import logging
import os
import threading
import time
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path

import boto3
import pendulum
from botocore.exceptions import ClientError, EndpointConnectionError
from gw.enums import MessageCategory
from gw.errors import GwTypeError
from gw.utils import responsive_sleep
from gwbase import ActorBase
from gwbase.actor_base import OnReceiveMessageDiagnostic
from gwbase.enums import UniverseType
from gwbase.named_types import HeartbeatA
from mypy_boto3_s3.service_resource import S3ServiceResource
from pika.channel import Channel as PikaChannel
from pika.frame import Method as FrameMethod
from pika.spec import Basic as PikaBasic
from pika.spec import BasicProperties as PikaBasicProperties
from pydantic import BaseModel
from slack_sdk.webhook import WebhookClient

from gear.config import EarSettings
from gear.utils import EAR_LOGGER as LG
from gear.utils import EAR_MESSAGE_LOGGER as LGMSG
from gear.utils import EAR_STATE_LOGGER as LGST
from gear.utils import (
    OUTPUT_DIRECTORY,
    STATE_DIRECTORY,
    EarWarningType,
    send_warning_to_slack,
)

MINIMUM_SCADA_REPORT_SECONDS = 10 * 60
THIRTY_MINUTES = 1800


class MessagePlus(BaseModel):
    KafkaTopic: str
    TimeReceivedUnixMs: int
    LogNote: str
    BodyBytes: bytes


@dataclass
class MessageState:
    message_time: pendulum.datetime
    reported_state: bool


def time_based_subfolder_name_from_unix_s(time_unix_s: int) -> str:
    return pendulum.from_timestamp(time_unix_s).strftime("%Y%m%d")


class Ear(ActorBase):
    cron_last_min_file: Path
    cron_last_hour_file: Path
    cron_last_day_file: Path
    last_file_name: str
    last_body: bytes
    messages_heard_this_hour: int = 0
    messages_heard_total: int = 0
    use_s3: bool = True
    s3_put_works: bool = True
    s3_resource: S3ServiceResource | None = None

    def __init__(self, settings: EarSettings, *, use_s3: bool = True) -> None:
        super().__init__(settings=settings)
        self.hb_int: int = 0
        self.settings: EarSettings = settings
        self._consume_exchange = settings.consume_exchange
        self.use_s3 = use_s3
        self.s3_put_works: bool = self.use_s3
        self.local_cache_dir = OUTPUT_DIRECTORY / (
            f"need_to_put/{self.settings.world_instance_alias}"
        )
        self.local_cache_dir.mkdir(exist_ok=True, parents=True)
        now = int(time.time())
        self.webhook = WebhookClient(url=self.settings.slack.web_hook_url)
        self.messages_heard_this_hour = 0
        self.messages_heard_total = 0
        self._s3_time_based_subfolder_name = time_based_subfolder_name_from_unix_s(
            int(time.time()),
        )
        self._last_min_cron_s = now - (now % 300)
        self._last_hour_cron_s = now - (now % 3600)
        self._last_day_cron_s = now - (now % 86400)
        self.cron_last_min_file = STATE_DIRECTORY / self.settings.minute_cron_file
        self.cron_last_hour_file = STATE_DIRECTORY / self.settings.hour_cron_file
        self.cron_last_day_file = STATE_DIRECTORY / self.settings.day_cron_file
        STATE_DIRECTORY.mkdir(exist_ok=True, parents=True)
        self.cron_last_min_file.touch()
        self.cron_last_hour_file.touch()
        self.cron_last_day_file.touch()
        self.main_thread = threading.Thread(target=self.main)
        self.main_thread.deamon = True
        if self.universe_type == UniverseType.Dev:
            self.flush_local_store()

    def on_queue_declareok(self, _unused_frame: FrameMethod) -> None:
        """
        OVERWRITE base class method. Binds to everything (`#`) in the
        configured consume exchange (default `ear_tx`; a scoped tap like
        `gnr_ear_tx` for a second, seed-store instance).
        Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.
        :param pika.frame.Method _unused_frame: The Queue.DeclareOk frame
        """

        LGST.info(
            "Binding %s to %s with %s",
            self.queue_name,
            self._consume_exchange,
            "#",
        )
        cb = functools.partial(self.on_direct_message_bindok, binding="#")
        self._single_channel.queue_bind(
            self.queue_name,
            self._consume_exchange,
            routing_key="#",
            callback=cb,
        )

    def local_start(self) -> None:
        """This overwrites local_start in actor_base, used for additional threads.
        It cannot assume the rabbit channels are established and that
        messages can be received or sent."""

        # _main_loop_running MUST be true prior to starting the main thread and
        # prior to this function exiting.
        # That way, a caller to ear.start() can reliably assume that if
        # _main_loop_running is NOT true, the main thread has exited.
        self._main_loop_running = True
        self.main_thread.start()

    def local_stop(self) -> None:
        self._main_loop_running = False
        self.main_thread.join()

    ########################
    # Receives
    ########################

    def on_message(
        self,
        _unused_channel: PikaChannel,
        basic_deliver: PikaBasic.Deliver,
        _unused_properties: PikaBasicProperties,
        body: bytes,
    ) -> None:
        """
        Overriding actor_base on_message
        """
        routing_key = basic_deliver.routing_key
        LG.debug(
            f"{self.alias}: Got {basic_deliver.routing_key} with delivery tag {basic_deliver.delivery_tag}",
        )
        self.acknowledge_message(basic_deliver.delivery_tag)

        try:
            type_name = self.get_payload_type_name(basic_deliver)
        except GwTypeError:
            return
        try:
            from_alias = self.from_alias_from_routing_key(routing_key)
        except GwTypeError as e:
            self._latest_on_message_diagnostic = (
                OnReceiveMessageDiagnostic.FROM_GNODE_DECODING_PROBLEM
            )
            s = f"IGNORING MESSAGE. {self._latest_on_message_diagnostic}: {e}"
            LGST.warning(s)
            LG.warning(s)
            return

        self.messages_heard_this_hour += 1
        self.messages_heard_total += 1
        try:
            msg_category = self.message_category_from_routing_key(routing_key)
        except GwTypeError:
            return

        from_alias_and_type = f"{from_alias}-{type_name}"
        if msg_category == MessageCategory.RabbitGwSerial:
            file_name = (
                f"{from_alias_and_type}-{int(time.time() * 1000)}-{self.alias}.txt"
            )
        else:
            file_name = (
                f"{from_alias_and_type}-{int(time.time() * 1000)}-{self.alias}.json"
            )

        if self.use_s3 and self.s3_put_works:
            success_putting_this_one = self.put_in_s3(file_name, body)
        else:
            success_putting_this_one = False
        self.last_file_name = file_name
        self.last_body = body
        if not success_putting_this_one:
            self.store_locally(file_name, body)

    ######################
    # S3 related
    #######################

    def possibly_update_s3_folder(self) -> bool:
        """Checks if current time is in a new day UTC

        Returns:
            bool: True if current time is a new day UTC
        """
        old_s3_time_based_subfolder_name = self._s3_time_based_subfolder_name
        self._s3_time_based_subfolder_name = time_based_subfolder_name_from_unix_s(
            int(time.time()),
        )
        changed = old_s3_time_based_subfolder_name != self._s3_time_based_subfolder_name
        s = f"S3 folder updated: {changed!s:5s}  {old_s3_time_based_subfolder_name}"
        if changed:
            s += f" -> {self._s3_time_based_subfolder_name}"
        LGST.info(s)
        return changed

    @property
    def output_folder_root(self) -> str:
        """The data in S3 is stored in subfolders with a 'hw1__1/eventstore/YYYYMMDD' convention.
        Note that the world instance (hw1__1) is constant for an ear. The time-based subfolder
        is updated in a daily cron job once there is more than 5 MB stored there."""
        return f"{self.settings.world_instance_alias}/eventstore/{self._s3_time_based_subfolder_name}"

    def update_s3_put_works(self) -> None:
        self.hb_int = (self.hb_int + 1) % 16
        h = HeartbeatA(my_hex=f"{self.hb_int:x}")
        from_alias_and_type = f"{self.alias}-{h.type_name}"
        self.put_in_s3(
            file_name=f"{from_alias_and_type}-{self.alias}.json",
            payload=h.as_type(),
        )

    def put_in_s3(self, file_name: str, payload: bytes) -> bool:
        """The core function of this repo: take messages that the ear hears and
        put them in S3. As a caveat, this function is MOCKED OUT in development
        to store locally instead.

        Args:
            file_name (str): the name for the file.
            payload: the content to be stored in the file

        Returns:
            True if the payload is loaded to S3 at the file_name, else False
        """

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
        s3_put_worked = False
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

        if s3_put_result is not None:
            if (
                "ResponseMetadata" not in s3_put_result
                or "HTTPStatusCode" not in s3_put_result["ResponseMetadata"]
            ):
                log_note = "some uncaught error"
            elif s3_put_result["ResponseMetadata"]["HTTPStatusCode"] != HTTPStatus.OK:
                log_note = f"HttpStatusCode {s3_put_result['ResponseMetadata']['HTTPStatusCode']} "
            else:
                s3_put_worked = True

        if s3_put_worked:
            LGMSG.info("Wrote to S3: %s", path_name)
            self.s3_put_works = True
            return True
        LGST(log_note)
        LG.warning(log_note)
        self.s3_put_works = False
        return False

    #################
    # Local caching
    #################

    def flush_local_store(self) -> None:
        for subdir, _, files in os.walk(self.local_cache_dir):
            for file in files:
                filepath = subdir + os.sep + file
                if filepath.endswith(".json"):
                    os.system(f"rm {filepath}")  # noqa: S605
                if filepath.endswith(".txt"):
                    os.system(f"rm {filepath}")  # noqa: S605
        LGST.debug(f"flushed all old data from {self.local_cache_dir}")

    def store_locally(self, file_name: str, payload: bytes) -> None:
        """Store message in folder output/need_to_put/world_instance_alias. Flush
        that directory if world_type is dev"""
        with Path(f"{self.local_cache_dir}/{file_name}").open("wb") as outfile:
            outfile.write(payload)
        LGST.info(f"wrote to {self.local_cache_dir}/{file_name}")

    def try_to_empty_cache(self) -> None:
        """For each file in the relevant need_to_put subfolder,
        try to put it in s3 and if successful, delete from subfolder

        """
        file_list = os.listdir(self.local_cache_dir)
        for file_name in file_list:
            local_copy = Path(f"{self.local_cache_dir}/{file_name}")
            with local_copy.open("rb") as read_file:
                payload = read_file.read()
            if self.put_in_s3(file_name=file_name, payload=payload):
                local_copy.unlink()
                LGST.info(f"Put cached {file_name} in S3 and deleted locally")

    ####################
    # Timing and scheduling related
    ####################

    @property
    def next_min_cron_s(self) -> int:
        last_cron_s = self._last_min_cron_s - (self._last_min_cron_s % 60)
        return last_cron_s + 60

    @property
    def next_hour_cron_s(self) -> int:
        last_cron_s = self._last_hour_cron_s - (self._last_hour_cron_s % 3600)
        return last_cron_s + 3600

    @property
    def next_day_cron_s(self) -> int:
        last_day_s = self._last_day_cron_s - (self._last_day_cron_s % 86400)
        return last_day_s + 86400

    def time_for_min_cron(self) -> bool:
        return time.time() > self.next_min_cron_s

    def time_for_hour_cron(self) -> bool:
        return time.time() > self.next_hour_cron_s

    def time_for_day_cron(self) -> bool:
        return time.time() > self.next_day_cron_s

    def cron_every_min_success(self) -> None:
        self._last_min_cron_s = int(time.time())
        self.cron_last_min_file.touch()

    def cron_every_hour_success(self) -> None:
        LGST.info("Ran cron every hour")
        self._last_hour_cron_s = int(time.time())
        self.cron_last_hour_file.touch()

    def cron_every_day_success(self) -> None:
        self._last_day_cron_s = int(time.time())
        LGST.info("Ran cron every day")
        self.cron_last_day_file.touch()

    def cron_every_min(self) -> None:
        if self.use_s3:
            self.update_s3_put_works()
        self.cron_every_min_success()

    def cron_every_hour(self) -> None:
        LGST.info("++cron_every_hour")
        path_dbg = 0
        if (
            self.messages_heard_this_hour == 0
            and (time.time() - self.cron_last_hour_file.stat().st_mtime)
            > THIRTY_MINUTES
        ):
            path_dbg |= 0x00000001
            warning_message = f"Ear service {self.alias} heard 0 messages last hour"
            LGST.warning(warning_message)
            LG.warning(warning_message)
            send_warning_to_slack(
                webhook=self.webhook,
                warning_type=EarWarningType.EAR_HEARD_NO_MESSAGES_FOR_AN_HOUR,
                warning_message=warning_message,
            )
        self.messages_heard_this_hour = 0
        if self.s3_put_works:
            path_dbg |= 0x00000002
            self.try_to_empty_cache()
            self.cron_every_hour_success()
        LGST.info(f"--cron_every_hour  path:0x{path_dbg:08X}")

    def cron_every_day(self) -> None:
        LGST.info("++cron_every_day")
        self.possibly_update_s3_folder()
        self.cron_every_day_success()
        LGST.info("--cron_every_day")

    def main(self) -> None:
        LGST.info("++ear.main")
        path_dbg = 0
        count_dbg = 0
        try:
            while self._main_loop_running:
                LGST.info(f"++ear.main.itr:{count_dbg:3d}")
                loop_path_dbg = 0
                if self.time_for_hour_cron():
                    loop_path_dbg |= 0x00000001
                    self.cron_every_hour()
                if self.time_for_day_cron():
                    loop_path_dbg |= 0x00000002
                    self.cron_every_day()
                sleep_seconds = min(max(self.next_hour_cron_s - time.time(), 0), 5 * 60)
                self.log_times()
                LGST.info(
                    f"--ear.main.itr:{count_dbg:3d}  sleep_seconds:{sleep_seconds}  path:0x{loop_path_dbg:08X}"
                )
                count_dbg += 1
                responsive_sleep(self, seconds=sleep_seconds)
                path_dbg |= loop_path_dbg
        except Exception as e:  # noqa: BLE001
            path_dbg |= 0x00000100
            s = "ERROR. ear main() exited with exception "
            with contextlib.suppress(Exception):
                s += f"{type(e).__name__}: {e}"
            LGST.exception(s)
        finally:
            self._main_loop_running = False
        LGST.info(f"--ear.main  itr:{count_dbg:3d}  path:0x{path_dbg:08X}")

    def log_times(self) -> None:
        if LGST.isEnabledFor(logging.INFO):
            LGST.info("Ear cron times")
            now_utc = int(time.time())
            for tag, utc_timestamp, overdue in [
                ("now", now_utc, True),
                ("hour", self.next_hour_cron_s, self.time_for_hour_cron()),
                ("day", self.next_day_cron_s, self.time_for_day_cron()),
            ]:
                tag_str = f"{tag:4s}"
                time_str = (
                    f"{tag_str}  passed: {overdue!s:5s}  "
                    f"utc: {pendulum.from_timestamp(utc_timestamp, 'UTC').isoformat()}  "
                    f"local: {pendulum.from_timestamp(utc_timestamp, 'local').isoformat()}"
                )
                LGST.info(time_str)
