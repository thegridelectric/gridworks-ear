import functools
import logging
import os
import threading
import time
import uuid
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path
from typing import no_type_check

import boto3
import pendulum
import xdg
from botocore.exceptions import ClientError, EndpointConnectionError
from gw.enums import MessageCategory
from gw.errors import GwTypeError
from gw.utils import responsive_sleep
from gwbase import ActorBase
from gwbase.actor_base import OnReceiveMessageDiagnostic
from gwbase.enums import UniverseType
from gwbase.types import HeartbeatA
from pydantic import BaseModel
from slack_sdk.webhook import WebhookClient

from gear.config import EarSettings
from gear.utils import BasicLog, EarWarningType, send_warning_to_slack

LOG_FORMAT = (
    "%(levelname) -10s %(asctime)s %(name) -30s %(funcName) "
    "-35s %(lineno) -5d: %(message)s"
)
LOGGER = logging.getLogger(__name__)

LOGGER.setLevel(logging.INFO)

DEV_OUTPUT_ROOT = xdg.xdg_data_home() / "gridworks/ear/output"

MINIMUM_SCADA_REPORT_SECONDS = 10 * 60
THIRTY_MINUTES = 1800


def get_folder_size(bucket, prefix):
    total_size = 0
    for obj in boto3.resource("s3").Bucket(bucket).objects.filter(Prefix=prefix):
        total_size += obj.size
    return total_size


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
    message_times: dict[str, MessageState]
    last_file_name: str
    last_body: bytes
    _messages_heard_this_hour: int = 0
    _messages_heard_total: int = 0
    use_s3: bool = True

    def __init__(self, settings: EarSettings, use_s3: bool = True):
        super().__init__(settings=settings)
        self.hb_int: int = 0
        self.settings: EarSettings = settings
        self._consume_exchange = "ear_tx"
        self.s3_resource = boto3.Session(
            region_name=settings.aws.region_name,
            profile_name=settings.aws.profile_name,
        ).resource("s3")
        self.use_s3 = use_s3
        self.s3_put_works: bool = True

        self.local_cache_dir = DEV_OUTPUT_ROOT / (
            f"need_to_put/{self.settings.world_instance_alias}"
        )
        self.local_cache_dir.mkdir(exist_ok=True, parents=True)

        now = int(time.time())
        self.webhook = WebhookClient(url=self.settings.slack.web_hook_url)
        self._messages_heard_this_hour = 0
        self._messages_heard_total = 0
        self._s3_time_based_subfolder_name = time_based_subfolder_name_from_unix_s(
            int(time.time())
        )
        self._last_min_cron_s = now - (now % 300)
        self._last_hour_cron_s = now - (now % 3600)
        self._last_day_cron_s = now - (now % 86400)
        for file in [
            self.settings.minute_cron_file,
            self.settings.hour_cron_file,
            self.settings.day_cron_file,
        ]:
            if not os.path.exists(file):
                # The file does not exist, so create it
                with open(file, "w") as outfile:
                    outfile.write("")
        os.utime(self.settings.day_cron_file, (time.time(), time.time()))
        os.utime(self.settings.hour_cron_file, (time.time(), time.time()))
        os.utime(self.settings.minute_cron_file, (time.time(), time.time()))
        self.log_csv = f"output/debug_logs/ear_{str(uuid.uuid4()).split('-')[1]}.csv"
        self.main_thread = threading.Thread(target=self.main)
        if self.universe_type == UniverseType.Dev:
            self.flush_local_store()

    @no_type_check
    def on_queue_declareok(self, _unused_frame) -> None:
        """
        OVERWRITE base class method. Binds to everything in ear_tx
        Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.
        :param pika.frame.Method _unused_frame: The Queue.DeclareOk frame
        """

        LOGGER.info(
            "Binding %s to %s with %s",
            self._consume_exchange,
            "ear_tx",
            "#",
        )
        cb = functools.partial(self.on_direct_message_bindok, binding="#")
        self._single_channel.queue_bind(
            self.queue_name,
            "ear_tx",
            routing_key="#",
            callback=cb,
        )

    def local_start(self) -> None:
        """This overwrites local_start in actor_base, used for additional threads.
        It cannot assume the rabbit channels are established and that
        messages can be received or sent."""
        self.main_thread.start()
        self._main_loop_running = True
        print("Just started main thread")

    def local_stop(self) -> None:
        self._main_loop_running = False
        self.main_thread.join()

    @property
    def messages_heard_total(self) -> int:
        return self._messages_heard_total

    ########################
    ## Receives
    ########################

    @no_type_check
    def on_message(self, _unused_channel, basic_deliver, properties, body) -> None:
        """
        Overriding actor_base on_message
        """
        routing_key = basic_deliver.routing_key
        LOGGER.debug(
            f"{self.alias}: Got {basic_deliver.routing_key} with delivery tag {basic_deliver.delivery_tag}"
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
            LOGGER.warning(
                f"IGNORING MESSAGE. {self._latest_on_message_diagnostic}: {e}"
            )
            return

        self._messages_heard_this_hour += 1
        self._messages_heard_total += 1
        try:
            msg_category = self.message_category_from_routing_key(routing_key)
        except GwTypeError:
            return

        if self.settings.logging_on or self.settings.log_message_summary:
            print(f"{pendulum.now('UTC')} MSG :  {from_alias} sent {type_name}")
        kafka_topic = f"{from_alias}-{type_name}"
        if msg_category == MessageCategory.RabbitGwSerial:
            file_name = (
                f"{kafka_topic}-{int(time.time() * 1000)}-{self.settings.my_fqdn}.txt"
            )
        else:
            file_name = (
                f"{kafka_topic}-{int(time.time() * 1000)}-{self.settings.my_fqdn}.json"
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
            int(time.time())
        )
        return old_s3_time_based_subfolder_name != self._s3_time_based_subfolder_name

    @property
    def output_folder_root(self) -> str:
        """The data in S3 is stored in subfolders with a 'hw1__1/eventstore/YYYYMMDD' convention.
        Note that the world instance (hw1__1) is constant for an ear. The time-based subfolder
        is updated in a daily cron job once there is more than 5 MB stored there."""
        return f"{self.settings.world_instance_alias}/eventstore/{self._s3_time_based_subfolder_name}"

    def update_s3_put_works(self):
        self.hb_int = (self.hb_int + 1) % 16
        h = HeartbeatA(my_hex=hex(self.hb_int)[2:])
        kafka_topic = f"{self.alias}-{h.type_name}"
        self.put_in_s3(
            file_name=f"{kafka_topic}-{self.settings.my_fqdn}.json", payload=h.as_type()
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
        # print(
        #     f"self.output_folder_root is {self.output_folder_root} and file_name is {file_name}"
        # )
        s3_object = self.s3_resource.Object(self.settings.aws.bucket_name, path_name)
        s3_put_worked = False
        log_note = ""
        s3_put_result = None
        try:
            s3_put_result = s3_object.put(Body=payload)
        except ClientError as e:
            log_note = f"botocore.exceptions.ClientError: {e}"
        except EndpointConnectionError as e:
            log_note = f"botocore.exceptions.EndpointConnectionError: {e}"
        except Exception as e:
            log_note = f"unknown error type {e}"

        if s3_put_result is not None:
            if "ResponseMetadata" not in s3_put_result.keys():
                log_note = "some uncaught error"
                # we could set this to raise an exception in dev setting only
            elif "HTTPStatusCode" not in s3_put_result["ResponseMetadata"].keys():
                log_note = "some uncaught error"
            elif (
                not s3_put_result["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK
            ):
                log_note = f"HttpStatusCode {s3_put_result['ResponseMetadata']['HTTPStatusCode']} "
            else:
                s3_put_worked = True

        if s3_put_worked:
            print(f"Wrote to S3: {path_name}")
            self.s3_put_works = True
            return True
        else:
            print(BasicLog.format("INFO", log_note))
            self.s3_put_works = False
            return False

    #################
    # Local caching
    #################

    def flush_local_store(self):
        for subdir, _, files in os.walk(self.local_cache_dir):
            for file in files:
                filepath = subdir + os.sep + file
                if filepath.endswith(".json"):
                    os.system(f"rm {filepath}")
                if filepath.endswith(".txt"):
                    os.system(f"rm {filepath}")
        BasicLog.format("DEBUG", f"flushed all old data from {self.local_cache_dir}")

    def store_locally(self, file_name: str, payload: bytes):
        """Store message in folder output/need_to_put/world_instance_alias. Flush
        that directory if world_type is dev"""
        with open(f"{self.local_cache_dir}/{file_name}", "wb") as outfile:
            outfile.write(payload)
        print(BasicLog.format("DEBUG", f"wrote to {self.local_cache_dir}/{file_name}"))

    def try_to_empty_cache(self):
        """For each file in the relevant need_to_put subfolder,
        try to put it in s3 and if successful, delete from subfolder

        """
        file_list = os.listdir(self.local_cache_dir)
        for file_name in file_list:
            with open(f"{self.local_cache_dir}/{file_name}", "rb") as read_file:
                payload = read_file.read()
                if self.put_in_s3(file_name=file_name, payload=payload):
                    os.remove(f"{self.local_cache_dir}/{file_name}")
                    print(
                        BasicLog.format(
                            "INFO", f"Put cached {file_name} in S3 and deleted locally"
                        )
                    )

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
        if time.time() > self.next_min_cron_s:
            return True
        return False

    def time_for_hour_cron(self) -> bool:
        if time.time() > self.next_hour_cron_s:
            return True
        return False

    def time_for_day_cron(self) -> bool:
        if time.time() > self.next_day_cron_s:
            return True
        return False

    def cron_every_min_success(self):
        self._last_min_cron_s = int(time.time())
        os.utime(self.settings.minute_cron_file, (time.time(), time.time()))

    def cron_every_hour_success(self):
        print(BasicLog.format("INFO", "Ran cron every hour"))
        self._last_hour_cron_s = int(time.time())
        os.utime(self.settings.hour_cron_file, (time.time(), time.time()))

    def cron_every_day_success(self):
        self._last_day_cron_s = int(time.time())
        print(BasicLog.format("INFO", "Ran cron every day"))
        os.utime(self.settings.day_cron_file, (time.time(), time.time()))

    def cron_every_min(self):
        if self.use_s3:
            self.update_s3_put_works()
        self.cron_every_min_success()

    def cron_every_hour(self):
        if self._messages_heard_this_hour == 0:
            if (
                time.time() - os.path.getmtime(self.settings.hour_cron_file)
            ) > THIRTY_MINUTES:
                warning_message = (
                    f"Ear service {self.settings.my_fqdn} heard 0 messages last hour"
                )
                print(BasicLog.format("WARNING", warning_message))
                send_warning_to_slack(
                    webhook=self.webhook,
                    warning_type=EarWarningType.EAR_HEARD_NO_MESSAGES_FOR_AN_HOUR,
                    warning_message=warning_message,
                )
        self._messages_heard_this_hour = 0
        if self.s3_put_works:
            self.try_to_empty_cache()
            self.cron_every_hour_success()

    def cron_every_day(self):
        self.possibly_update_s3_folder()
        self.cron_every_day_success()

    def main(self):
        while self._main_loop_running:
            if self.time_for_min_cron():
                self.cron_every_min()
            if self.time_for_hour_cron():
                self.cron_every_hour()
            if self.time_for_day_cron():
                self.cron_every_day()

            responsive_sleep(self, 10)
