import json
import os
import time
from typing import NamedTuple

import boto3
import botocore
import pendulum
from slack_sdk.webhook import WebhookClient

from gear.config import EarSettings
from ear_base import EarBase
from utils import (
    BasicLog,
    EarWarningType,
    MessageFormatType,
    WorldType,
    responsive_sleep,
    send_warning_to_slack,
)

DEV_OUTPUT_ROOT = "output/"


def get_folder_size(bucket, prefix):
    total_size = 0
    for obj in boto3.resource("s3").Bucket(bucket).objects.filter(Prefix=prefix):
        total_size += obj.size
    return total_size


class MessagePlus(NamedTuple):
    KafkaTopic: str
    TimeReceivedUnixMs: int
    LogNote: str
    BodyBytes: bytes


class Ear(EarBase):
    NEW_FOLDER_TRIGGER_MEGA_BYTES = 5

    def __init__(self, settings: EarSettings):
        super(Ear, self).__init__(settings=settings)
        self.settings=settings
        self.s3_resource = boto3.resource("s3")
        self.s3_put_works: bool = False

        self.local_cache_dir = f"output/need_to_put/{self.settings.g_node_data.WorldInstanceAlias}"
        if not os.path.exists(self.local_cache_dir):
            os.makedirs(self.local_cache_dir)

        now = int(time.time())
        self.webhook = WebhookClient(url=self.settings.slack.web_hook_url)
        self._messages_heard_this_hour = 0
        self._s3_time_based_subfolder_name = self.time_based_subfolder_name_from_unix_s(time.time())
        self._last_min_cron_s = now - (now % 300)
        self._last_hour_cron_s = now - (now % 3600)
        self._last_day_cron_s = now - (now % 86400)
        os.utime(self.settings.day_cron_file,  (time.time(), time.time()))
        os.utime(self.settings.hour_cron_file, (time.time(), time.time()))
        os.utime(self.settings.minute_cron_file, (time.time(), time.time()))

        print(BasicLog.format("INFO", f"Initialized {self.__class__}"))

    #######################
    # Various GNode-related
    #######################

    @property
    def world_type(self) -> WorldType:
        """Returns the WorldType: Dev, Shadow or Real, depending on the
        world root alias (found in settings).

        Dev worlds have root aliases that start with d. They are intended
        to have world instances that run locally in development environments.
        In particular, the same world instance can be created multiple times. Output
        data from dev world instances is not intended for permanent storage.

        Shadow worlds have root aliases that start with 's'. They are intended to
        be simulations shared between multiple entities. Output data is intended
        to be stored. A single shadow world instance is only supposed to run
        once - that is, there should be at most one instance of each time for
        each shadow world instance.

        There is only supposed to be one real world, and its root alias is
        'w'.
        """

        root_alias = self.settings.g_node_data.WorldInstanceAlias.split(".")[0]
        if root_alias == "w":
            return WorldType.REAL
        if root_alias.startswith("dw"):
            return WorldType.DEV
        if root_alias.startswith("hw"):
            return WorldType.HYBRID
        if root_alias.startswith("sw"):
            return WorldType.SHADOW
        raise Exception(
            "World Root must be 'w' or start with dw (dev), hw(hybrid) or sw(for shadow)"
        )

    def on_message(self, kafka_topic: str, payload: bytes, message_format_type: MessageFormatType):
        self._messages_heard_this_hour += 1
        self.kafka_topic = kafka_topic
        self.payload = payload
        if message_format_type == MessageFormatType.GW_SERIAL:
            file_name = f"{kafka_topic}-{int(time.time() * 1000)}-{self.settings.my_fqdn}.txt"
        else:
            file_name = f"{kafka_topic}-{int(time.time() * 1000)}-{self.settings.my_fqdn}.json"

        if self.s3_put_works:
            success_putting_this_one = self.put_in_s3(file_name, payload)
        else:
            success_putting_this_one = False

        if not success_putting_this_one:
            self.store_locally(file_name, payload)

    ######################
    # S3 related
    #######################

    def possibly_update_s3_folder(self) -> bool:
        """Checks if the curent output folder has more than 5 MB in it. If yes, it
        sets the time based subfolder name based on the time right now.

        Returns:
            bool: True if a new folder has been created, false otherwise.
        """
        size_bytes = get_folder_size(self.settings.aws.bucket_name, self.output_folder_root)
        if size_bytes > self.NEW_FOLDER_TRIGGER_MEGA_BYTES * 10**6:
            candidate_new = self.time_based_subfolder_name_from_unix_s(time.time())
            if candidate_new != self._s3_time_based_subfolder_name:
                self._s3_time_based_subfolder_name = candidate_new
                print(f"output_folder_root is now {self.output_folder_root}")
                return True
            return False
        return False

    @property
    def output_folder_root(self) -> str:
        """The data in S3 is stored in subfolders with a 'hw1__1/eventstore/YYYYMMDD' convention.
        Note that the world instance (hw1__1) is constant for an ear. The time-based subfolder
        is updated in a daily cron job once there is more than 5 MB stored there."""
        return f"{self.settings.g_node_data.WorldInstanceAlias}/eventstore/{self._s3_time_based_subfolder_name}"

    def time_based_subfolder_name_from_unix_s(self, time_unix_s: int) -> str:
        return pendulum.from_timestamp(time_unix_s).strftime("%Y%m%d")

    def update_s3_put_works(self):
        payload = json.dumps(
            f'"EarDns": "{self.settings.my_fqdn}","UnixTimeMs": {int(time.time()) * 1000}'
        )
        world_alias = self.settings.g_node_data.WorldInstanceAlias.split("__")[0]
        self.put_in_s3(file_name=f"{world_alias}-heartbeat.a-0-{self.settings.my_fqdn}.txt", payload=payload)

    def put_in_s3(self, file_name: str, payload: str) -> bool:
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
        print(f"self.output_folder_root is {self.output_folder_root} and file_name is {file_name}")
        s3_object = self.s3_resource.Object(self.settings.aws.bucket_name, path_name)
        s3_put_worked = False
        log_note = ""
        s3_put_result = None
        try:
            s3_put_result = s3_object.put(Body=payload)
        except botocore.exceptions.ClientError as e:
            log_note = f"botocore.exceptions.ClientError: {e}"
        except botocore.exceptions.EndpointConnectionError as e:
            log_note = f"botocore.exceptions.EndpointConnectionError: {e}"
        except Exception as e:
            log_note = f"unknown error type {e}"

        if s3_put_result is not None:
            if "ResponseMetadata" not in s3_put_result.keys():
                log_note = "some uncaught error"
                # we could set this to raise an exception in dev setting only
            else:
                if "HTTPStatusCode" not in s3_put_result["ResponseMetadata"].keys():
                    log_note = "some uncaught error"
                else:
                    if not s3_put_result["ResponseMetadata"]["HTTPStatusCode"] == 200:
                        log_note = (
                            f"HttpStatusCode {s3_put_result['ResponseMetadata']['HTTPStatusCode']} "
                        )
                    else:
                        s3_put_worked = True

        if s3_put_worked:
            # print(BasicLog.format("DEBUG", f"S3 put of {path_name} worked"))
            self.s3_put_works = True
            return True
        else:
            print(BasicLog.format("INFO", log_note))
            self.s3_put_works = False
            return False

    #################
    # Local caching
    #################

    def store_locally(self, file_name: str, payload: bytes):
        """Store message in folder output/need_to_put/world_intance_alias. Flush
        that directory if world_type is dev"""
        if self.world_type == WorldType.DEV:
            print(
                BasicLog.format(
                    "DEBUG", f"dev world, so flushing all old data from {self.local_cache_dir}"
                )
            )
            for subdir, dirs, files in os.walk(self.local_cache_dir):
                for file in files:
                    filepath = subdir + os.sep + file
                    if filepath.endswith(".json"):
                        os.system(f"rm {filepath}")
                    if filepath.endswith(".txt"):
                        os.system(f"rm {filepath}")

        with open(f"{self.local_cache_dir}/{file_name}", "wb") as outfile:
            outfile.write(payload)
        print(BasicLog.format("DEBUG", f"wrote to {self.local_cache_dir}/{file_name}"))

    def try_to_empty_cache(self) -> "bool":
        """For each file in the relevant need_to_put subfolder,
        try to put it in s3 and if successful, delete from subfolder

        Returns:
            bool:True if subfolder is empty at the end
        """
        file_list = os.listdir(self.local_cache_dir)
        for file_name in file_list:
            with open(f"{self.local_cache_dir}/{file_name}", "rb") as read_file:
                payload = read_file.read()
                if self.put_in_s3(file_name=file_name, payload=payload):
                    os.remove(f"{self.local_cache_dir}/{file_name}")
                    print(
                        BasicLog.format("INFO", f"Put cached {file_name} in S3 and deleted locally")
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
        self.update_s3_put_works()
        self.cron_every_min_success()

    def cron_every_hour(self):
        if self._messages_heard_this_hour == 0:
            if (time.time() - os.path.getmtime(self.settings.hour_cron_file)) > 1800:
                warning_message = f"Ear service {self.settings.my_fqdn} heard 0 messages last hour"
                print(BasicLog.format("WARNING", warning_message))
                response_status_code = send_warning_to_slack(
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
        self._main_loop_running = True
        while self._main_loop_running is True:
            if self.time_for_min_cron():
                self.cron_every_min()
            if self.time_for_hour_cron():
                self.cron_every_hour()
            if self.time_for_day_cron():
                self.cron_every_day()

            responsive_sleep(self, 1)
