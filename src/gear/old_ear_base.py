import csv
import json
import logging
import os
import threading
import uuid
from abc import ABC, abstractmethod
from typing import List

from schemata.errors import SchemaError
import paho.mqtt.client as mqtt

from config import EarSettings
from utils import QOS, BasicLog, MessageFormatType, MessageSummary, Subscription, mqtt_topic_decode, short_log_time, mqtt_topic_encode

from schemata.ear_g_node_alias_list_toss_maker import (
    EarGNodeAliasListToss,
    EarGNodeAliasListToss_Maker,
)

class EarBase(ABC):
    SCADA_SERIAL_MESSAGE_TYPE_ALIAS_LIST = ["gs.pwr.100"]
    SCADA_JSON_MESSAGE_TYPE_ALIAS_LIST = [
        "snapshot.spaceheat.100",
        "gs.pwr.100",
        "gt.sh.status.110"
    ]
    ATN_SERIAL_MESSAGE_TYPE_ALIAS_LIST = ["gs.pwr.100"]
    ATN_JSON_MESSAGE_TYPE_ALIAS_LIST = ["gt.dispatch.boolean.100", "gt.sh.cli.atn.cmd.110", "gt.telemetry.110"]
    LOCAL_SENSOR_TYPE_ALIAS_LIST = ["gt.telemetry.110"]

    def __init__(self, settings: EarSettings):
        self._main_loop_running = False
        self.main_thread = None
        self.settings = settings
        try:
            self.settings.g_node_data = EarGNodeAliasListToss_Maker.type_to_tuple(self.settings.g_node_data_type)
        except SchemaError as e:
            raise Exception(f"Error in settings. check .env: {e}")
        self.log_csv = f"output/debug_logs/earbase_{str(uuid.uuid4()).split('-')[1]}.csv"
        self.client_id = "-".join(str(uuid.uuid4()).split("-")[:-1])
        self.client = mqtt.Client(self.client_id)
        self.client.username_pw_set(
            username=settings.gridworks_mqtt.username,
            password=settings.gridworks_mqtt.password.get_secret_value(),
        )
        self.client.on_message = self.on_mqtt_message
        self.client.on_connect = self.on_connect
        self.client.on_connect_fail = self.on_connect_fail
        self.client.on_disconnect = self.on_disconnect
        if self.settings.logging_on:
            self.client.on_log = self.on_log
        if self.settings.mqtt_client_logging_on:
            self.client.enable_logger(logger=logging.getLogger(settings.mqtt_client_logger_name))

    ##########################
    # What GNodes does the ear track
    ###########################

    def my_g_node_alias_list(self) -> List[str]:
        return self.my_atn_alias_list() + self.my_scada_alias_list()

    def my_atn_alias_list(self) -> List[str]:
        return self.settings.g_node_data.AtomicTNodeAliasList

    def my_scada_alias_list(self) -> List[str]:
        return self.settings.g_node_data.ScadaAliasList

    def my_local_sensor_hack_list(self) -> List[str]:
        return ["a.garage.temp1", "a.tank.out.temp1", "a.tank.in.temp1", "a.tank.temp0"]

    def recognized_topics(self) -> List[str]:
        topics = []
        for g_node_alias in self.my_atn_alias_list():
            for message_type_alias in (
                self.ATN_JSON_MESSAGE_TYPE_ALIAS_LIST + self.ATN_SERIAL_MESSAGE_TYPE_ALIAS_LIST
            ):
                topics.append(f"gw/{g_node_alias}/{message_type_alias}")
        for g_node_alias in self.my_scada_alias_list():
            for message_type_alias in (
                self.SCADA_JSON_MESSAGE_TYPE_ALIAS_LIST + self.SCADA_SERIAL_MESSAGE_TYPE_ALIAS_LIST
            ):
                topics.append(f"gw/{g_node_alias}/{message_type_alias}")
        for alias in self.my_local_sensor_hack_list():
            for message_type_alias in (
                self.LOCAL_SENSOR_TYPE_ALIAS_LIST
            ):
                topics.append(f"{alias}/{message_type_alias}")
        return topics

    def subscriptions(self) -> List[Subscription]:
        subscriptions = []
        for topic in self.recognized_topics():
            subscriptions += [
                Subscription(
                    Topic=f"{mqtt_topic_encode(topic)}",
                    Qos=QOS.AtLeastOnce,
                ),
            ]
        return subscriptions

    def subscribe(self):
        subscriptions = list(map(lambda x: (f"{x.Topic}", x.Qos.value), self.subscriptions()))
        if self.settings.log_message_summary:
            print(BasicLog.format("DEBUG", f"Subscribing to:"))
            for i, (topic, _) in enumerate(subscriptions):
                print(BasicLog.format("DEBUG", f"  {topic}"))
        self.client.subscribe(subscriptions)

    def mqtt_log_hack(self, row):
        if self.settings.logging_on:
            with open(self.log_csv, "a") as outfile:
                write = csv.writer(outfile, delimiter=",")
                write.writerow(row)

    # noinspection PyUnusedLocal
    def on_log(self, client, userdata, level, buf):
        self.mqtt_log_hack([f"({short_log_time()}) log: {buf}"])

    # noinspection PyUnusedLocal
    def on_connect(self, client, userdata, flags, rc):
        self.mqtt_log_hack(
            [
                f"({short_log_time()}) Mqtt client Connected flags {str(flags)} + result code {str(rc)}"
            ]
        )
        self.subscribe()

    # noinspection PyUnusedLocal
    def on_connect_fail(self, client, userdata):
        self.mqtt_log_hack([f"({short_log_time()}) Mqtt client Connect fail"])

    # noinspection PyUnusedLocal
    def on_disconnect(self, client, userdata, rc):
        self.mqtt_log_hack(
            [f"({short_log_time()}) Mqtt client disconnected! result code {str(rc)}"]
        )

    # noinspection PyUnusedLocal
    def on_mqtt_message(self, client, userdata, message):
        """Converts the

        What this does NOT do is evaluate whether the message is of the correct
        format, comes from a known GNodeInstance, etc
        """
        try:
            topic = mqtt_topic_decode(message.topic)
            words = topic.split("/")
        except IndexError:
            raise Exception("malformed topic. This should not have been subscribed to!")
        if words[0] == 'gw':
            from_alias = words[1]
            type_alias = words[2]
        else:
            from_alias = words[0]
            type_alias = words[1]
        if (
            type_alias
            in self.ATN_SERIAL_MESSAGE_TYPE_ALIAS_LIST + self.SCADA_SERIAL_MESSAGE_TYPE_ALIAS_LIST
        ):
            message_format_type = MessageFormatType.GW_SERIAL
        else:
            message_format_type = MessageFormatType.JSON
        if self.settings.logging_on or self.settings.log_message_summary:
            print(MessageSummary.format("IN", "ear", message.topic, message.payload))

        self.on_message(
            kafka_topic=f"{from_alias}-{type_alias}",
            payload=message.payload,
            message_format_type=message_format_type,
        )

    @abstractmethod
    def on_message(self, kafka_topic: str, payload: bytes, message_format_type: MessageFormatType):
        raise NotImplementedError

    def terminate_main_loop(self):
        self._main_loop_running = False

    @abstractmethod
    def main(self):
        raise NotImplementedError

    def start(self):
        print(
            BasicLog.format(
                "INFO",
                (
                    f"Connecting MQTT client to [{self.settings.gridworks_mqtt.host}] / "
                    f"{self.settings.gridworks_mqtt.port}"
                )
            )
        )
        self.client.connect(
            self.settings.gridworks_mqtt.host, port=self.settings.gridworks_mqtt.port
        )
        self.client.loop_start()
        self.main_thread = threading.Thread(target=self.main)
        self.main_thread.start()
        print(BasicLog.format("INFO", f"Started {self.__class__}"))

    def stop(self):
        print(BasicLog.format("INFO", "Stopping ..."))
        self.terminate_main_loop()
        self.client.disconnect()
        self.client.loop_stop()
        self.main_thread.join()
        print(BasicLog.format("INFO", "Stopped"))
