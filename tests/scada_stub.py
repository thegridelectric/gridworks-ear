import json
import uuid

import pika
from gwbase import ActorBase
from gwbase.config import GNodeSettings
from gwbase.enums import MessageCategory

STATUS_ROUTING_KEY = (
    "gw.d1-isone-me-versant-keene-beech-scada.gridworks-event-gt-sh-status"
)


class DummyScada(ActorBase):
    actor_main_stopped: bool = False

    def __init__(self, settings: GNodeSettings):
        settings.g_node_alias = "d1.isone.me.versant.keene.beech.scada"
        settings.g_node_role_value = "Scada"
        self.folder_base = "tests/sample_scada_messages/"
        super().__init__(settings=settings)

    def prepare_for_death(self) -> None:
        self.actor_main_stopped = True

    def send_status(self) -> None:
        """
        Sends an old sample status message
        """
        STATUS_FILE = "d1.isone.me.versant.keene.beech.scada-gridworks.event.gt.sh.status-1715905380350-100.26.91.172.json"
        type_name = "gridworks.event.gt.sh.status"
        routing_key = (
            "gw." + self.alias.replace(".", "-") + "." + type_name.replace(".", "-")
        )

        assert STATUS_FILE.split("-")[0] == self.alias
        assert STATUS_FILE.split("-")[1] == type_name
        assert routing_key == STATUS_ROUTING_KEY

        with open(self.folder_base + STATUS_FILE) as f:
            payload_dict = json.load(f)
            payload_bytes = json.dumps(payload_dict).encode("utf-8")

        properties = pika.BasicProperties(
            reply_to=self.queue_name,
            app_id=self.alias,
            type=MessageCategory.MqttJsonBroadcast,
            correlation_id=str(uuid.uuid4()),
        )

        self._single_channel.basic_publish(
            exchange="amq.topic",
            routing_key=routing_key,
            body=payload_bytes,
            properties=properties,
        )
        print(f"Sent msg with routing key {routing_key} to amq.topic")
