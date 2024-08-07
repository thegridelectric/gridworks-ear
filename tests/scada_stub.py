import json
import uuid
from pathlib import Path

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
        status_file = "d1.isone.me.versant.keene.beech.scada-gridworks.event.gt.sh.status-1715905380350-100.26.91.172.json"
        type_name = "gridworks.event.gt.sh.status"
        routing_key = (
            "gw." + self.alias.replace(".", "-") + "." + type_name.replace(".", "-")
        )

        assert status_file.split("-")[0] == self.alias
        assert status_file.split("-")[1] == type_name
        assert routing_key == STATUS_ROUTING_KEY
        path = Path(self.folder_base + status_file)
        with path.open() as f:
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
