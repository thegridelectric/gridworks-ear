
import dotenv
from gear.config import EarSettings
from gwbase.enums import GNodeRole, UniverseType
from gear.ear import Ear
import uuid
import time
import json
from gwbase.types import Ready, HeartbeatA
from gwbase.enums import MessageCategory
import pika
settings = EarSettings(_env_file=dotenv.find_dotenv())

from tests.scada_stub import TestScada

scada_stub = TestScada(settings)

settings.rabbit.url.get_secret_value()
scada_stub.start()

scada_stub.send_status()





