

from gear.config import EarSettings
from gwbase.enums import GNodeRole, UniverseType
from gear.ear import Ear
import uuid
import time
import json
from gwbase.types import Ready, HeartbeatA
from gwbase.enums import MessageCategory
import pendulum
import dotenv
from gear.config import EarSettings
settings = EarSettings(_env_file=dotenv.find_dotenv())
settings.rabbit.url.get_secret_value()

ear = Ear(settings)

ear.start()
