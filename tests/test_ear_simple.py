import time

import dotenv

from gear.config import EarSettings
from gear.ear import Ear


# from tests.scada_stub import DummyScada
#
# def test_send_scada_status() -> None:
#
#     settings = EarSettings(_env_file=dotenv.find_dotenv())
#     scada_stub = DummyScada(settings)
#     scada_stub.start()
#     try:
#         scada_stub.send_status()
#     finally:
#         try:
#             scada_stub.stop()
#         except: # noqa
#             pass


def test_start_stop_ear() -> None:
    settings = EarSettings(_env_file=dotenv.find_dotenv())
    # settings.rabbit.url.get_secret_value()
    ear = Ear(settings)
    ear.start()
    try:
        time.sleep(0.25)
    finally:
        try:
            ear.stop()
        except:  # noqa
            pass
