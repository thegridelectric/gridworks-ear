import enum
import logging
from typing import NamedTuple

import pendulum
import xdg
from slack_sdk.webhook import WebhookClient

DEFAULT_STEP_DURATION = 0.1

EAR_LOG_FORMAT_STR = "%(asctime)s - %(message)s"

EAR_LOG_NAME = "ear"
EAR_STATE_LOG_NAME = EAR_LOG_NAME + ".state"
EAR_MESSAGE_LOG_NAME = EAR_LOG_NAME + ".message"

EAR_LOGGER = logging.getLogger(EAR_LOG_NAME)
EAR_STATE_LOGGER = logging.getLogger(EAR_STATE_LOG_NAME)
EAR_MESSAGE_LOGGER = logging.getLogger(EAR_MESSAGE_LOG_NAME)

_EAR_SUBDIR = "gridworks/ear"
OUTPUT_DIRECTORY = xdg.xdg_data_home() / _EAR_SUBDIR / "output"
STATE_DIRECTORY = xdg.xdg_state_home() / _EAR_SUBDIR
LOG_DIRECTORY = STATE_DIRECTORY / "log"
EAR_STATE_LOG_PATH = LOG_DIRECTORY / "state.txt"
EAR_MESSAGE_LOG_PATH = LOG_DIRECTORY / "message.txt"
LOG_MESSAGE_BYTES = 1 * 1024 * 1024
LOG_BACKUPS = 4


class WorldType(enum.Enum):
    REAL = "Real"
    SHADOW = "Shadow"
    DEV = "Dev"
    HYBRID = "Hybrid"


class QOS(enum.Enum):
    AtMostOnce = 0
    AtLeastOnce = 1
    ExactlyOnce = 2


class Subscription(NamedTuple):
    Topic: str
    Qos: QOS


class EarWarningType(enum.Enum):
    EAR_APPEARS_DEAD = "EarAppearsDead"
    EAR_HEARD_NO_MESSAGES_FOR_AN_HOUR = "EarHeardNoMessagesForAnHour"


def send_warning_to_slack(
    webhook: WebhookClient,
    warning_type: EarWarningType,
    warning_message: str,
) -> int:
    """Requires a webhook loaded with the webhook url from ear.settings and
    should be used to send a  warning message.  Returns the response code from
    the WebHookClient"""
    response = webhook.send(
        text="fallback",
        blocks=[
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*S3 Ear Warning!:*\n {warning_type.value}: {warning_message}",
                },
            },
        ],
    )
    return response.status_code


def send_recovery_to_slack(
    webhook: WebhookClient,
    warning_type: EarWarningType,
    recovery_message: str,
) -> int:
    """Requires a webhook loaded with the webhook url from ear.settings and
    should be used to send a recovery message. Returns the response code from
    the WebHookClient"""
    response = webhook.send(
        text="fallback",
        blocks=[
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*S3 Ear Recovery!:*\n {warning_type.value}: {recovery_message}",
                },
            },
        ],
    )
    return response.status_code


def short_log_time() -> str:
    time_utc = pendulum.now("UTC")
    return time_utc.strftime("%Y-%m-%d %H:%M:%S")
