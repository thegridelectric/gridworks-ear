import enum
import time
from typing import Any, NamedTuple, Optional
from slack_sdk.webhook import WebhookClient

import pendulum

DEFAULT_STEP_DURATION = 0.1

class MessageFormatType(enum.Enum):
    JSON = "Json"
    GW_SERIAL = "GwSerial"


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
) -> str:
    """ Requires a webhook loaded with the webhook url from ear.settings and
    should be used to send a  warning message.  Returns the response code from 
    the WebHookClient"""
    response = webhook.send(
        text="fallback",
        blocks=[
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*S3 Ear Warning!:*\n {warning_type.value}: {warning_message}"
                }
            }
        ]
    )
    try:
        status_code = response.status_code
    except:
        return "No status code returned in webhook response!"
    return status_code


def send_recovery_to_slack(
    webhook: WebhookClient, 
    warning_type: EarWarningType, 
    recovery_message: str
) -> str:
    """ Requires a webhook loaded with the webhook url from ear.settings and
    should be used to send a recovery message. Returns the response code from 
    the WebHookClient"""
    response = webhook.send(
        text="fallback",
        blocks=[
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*S3 Ear Recovery!:*\n {warning_type.value}: {recovery_message}"
                }
            }
        ]
    )
    try:
        status_code = response.status_code
    except:
        return "No status code returned in webhook response!"
    return status_code


def short_log_time() -> str:
    time_utc = pendulum.now("UTC")
    return time_utc.strftime("%Y-%m-%d %H:%M:%S")


class BasicLog:
    DEFAULT_FORMAT = "{timestamp} {level:5s}: {log_note:33s}"

    @classmethod
    def format(
        cls,
        level: str,  # TODO: turn this into an enum
        log_note: str,
        timestamp: Optional[pendulum.datetime] = None,
    ) -> str:
        """
        Formats a single line summary of message receipt/publication.

        Args:
            log_note: the info level note
            timestamp: "pendulum.now("UTC") by default"

        Returns:
            Formatted string.
        """
        try:
            if timestamp is None:
                timestamp = pendulum.now("UTC")
            return cls.DEFAULT_FORMAT.format(
                timestamp=timestamp.isoformat(),
                level=level,
                log_note=log_note,
            )
        except Exception as e:
            print(f"ouch got {e}")
            return ""
