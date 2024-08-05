"""Settings for the GridWorks Ear, readable from environment and/or from env files."""
from typing import List
from typing import Optional

from gwbase.config import EnumSettings
from gwbase.config import GNodeSettings
from pydantic import BaseModel
from pydantic import SecretStr
from pydantic_settings import BaseSettings


DEFAULT_ENV_FILE = ".env"


class SlackClient(BaseModel):
    web_hook_url: str = ""


class AwsClient(BaseModel):
    """Settings for interacting with Aws"""

    region_name: str = "us-east-1"
    hosted_zone_id: SecretStr = SecretStr("")
    bucket_name: str = "gwdev"


class EarSettings(GNodeSettings):
    """Settings for the GridWorks ear."""

    g_node_alias: str = "d1.ear"
    g_node_id: str = "00000000-0000-0000-0000-000000000000"
    world_instance_alias: str = "d1__1"
    my_fqdn: str = "localhost"  # the fqdn for the ear
    aws: AwsClient = AwsClient()
    slack: SlackClient = SlackClient()
    output_dir: str = "output"
    logging_on: bool = False
    log_message_summary: bool = False
    minute_cron_file: str = "cron_last_minute.txt"
    hour_cron_file: str = "cron_last_hour.txt"
    day_cron_file: str = "cron_last_day.txt"
    hour_messages_count_file: str = "messages_heard_last_hour.txt"

    class Config:
        env_prefix = "EAR_"
        env_nested_delimiter = "__"
        extra = "ignore"  # Ignore extra fields in the environment


class WatchdogSettings(BaseSettings):
    warning_silencer_file: str = "DO_NOT_SEND_SERVICE_WARNINGS.txt"

    class Config:
        env_prefix = "WATCHDOG_"
        env_nested_delimiter = "__"
