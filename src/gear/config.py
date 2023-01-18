"""Settings for the GridWorks Ear, readable from environment and/or from env files."""
from typing import List, Optional
from gridworks.gw_config import GNodeSettings

from pydantic import BaseModel, BaseSettings, SecretStr

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
    world_instance_alias: str = "d1__1"
    g_node_data_type: str = '{"EarDns": "localhost", "WorldInstanceAlias": "dwtest__1", "AtomicTNodeAliasList": ["dwtest.isone.ct.newhaven.orange1"], "ScadaAliasList": ["dwtest.isone.ct.newhaven.orange1.ta.scada"], "TypeAlias": "ear.g.node.alias.list.toss.100"}'
    my_fqdn: str = "d1-1.electricity.works"
    aws: AwsClient = AwsClient()
    slack: SlackClient = SlackClient()
    output_dir: str = "output"
    logging_on: bool = False
    log_message_summary: bool = False
    mqtt_client_logging_on: bool = False
    mqtt_client_logger_name: str = "gridworks.ear"
    minute_cron_file: str = "cron_last_minute.txt"
    hour_cron_file: str = "cron_last_hour.txt"
    day_cron_file: str = "cron_last_day.txt"
    hour_messages_count_file: str = "messages_heard_last_hour.txt"

    class Config:
        env_prefix = "EAR_"
        env_nested_delimiter = "__"


class WatchdogSettings(BaseSettings):
    warning_silencer_file: str = "DO_NOT_SEND_SERVICE_WARNINGS.txt"
    class Config:
        env_prefix = "WATCHDOG_"
        env_nested_delimiter = "__"