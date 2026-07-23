"""Settings for the GridWorks Ear, readable from environment and/or from env files."""

from gwbase.config import GNodeSettings
from pydantic import BaseModel, SecretStr
from pydantic_settings import BaseSettings

DEFAULT_ENV_FILE = ".env"


class SlackClient(BaseModel):
    web_hook_url: str = ""


class S3TypeClient(BaseModel):
    """Settings for interacting with an S3-compatible object store.

    Default endpoint (empty `endpoint_url`) is AWS S3, as always. Setting
    `endpoint_url` aims the same boto3 client at any S3-compatible host —
    e.g. Backblaze B2 (`https://s3.us-east-005.backblazeb2.com`) for the
    seed ear's `gw-seedstore`."""

    profile_name: str = "default"
    region_name: str = "us-east-1"
    hosted_zone_id: SecretStr = SecretStr("")
    bucket_name: str = "gwdev"
    endpoint_url: str = ""


class EarSettings(GNodeSettings):
    """Settings for the GridWorks ear."""

    g_node_alias: str = "d1.ear"
    g_node_id: str = "00000000-0000-0000-0000-000000000000"
    world_instance_alias: str = "d1__1"
    # The exchange this ear's queue binds (`#`). Default = the universal
    # audit tap; a second instance may point at a scoped tap instead (e.g.
    # `gnr_ear_tx`, the registry slice) to capture a small precious stream
    # into its own store — same code, different slice, different bucket.
    consume_exchange: str = "ear_tx"
    s3: S3TypeClient = S3TypeClient()
    slack: SlackClient = SlackClient()
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
