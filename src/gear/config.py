"""Settings for the GridWorks Ear, readable from environment and/or env files."""

from gwbase import ServiceSettings
from pydantic import BaseModel
from pydantic_settings import SettingsConfigDict

DEFAULT_ENV_FILE = ".env"


class S3TypeClient(BaseModel):
    """Settings for interacting with an S3-compatible object store.

    Default endpoint (empty `endpoint_url`) is AWS S3, as always. Setting
    `endpoint_url` aims the same boto3 client at any S3-compatible host —
    e.g. Backblaze B2 (`https://s3.us-east-005.backblazeb2.com`) for the
    seed ear's `gw-seedstore`."""

    profile_name: str = "default"
    region_name: str = "us-east-1"
    bucket_name: str = "gwdev"
    endpoint_url: str = ""


class EarSettings(ServiceSettings):
    """The ear as a gwbase service: `ServiceSettings` identity (the
    `service_alias` is the witness identity — last segment of every object
    key) plus the ear's own wires. One env prefix: `EAR_`."""

    service_name: str = "ear"  # XDG path segment
    # First path segment of every object key: the world this ear witnesses.
    world_instance_alias: str = "d1__1"
    # The exchange this ear's queue binds (`#`). Default = the universal
    # audit tap; a scoped instance points at its slice instead (e.g.
    # `gnr_ear_tx`, the registry slice).
    consume_exchange: str = "ear_tx"
    s3: S3TypeClient = S3TypeClient()

    model_config = SettingsConfigDict(
        env_prefix="EAR_",
        env_nested_delimiter="__",
        env_file=DEFAULT_ENV_FILE,
        extra="ignore",
    )
