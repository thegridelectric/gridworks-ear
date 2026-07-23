import contextlib
import logging
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Annotated

import dotenv
import rich
import typer

from gear.config import EarSettings
from gear.ear import Ear
from gear.utils import (
    EAR_LOG_FORMAT_STR,
    EAR_MESSAGE_LOG_PATH,
    EAR_STATE_LOG_PATH,
    LOG_BACKUPS,
    LOG_DIRECTORY,
    LOG_MESSAGE_BYTES,
    OUTPUT_DIRECTORY,
    STATE_DIRECTORY,
)
from gear.utils import EAR_MESSAGE_LOGGER as LGMSG
from gear.utils import EAR_STATE_LOGGER as LGST

app = typer.Typer(no_args_is_help=True)

VERBOSITY_INFO = 1
VERBOSITY_DEBUG = 2
VERBOSITY_MESSAGES = 3

TICK_SECONDS = 5


def _setup_logging(verbose: int) -> None:
    if verbose >= VERBOSITY_DEBUG:
        log_level = logging.DEBUG
    elif verbose == VERBOSITY_INFO:
        log_level = logging.INFO
    else:
        log_level = logging.WARNING
    logging.basicConfig(level=log_level, format=EAR_LOG_FORMAT_STR)
    LOG_DIRECTORY.mkdir(exist_ok=True, parents=True)
    for logger, log_path in [
        (LGST, EAR_STATE_LOG_PATH),
        (LGMSG, EAR_MESSAGE_LOG_PATH),
    ]:
        handler = RotatingFileHandler(
            filename=log_path,
            maxBytes=LOG_MESSAGE_BYTES,
            backupCount=LOG_BACKUPS,
        )
        handler.setFormatter(logging.Formatter(EAR_LOG_FORMAT_STR))
        logger.addHandler(handler)
        logger.propagate = False
        logger.setLevel(logging.INFO)
    LGST.addHandler(logging.StreamHandler())
    if verbose >= VERBOSITY_MESSAGES:
        LGMSG.addHandler(logging.StreamHandler())


@app.callback()
def main_app_callback(
    verbose: Annotated[int, typer.Option("--verbose", "-v", count=True)] = 0,
) -> None:
    _setup_logging(verbose)


@app.command()
def config() -> None:
    """Show configuration and exit."""
    env_path = Path(dotenv.find_dotenv())
    rich.print(EarSettings(_env_file=env_path))
    rich.print(f"Env file: <{env_path}>  exists: {env_path.exists()}")
    rich.print(f"State directory: {STATE_DIRECTORY}")
    rich.print(f"Output directory: <{OUTPUT_DIRECTORY}>")
    rich.print(f"Log directory: <{LOG_DIRECTORY}>")


@app.command()
def listen(*, s3: bool = True) -> None:
    """Run the ear."""
    env_path = Path(dotenv.find_dotenv())
    settings = EarSettings(_env_file=env_path)
    ear = Ear(settings, use_s3=s3)
    rich.print("Running the Ear")
    LGST.info("++ear listen")
    LGST.info(f"Env file: <{env_path}>  exists: {env_path.exists()}")
    LGST.info(f"Consume exchange: {settings.consume_exchange}")
    LGST.info(f"Bucket: {settings.s3.bucket_name}")
    LGST.info(f"use s3?: {s3}")
    ear.start()
    try:
        while ear.main_loop_running:
            time.sleep(TICK_SECONDS)
            ear.periodic_tick()
    except KeyboardInterrupt:
        s = "Ear stopped by keyboard interrupt."
        rich.print(s)
        LGST.info(s)
    finally:
        with contextlib.suppress(Exception):
            ear.stop()
    LGST.info("--ear listen")


if __name__ == "__main__":
    app()
