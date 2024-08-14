import contextlib
import logging
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Annotated

import dotenv
import rich
import typer

from gear.cli.dummy import DummyScada
from gear.cli.service import app as service_app
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
app.add_typer(
    service_app,
    name="service",
    help="Interact with gridworks-ear systemd service.",
)

VERBOSITY_INFO = 1
VERBOSITY_DEBUG = 2
VERBOSITY_MESSAGES = 3


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
    rich.print(f"Cron directory: {STATE_DIRECTORY}")
    rich.print(f"Output directory: <{OUTPUT_DIRECTORY}>")
    rich.print(f"Log directory: <{LOG_DIRECTORY}>")


def _log_startup(env_path: Path, ear: Ear, use_s3: bool) -> None:  # noqa: FBT001
    rich.print("Running the Ear")
    LGST.info("++ear listen")
    s = f"Config file: <{env_path}>  exists: {env_path.exists()}"
    rich.print(s)
    LGST.info(s)
    LGST.info("Settings:\n")
    LGST.info(ear.settings.model_dump_json(indent=2))
    LGST.info(f"Env file: <{env_path}>  exists: {env_path.exists()}")
    LGST.info(f"Cron directory: {STATE_DIRECTORY}")
    LGST.info(f"Output directory: <{OUTPUT_DIRECTORY}>")
    LGST.info(f"Log directory: <{LOG_DIRECTORY}>")
    LGST.info(f"use s3?: {use_s3}")
    ear.log_times()


@app.command()
def listen(*, s3: bool = True) -> None:
    """Run the ear."""
    env_path = Path(dotenv.find_dotenv())
    settings = EarSettings(_env_file=env_path)
    ear = Ear(settings, use_s3=s3)
    _log_startup(env_path, ear, s3)
    ear.start()
    try:
        while ear.main_loop_running:
            time.sleep(5)
    except KeyboardInterrupt:
        s = "Ear stopped by keyboard interrupt."
        rich.print(s)
        LGST.info(s)
    finally:
        with contextlib.suppress(Exception):
            ear.stop()
    LGST.info("--ear listen")


@app.command()
def dummy(
    n: int = 1,
    interval: int = 5,
) -> None:
    """
    Run a dummy scada which periodically publishes *n* status messages at
    *interval* seconds. If *n* is 0, run forever.
    """
    rich.print(f"Running Dummy Scada, n:{n or 'forever'}  interval:{interval}")
    env_path = Path(dotenv.find_dotenv())
    rich.print(f"Config file: <{env_path}>  exists: {env_path.exists()}")
    settings = EarSettings(_env_file=env_path)
    rich.print(settings)
    scada_stub = DummyScada(settings)
    scada_stub.start()
    while not scada_stub.consuming:
        time.sleep(0.1)
    i = 0
    try:
        while n == 0 or i < n:
            scada_stub.send_status()
            i += 1
            if n == 0 or i < n:
                time.sleep(interval)
    finally:
        with contextlib.suppress(Exception):
            scada_stub.stop()


if __name__ == "__main__":
    app()
