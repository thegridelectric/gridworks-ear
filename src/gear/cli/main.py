import logging
import time
from pathlib import Path

import dotenv
import rich
import typer

from gear.cli.dummy import DummyScada
from gear.cli.service import app as service_app
from gear.config import EarSettings
from gear.ear import Ear

app = typer.Typer(no_args_is_help=True)
app.add_typer(
    service_app, name="service", help="Interact with gridworks-ear systemd service."
)


@app.callback()
def main_app_callback(
    verbose: bool = False,
):
    if verbose:
        print("Enabling verbose logging")
        logging.basicConfig()
        log = logging.getLogger()
        log.setLevel(logging.DEBUG)


@app.command()
def config() -> None:
    """Show configuration and exit."""
    env_path = Path(dotenv.find_dotenv())
    rich.print(f"Config file: <{env_path}>  exists: {env_path.exists()}")
    rich.print(EarSettings(_env_file=env_path))


@app.command()
def listen() -> None:
    """Run the ear."""
    rich.print("Running the Ear")
    env_path = Path(dotenv.find_dotenv())
    rich.print(f"Config file: <{env_path}>  exists: {env_path.exists()}")
    settings = EarSettings(_env_file=dotenv.find_dotenv())
    ear = Ear(settings)
    ear.start()
    try:
        while ear.main_loop_running:
            time.sleep(5)
    finally:
        try:
            ear.stop()
        except:  # noqa
            pass


@app.command()
def dummy(
    n: int = 1,
    interval: int = 5,
) -> None:
    """
    Run a dummy scada which periodically publishes *n* status messages at
    *interval* seconds. If *n* is 0, run forever.
    """
    rich.print(f"Running Dummy Scada, n:{n if n else 'forever'}  interval:{interval}")
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
        try:
            scada_stub.stop()
        except:  # noqa
            pass


if __name__ == "__main__":
    app()
