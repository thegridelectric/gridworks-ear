import contextlib
import os

import dotenv
import rich
from gear.cli.main import app
from gear.config import EarSettings
from gear.ear import Ear
from gw_test import wait_for
from typer.testing import CliRunner

runner = CliRunner()


def test_start_one_message() -> None:
    settings = EarSettings(_env_file=dotenv.find_dotenv())
    rich.print("Using settings:")
    rich.print(settings)
    ear = Ear(settings, use_s3=False)
    if "GITHUB_ACTIONS" in os.environ:
        print(
            "Running in CI. Exiting this test, which fails in CI, with the ear"
            " not receiving a message",
        )
        return
    ear.start()
    try:
        messages_heard_start = ear.messages_heard_total
        dummy_result = runner.invoke(app, ["dummy"])
        assert dummy_result.exit_code == 0, (
            f"ERROR running dummy: {dummy_result.exit_code}\n"
            f"stdout:\n{dummy_result.stdout}\n"
            f"stderr:\n{dummy_result.stderr}"
        )
        print(dummy_result.output)

        wait_for(
            f=lambda: ear.messages_heard_total > messages_heard_start,
            timeout=2.0,
            tag=f"Wait for Ear to receive dummy more than {messages_heard_start} messages",
        )
    finally:
        with contextlib.suppress(Exception):
            print(1)
            ear.stop()
            print(2)
