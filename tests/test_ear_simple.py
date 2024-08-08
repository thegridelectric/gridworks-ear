import dotenv
from gear.cli.main import app
from gear.config import EarSettings
from gear.ear import Ear
from gw_test import wait_for
from typer.testing import CliRunner

runner = CliRunner()


def test_start_one_message() -> None:
    settings = EarSettings(_env_file=dotenv.find_dotenv())
    ear = Ear(settings, use_s3=False)
    ear.start()
    try:
        messages_heard_start = ear.messages_heard_total
        dummy_result = runner.invoke(app, ["dummy"])
        assert dummy_result.exit_code == 0
        wait_for(
            f=lambda: ear.messages_heard_total > messages_heard_start,
            timeout=2.0,
            tag=f"Wait for Ear to receive dummy more than {messages_heard_start} messages",
        )
    finally:
        try:
            ear.stop()
        except:  # noqa
            pass
