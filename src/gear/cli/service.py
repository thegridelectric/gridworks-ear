import os
import subprocess
from pathlib import Path

import typer

app = typer.Typer(no_args_is_help=True)


def _run_command(command: str | list[str], dry_run: bool) -> None:
    if isinstance(command, str):
        command = command.split()
    if dry_run:
        print(" ".join(command))
    else:
        result = subprocess.run(command, check=False, capture_output=True)
        if result.stderr:
            print(result.stderr.decode("utf-8"))
        if result.stdout:
            print(result.stdout.decode("utf-8"))


def _run_commands(commands: list[str | list[str]], dry_run: bool) -> None:
    for command in commands:
        _run_command(command, dry_run)


def _uninstall(dry_run: bool = False):
    _run_commands(
        [
            "sudo systemctl stop gridworks-ear.service",
            "sudo systemctl disable gridworks-ear.service",
            "sudo systemctl daemon-reload",
            "sudo rm /lib/systemd/system/gridworks-ear.service",
            "rm /home/ubuntu/gridworks-ear-service-env",
        ],
        dry_run=dry_run,
    )


@app.command()
def install(dry_run: bool = False):
    _uninstall(dry_run=dry_run)
    _run_commands(
        [
            f"sudo ln -s {Path(__file__).parent}/gridworks-ear.service /lib/systemd/system",
            f"ln -s {os.getenv('VIRTUAL_ENV')} /home/ubuntu/gridworks-ear-service-env",
            "sudo systemctl enable /lib/systemd/system/gridworks-ear.service",
            "sudo systemctl start gridworks-ear.service",
        ],
        dry_run,
    )


@app.command()
def uninstall(dry_run: bool = False):
    _uninstall(dry_run=dry_run)


@app.command()
def start(dry_run: bool = False):
    _run_commands(["sudo systemctl start gridworks-ear.service"], dry_run=dry_run)


@app.command()
def stop(dry_run: bool = False):
    _run_commands(["sudo systemctl stop gridworks-ear.service"], dry_run=dry_run)


@app.command()
def restart(dry_run: bool = False):
    _run_commands(["sudo systemctl restart gridworks-ear.service"], dry_run=dry_run)


@app.command()
def status(dry_run: bool = False):
    _run_commands(
        ["systemctl status --no-pager -n 0 gridworks-ear.service"], dry_run=dry_run
    )
