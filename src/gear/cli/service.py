import typer

app = typer.Typer(no_args_is_help=True)


def _uninstall():
    ...
    # sudo systemctl stop gridworks-ear.service
    # sudo systemctl disable gridworks-ear.service
    # sudo systemctl daemon-reload
    # sudo rm /lib/systemd/system/gridworks-ear.service
    # rm /home/ubuntu/gridworks-ear-service-env


@app.command()
def install():
    _uninstall()
    #
    # sudo ln -s ./gridworks-ear.service /lib/systemd/system
    # ln -s /home/ubuntu/gridworks-ear-service-env
    # sudo systemctl enable /lib/systemd/system/gridworks-ear.service
    # sudo systemctl start gridworks-ear.service


@app.command()
def uninstall():
    _uninstall()


@app.command()
def start():
    ...
    # sudo systemctl start gridworks-ear.service


@app.command()
def stop():
    ...
    # sudo systemctl stop gridworks-ear.service


@app.command()
def restart():
    ...
    # sudo systemctl restart gridworks-ear.service


@app.command()
def status():
    ...
    # systemctl status --no-pager -n 0 gridworks-ear.service
