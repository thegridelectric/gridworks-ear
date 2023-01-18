"""Command-line interface."""
import click


@click.command()
@click.version_option()
def main() -> None:
    """Gridworks Ear."""


if __name__ == "__main__":
    main(prog_name="gridworks-ear")  # pragma: no cover
