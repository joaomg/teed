import typer
from typing import Optional
from . import config
from . import bulkcm

# Program


program = typer.Typer()
program.add_typer(bulkcm.program, name="bulkcm")

# Helpers


def version(value: bool):
    if value:
        typer.echo(config.VERSION)
        raise typer.Exit()


# Command


@program.callback()
def program_main(
    version: Optional[bool] = typer.Option(None, "--version", callback=version)
):
    """Analyze and transform telco data."""
    pass


if __name__ == "__main__":
    program(prog_name="teed")
