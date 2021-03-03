import typer
from .main import program
from teed.bulkcm import to_csv, probe


@program.command(name="bulkcm-probe")
def program_bulkcm_probe(file_path: str):
    """
    Run BulkCm probe, receives one XML bulkcm file and outputs information to console
    """

    # Probe bulkcm in file_path
    try:
        probe(file_path)
    except Exception as exception:
        typer.secho(str(exception), err=True, fg=typer.colors.RED, bold=True)
        raise typer.Exit(1)


@program.command(name="bulkcm-parse")
def program_bulkcm_parse(file_path: str, output_dir: str):
    """
    Run BulkCm parsing, receives one XML bulkcm file and outputs CSV to directory
    """

    # Parse bulkcm in file_path, write content to CSV files in the output directory
    try:
        to_csv(file_path, output_dir)
    except Exception as exception:
        typer.secho(str(exception), err=True, fg=typer.colors.RED, bold=True)
        raise typer.Exit(1)
