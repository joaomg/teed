from typer.testing import CliRunner

from teed import __version__
from teed.__main__ import program

runner = CliRunner()


def test_teed_program():
    result = runner.invoke(program, "--version")
    assert result.exit_code == 0
    assert result.stdout.count(__version__)
