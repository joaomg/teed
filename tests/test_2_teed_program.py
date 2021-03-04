from typer.testing import CliRunner
from teed.__main__ import program
from teed import __version__

runner = CliRunner()


def test_teed_program():
    result = runner.invoke(program, "--version")
    assert result.exit_code == 0
    assert result.stdout.count(__version__)
