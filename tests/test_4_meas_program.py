from teed.meas import program
from typer.testing import CliRunner

runner = CliRunner()


def test_meas_parse_program():

    result = runner.invoke(program, ["data/mdc*.xml", "data"])
    assert result.exit_code == 0
    assert result.stdout.count("Parent process exiting...")
    # print(result.stdout)
    # assert result.stdout.count("Producer starting")
    # assert result.stdout.count("Consumer starting")
