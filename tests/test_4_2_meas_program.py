import os
from teed.meas import program
from typer.testing import CliRunner

runner = CliRunner()


def test_meas_parse_program():

    try:
        os.remove("data/UtranCell-900-9995823c30bcf308b91ab0b66313e86a.csv")
    except FileNotFoundError:
        pass

    result = runner.invoke(program, ["data/mdc*.xml", "data"])
    assert result.exit_code == 0
    assert result.stdout.count("Producer and consumer done, exiting.")
    assert os.path.exists("data/UtranCell-900-9995823c30bcf308b91ab0b66313e86a.csv")
