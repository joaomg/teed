from typer.testing import CliRunner
from teed import program

runner = CliRunner()


# General


# @pytest.mark.skipif(helpers.is_platform("windows"), reason="It doesn't work for Windows")
def test_bulkcm_probe_program():
    result = runner.invoke(program, "bulkcm-probe data/bulkcm.xml")
    assert result.exit_code == 0
    assert result.stdout.count("Probing the BulkCm file: data/bulkcm.xml")
    assert result.stdout.count(
        "configData, distinguished name prefix: DC=a1.companyNN.com"
    )
    assert result.stdout.count("SubNetwork id: 1")
    assert result.stdout.count("#ManagedElement: 2")


def test_bulkcm_parse_program():
    result = runner.invoke(program, "bulkcm-parse data/bulkcm.xml data")
    assert result.exit_code == 0
