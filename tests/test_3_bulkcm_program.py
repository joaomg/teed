from typer.testing import CliRunner
from teed.bulkcm import program

runner = CliRunner()


def test_bulkcm_probe_program():

    # valid bulkcm file
    result = runner.invoke(program, "probe data/bulkcm.xml")
    assert result.exit_code == 0
    assert result.stdout.count("Probing data/bulkcm.xml")

    assert result.stdout.count(
        """[{'dnPrefix': 'DC=a1.companyNN.com',
  'SubNetwork(s)': [{'id': '1',
                     'ManagementNode': 1,
                     'MeContext': 0,
                     'ManagedElement': 2}]}]"""
    )
    assert result.stdout.count("Duration: ")

    # invalid xml file
    result = runner.invoke(program, "probe data/tag_mismatch.xml")
    assert result.exit_code == 1
    assert result.stdout.count("Probing data/tag_mismatch.xml")
    assert result.stdout.count(
        "Opening and ending tag mismatch: abx line 0 and abcMax, line 15, column 65 (tag_mismatch.xml, line 15)"
    )


def test_bulkcm_split_program():

    # split bulkcm.xml to data directory
    result = runner.invoke(program, "split data/bulkcm.xml data")
    assert result.exit_code == 0
    assert result.stdout.count("Spliting the BulkCm file by SubNetwork: data/bulkcm.xml")
    assert result.stdout.count("SubNetwork 1 to data/bulkcm_SubNetwork_1.xml")
    assert result.stdout.count("#SubNetwork found: #1")

    # lacking output directory parameter
    result = runner.invoke(program, "split data/tag_mismatch.xml")
    assert result.exit_code == 2
    assert result.stdout.count("Error: Missing argument 'OUTPUT_DIR'.")

    # invalid xml file
    result = runner.invoke(program, "split data/tag_mismatch.xml data")
    assert result.exit_code == 1
    assert result.stdout.count(
        "Spliting the BulkCm file by SubNetwork: data/tag_mismatch.xml"
    )
    assert result.stdout.count(
        "Opening and ending tag mismatch: abx line 15 and abcMax, line 15, column 65 (tag_mismatch.xml, line 15)"
    )


def test_bulkcm_parse_program():

    result = runner.invoke(program, "parse data/bulkcm.xml data")
    assert result.exit_code == 0
    assert result.stdout.count("Parsing data/bulkcm.xml")

    # invalid xml file
    result = runner.invoke(program, "parse data/tag_mismatch.xml data")
    assert result.exit_code == 1
    assert result.stdout.count("Parsing data/tag_mismatch.xml")
    assert result.stdout.count("Error parsing data/tag_mismatch.xml")
    assert result.stdout.count(
        "Opening and ending tag mismatch: abx line 0 and abcMax, line 15, column 65 (tag_mismatch.xml, line 15)"
    )
