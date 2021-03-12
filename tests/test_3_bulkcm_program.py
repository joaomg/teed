from teed.bulkcm import program
from typer.testing import CliRunner

runner = CliRunner()


def test_bulkcm_probe_program():

    # valid bulkcm file
    result = runner.invoke(program, "probe data/bulkcm.xml")
    assert result.exit_code == 0
    assert result.stdout.count("Probing data/bulkcm.xml")

    assert result.stdout.count(
        """{'encoding': 'UTF-8',
 'nsmap': {None: 'http://www.3gpp.org/ftp/specs/archive/32_series/32.615#configData',
           'xn': 'http://www.3gpp.org/ftp/specs/archive/32_series/32.625#genericNrm'},
 'fileHeader': None,
 'configData': [{'dnPrefix': 'DC=a1.companyNN.com',
                 'SubNetwork(s)': [{'id': '1',
                                    'ManagementNode': 1,
                                    'ManagedElement': 2}]}],
 'fileFooter': None}"""
    )
    assert result.stdout.count("Duration: ")

    # invalid xml file
    result = runner.invoke(program, "probe data/tag_mismatch.xml")
    assert result.exit_code == 1
    assert result.stdout.count("Probing data/tag_mismatch.xml")
    assert result.stdout.count(
        "Opening and ending tag mismatch: abx line 15 and abcMax, line 15, column 65 (tag_mismatch.xml, line 15)"
    )


def test_bulkcm_split_program():

    # split bulkcm.xml to data directory, ignoring the SubNetwork 1
    result = runner.invoke(program, "split data/bulkcm.xml data -s dummyNetwork")
    assert result.exit_code == 0
    assert result.stdout.count("Spliting data/bulkcm.xml to data")
    assert result.stdout.count("Ignored SubNetwork 1")
    assert result.stdout.count("SubNetwork processed: #0")
    assert result.stdout.count("SubNetwork ignored: #1")

    # split bulkcm.xml to data directory
    result = runner.invoke(program, "split data/bulkcm.xml data")
    assert result.exit_code == 0
    assert result.stdout.count("Spliting data/bulkcm.xml to data")
    assert result.stdout.count("SubNetwork 1 in data/bulkcm_1.xml")
    assert result.stdout.count("SubNetwork processed: #1")
    assert result.stdout.count("SubNetwork ignored: #0")

    # lacking output directory parameter
    result = runner.invoke(program, "split data/tag_mismatch.xml")
    assert result.exit_code == 2
    assert result.stdout.count("Error: Missing argument 'OUTPUT_DIR'.")

    # invalid xml file
    result = runner.invoke(program, "split data/tag_mismatch.xml data")
    assert result.exit_code == 1
    assert result.stdout.count("Spliting data/tag_mismatch.xml to data")
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
