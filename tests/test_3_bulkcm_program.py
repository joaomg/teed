import re
from typer.testing import CliRunner

from teed.bulkcm import program

runner = CliRunner()
ansi_escape = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")


def test_bulkcm_probe_program():
    # valid bulkcm file
    result = runner.invoke(program, "probe data/bulkcm.xml")
    output = ansi_escape.sub("", result.stdout)
    assert result.exit_code == 0
    assert output.count("Probing data/bulkcm.xml")

    assert output.count(
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
    assert output.count("Duration: ")

    # invalid xml file
    result = runner.invoke(program, "probe data/tag_mismatch.xml")
    output = ansi_escape.sub("", result.stdout)
    assert result.exit_code == 1
    assert output.count("Probing data/tag_mismatch.xml")
    assert output.count(
        "Opening and ending tag mismatch: abx line 15 and abcMax, line 15, column 65 (<string>, line 15)"
    )


def test_bulkcm_split_program():
    # split bulkcm.xml to data directory, ignoring the SubNetwork 1
    result = runner.invoke(program, "split data/bulkcm.xml data -s dummyNetwork")
    output = ansi_escape.sub("", result.stdout)
    assert result.exit_code == 0
    assert output.count("Splitting data/bulkcm.xml to data")
    assert output.count("Ignored SubNetwork 1")
    assert output.count("SubNetwork processed: #0")
    assert output.count("SubNetwork ignored: #1")

    # split bulkcm.xml to data directory
    result = runner.invoke(program, "split data/bulkcm.xml data")
    output = ansi_escape.sub("", result.stdout)
    assert result.exit_code == 0
    assert output.count("Splitting data/bulkcm.xml to data")
    assert output.count("SubNetwork 1 in data/bulkcm_1.xml")
    assert output.count("SubNetwork processed: #1")
    assert output.count("SubNetwork ignored: #0")

    # lacking output directory parameter
    result = runner.invoke(program, "split data/tag_mismatch.xml")
    output = ansi_escape.sub("", result.stdout)
    assert result.exit_code == 2
    assert output.count(
        "Usage: root split [OPTIONS] FILE_PATH_OR_URI OUTPUT_DIR_OR_BUCKET"
    )

    # invalid xml file
    result = runner.invoke(program, "split data/tag_mismatch.xml data")
    output = ansi_escape.sub("", result.stdout)
    assert result.exit_code == 1
    assert output.count("Splitting data/tag_mismatch.xml to data")
    assert output.count(
        "Opening and ending tag mismatch: abx line 15 and abcMax, line 15, column 65 (tag_mismatch.xml, line 15)"
    )


def test_bulkcm_parse_program():
    result = runner.invoke(program, "parse data/bulkcm.xml data")
    output = ansi_escape.sub("", result.stdout)
    assert result.exit_code == 0
    assert output.count("Parsing data/bulkcm.xml")

    # invalid xml file
    result = runner.invoke(program, "parse data/tag_mismatch.xml data")
    output = ansi_escape.sub("", result.stdout)
    assert result.exit_code == 1
    assert output.count("Parsing data/tag_mismatch.xml")
    assert output.count("Error parsing data/tag_mismatch.xml")
    assert output.count(
        "Opening and ending tag mismatch: abx line 15 and abcMax, line 15, column 65 (tag_mismatch.xml, line 15)"
    )
