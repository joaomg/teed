import os
import csv
from lxml import etree
from teed import bulkcm


def test_probe():
    """ Test bulkcm.probe """

    # default probing
    assert bulkcm.probe("data/bulkcm.xml") == {
        "encoding": "UTF-8",
        "nsmap": {
            None: "http://www.3gpp.org/ftp/specs/archive/32_series/32.615#configData",
            "xn": "http://www.3gpp.org/ftp/specs/archive/32_series/32.625#genericNrm",
        },
        "fileHeader": None,
        "configData": [
            {
                "dnPrefix": "DC=a1.companyNN.com",
                "SubNetwork(s)": [{"id": "1", "ManagementNode": 1, "ManagedElement": 2}],
            }
        ],
        "fileFooter": None,
    }

    # probe by ManagementNode and ManagedElement
    assert bulkcm.probe("data/bulkcm.xml", ["ManagementNode", "ManagedElement"]) == {
        "encoding": "UTF-8",
        "nsmap": {
            None: "http://www.3gpp.org/ftp/specs/archive/32_series/32.615#configData",
            "xn": "http://www.3gpp.org/ftp/specs/archive/32_series/32.625#genericNrm",
        },
        "fileHeader": None,
        "configData": [
            {
                "dnPrefix": "DC=a1.companyNN.com",
                "SubNetwork(s)": [{"id": "1", "ManagementNode": 1, "ManagedElement": 2}],
            }
        ],
        "fileFooter": None,
    }

    # an invalid XML file raises an exception
    try:
        bulkcm_file = []
        bulkcm_file = bulkcm.probe("data/tag_mismatch.xml")
    except Exception as e:
        # check the outcome is still an empty list
        assert bulkcm_file == []

        # check the exception message
        # signals an invalid XML doc
        assert (
            str(e)
            == "Opening and ending tag mismatch: abx line 15 and abcMax, line 15, column 65 (tag_mismatch.xml, line 15)"
        )


def test_split():
    """ Test bulkcm.split """

    for sn_id, sn_file_path in bulkcm.split("data/bulkcm.xml", "tests"):
        assert sn_id == "1"
        assert sn_file_path == "tests/bulkcm_1.xml"
        assert os.path.exists(sn_file_path)

    # remove tests/bulkcm_SubNetwork_1.xml if exists
    try:
        os.remove("tests/bulkcm_1.xml")
    except FileNotFoundError:
        pass

    # creating the new tests/bulkcm_SubNetwork_1.xml
    for sn_id, sn_file_path in bulkcm.split("data/bulkcm.xml", "tests"):

        # check if tests/bulkcm_SubNetwork_1.xml exists
        assert os.path.exists(sn_file_path)

    # compare contents with the input
    # they must be the same since there's
    # only a SubNetwork in bulkcm.xml
    # using ns_clean we ignore the extra ns
    # placed in the SubNetwork elements
    parser = etree.XMLParser(
        no_network=True,
        ns_clean=True,
        remove_blank_text=True,
        remove_comments=True,
        remove_pis=True,
        huge_tree=True,
        recover=False,
    )
    source = etree.parse("data/bulkcm.xml", parser=parser)
    target = etree.parse(sn_file_path, parser=parser)

    assert etree.tostring(source) == etree.tostring(target)


def test_parse():
    """ Test bulkcm.parse """

    stream = bulkcm.BulkCmParser.stream_to_csv("data")
    bulkcm.parse("data/bulkcm.xml", "data", stream)

    try:
        stream = bulkcm.BulkCmParser.stream_to_csv("data")
        bulkcm.parse("data/bulkcm_empty.xml", "data", stream)
    except Exception as e:
        assert str(e) == "Document is empty, line 1, column 1 (bulkcm_empty.xml, line 1)"

    stream = bulkcm.BulkCmParser.stream_to_csv("data")
    bulkcm.parse("data/bulkcm_no_configData.xml", "data", stream)

    stream = bulkcm.BulkCmParser.stream_to_csv("data")
    metadata, duration = bulkcm.parse(
        "data/bulkcm_with_header_footer.xml", "data", stream
    )

    assert metadata == {
        "dateTime": "2001-05-07T12:00:00+02:00",
        "fileFormatVersion": "32.615 V4.0",
        "senderName": "DC=a1.companyNN.com,SubNetwork=1,IRPAgent=1",
        "vendorName": "Company NN",
    }

    with open(
        "data/ManagedElement-86e5e4a02537853275d324e413ad88aa.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == [
            "node_id",
            "managedElementType",
            "userLabel",
            "vendorName",
            "userDefinedState",
            "locationName",
        ]
        assert list(reader) == [
            {
                "node_id": "1",
                "managedElementType": "RNC",
                "userLabel": "Paris RN1",
                "vendorName": "Company NN",
                "userDefinedState": "commercial",
                "locationName": "Champ de Mars",
            },
            {
                "node_id": "2",
                "managedElementType": "RNC",
                "userLabel": "Paris RN2",
                "vendorName": "Company NN",
                "userDefinedState": "commercial",
                "locationName": "Concorde",
            },
        ]

    with open(
        "data/ManagementNode-b399070ab476be4cc8e408571d5ad171.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == [
            "node_id",
            "userLabel",
            "vendorName",
            "userDefinedState",
            "locationName",
        ]
        assert list(reader) == [
            {
                "node_id": "1",
                "userLabel": "Paris MN1",
                "vendorName": "Company NN",
                "userDefinedState": "commercial",
                "locationName": "Montparnasse",
            }
        ]

    with open(
        "data/SubNetwork-72cb0caa41e7305a3da123a321ba44a7.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == ["node_id", "userLabel", "userDefinedNetworkType"]
        assert list(reader) == [
            {"node_id": "1", "userLabel": "Paris SN1", "userDefinedNetworkType": "UMTS"}
        ]

    stream = bulkcm.BulkCmParser.stream_to_csv("data")
    metadata, duration = bulkcm.parse(
        "data/bulkcm_with_vsdatacontainer.xml", "data", stream
    )

    with open(
        "data/SubNetwork-c693ebc80e2b73b0d3bbece47c529399.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == ["node_id"]
        assert list(reader) == [{"node_id": "1"}]

    with open(
        "data/ManagedElement-c693ebc80e2b73b0d3bbece47c529399.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == ["node_id"]
        assert list(reader) == [{"node_id": "1"}]

    with open(
        "data/RncFunction-c693ebc80e2b73b0d3bbece47c529399.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == ["node_id"]
        assert list(reader) == [{"node_id": "1"}]

    with open(
        "data/vsDataRncHandOver-556b3e085f577badc121eae9d6f1c7e1.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == ["node_id", "abcMin", "abcMax"]
        assert list(reader) == [{"node_id": "1", "abcMin": "12", "abcMax": "34"}]

    stream = bulkcm.BulkCmParser.stream_to_csv("data")
    metadata, duration = bulkcm.parse("data/bulkcm_with_utrancell.xml", "data", stream)

    with open(
        "data/vsDataUtranCell-a19f9c0eafa3491fee187d78689cd574.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == ["node_id", "sc", "pcpichpower"]
        assert list(reader) == [{"node_id": "Cell1", "sc": "111", "pcpichpower": "222"}]

    with open(
        "data/vsDataRncHandOver-556b3e085f577badc121eae9d6f1c7e1.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == ["node_id", "abcMin", "abcMax"]
        assert list(reader) == [{"node_id": "1", "abcMin": "12", "abcMax": "34"}]
