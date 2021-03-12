import csv
import os

import yaml
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

    # verify metadata returned by parse
    assert metadata == {
        "dateTime": "2001-05-07T12:00:00+02:00",
        "fileFormatVersion": "32.615 V4.0",
        "senderName": "DC=a1.companyNN.com,SubNetwork=1,IRPAgent=1",
        "vendorName": "Company NN",
    }

    # verify metadata writen by parse to yaml file
    with open("data/bulkcm_with_header_footer_metadata.yml", "r") as yaml_file:
        metadata = yaml.load(yaml_file, Loader=yaml.FullLoader)
        assert metadata == {
            "dateTime": "2001-05-07T12:00:00+02:00",
            "fileFormatVersion": "32.615 V4.0",
            "senderName": "DC=a1.companyNN.com,SubNetwork=1,IRPAgent=1",
            "vendorName": "Company NN",
        }

    with open(
        "data/ManagedElement-2ad0c41c0141dad76eae0425d952470c.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == [
            "node_id",
            "node_key",
            "managedElementType",
            "userLabel",
            "vendorName",
            "userDefinedState",
            "locationName",
        ]
        assert list(reader) == [
            {
                "node_id": "1",
                "node_key": "[{'SubNetwork': '1'}, {'ManagedElement': '1'}]",
                "managedElementType": "RNC",
                "userLabel": "Paris RN1",
                "vendorName": "Company NN",
                "userDefinedState": "commercial",
                "locationName": "Champ de Mars",
            },
            {
                "node_id": "2",
                "node_key": "[{'SubNetwork': '1'}, {'ManagedElement': '2'}]",
                "managedElementType": "RNC",
                "userLabel": "Paris RN2",
                "vendorName": "Company NN",
                "userDefinedState": "commercial",
                "locationName": "Concorde",
            },
        ]

    with open(
        "data/ManagementNode-bcbc45d45148e76ee80ad0ce9110eac7.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == [
            "node_id",
            "node_key",
            "userLabel",
            "vendorName",
            "userDefinedState",
            "locationName",
        ]
        assert list(reader) == [
            {
                "node_id": "1",
                "node_key": "[{'SubNetwork': '1'}, {'ManagementNode': '1'}]",
                "userLabel": "Paris MN1",
                "vendorName": "Company NN",
                "userDefinedState": "commercial",
                "locationName": "Montparnasse",
            }
        ]

    with open(
        "data/SubNetwork-05edfb4b65ec197423ff273ef30ddaff.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == [
            "node_id",
            "node_key",
            "userLabel",
            "userDefinedNetworkType",
        ]
        assert list(reader) == [
            {
                "node_id": "1",
                "node_key": "[{'SubNetwork': '1'}]",
                "userLabel": "Paris SN1",
                "userDefinedNetworkType": "UMTS",
            }
        ]

    stream = bulkcm.BulkCmParser.stream_to_csv("data")
    metadata, duration = bulkcm.parse(
        "data/bulkcm_with_vsdatacontainer.xml", "data", stream
    )

    with open(
        "data/SubNetwork-e3c968f12ec1ae219a7e2f9d7829a67d.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == ["node_id", "node_key"]
        assert list(reader) == [{"node_id": "1", "node_key": "[{'SubNetwork': '1'}]"}]

    with open(
        "data/ManagedElement-e3c968f12ec1ae219a7e2f9d7829a67d.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == ["node_id", "node_key"]
        assert list(reader) == [
            {"node_id": "1", "node_key": "[{'SubNetwork': '1'}, {'ManagedElement': '1'}]"}
        ]

    with open(
        "data/RncFunction-e3c968f12ec1ae219a7e2f9d7829a67d.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == ["node_id", "node_key"]
        assert list(reader) == [
            {
                "node_id": "1",
                "node_key": "[{'SubNetwork': '1'}, {'ManagedElement': '1'}, {'RncFunction': '1'}]",
            }
        ]

    with open(
        "data/vsDataRncHandOver-430a850294cd3dd7ab5fac2a6e8b8c75.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == ["node_id", "node_key", "abcMin", "abcMax"]
        assert list(reader) == [
            {
                "node_id": "1",
                "node_key": "[{'SubNetwork': '1'}, {'ManagedElement': '1'}, {'RncFunction': '1'}, {'vsDataRncHandOver': '1'}]",
                "abcMin": "12",
                "abcMax": "34",
            }
        ]

    stream = bulkcm.BulkCmParser.stream_to_csv("data")
    metadata, duration = bulkcm.parse("data/bulkcm_with_utrancell.xml", "data", stream)

    with open(
        "data/vsDataUtranCell-58abad5af457eb77f98a476b59a0e146.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == ["node_id", "node_key", "sc", "pcpichpower"]
        assert list(reader) == [
            {
                "node_id": "Cell1",
                "node_key": "[{'SubNetwork': '1'}, {'ManagedElement': '1'}, {'RncFunction': '1'}, {'vsDataUtranCell': 'Cell1'}]",
                "sc": "111",
                "pcpichpower": "222",
            }
        ]

    with open(
        "data/vsDataRncHandOver-430a850294cd3dd7ab5fac2a6e8b8c75.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == ["node_id", "node_key", "abcMin", "abcMax"]
        assert list(reader) == [
            {
                "node_id": "1",
                "node_key": "[{'SubNetwork': '1'}, {'ManagedElement': '1'}, {'RncFunction': '1'}, {'vsDataUtranCell': 'Cell1'}, {'vsDataRncHandOver': '1'}]",
                "abcMin": "12",
                "abcMax": "34",
            }
        ]
