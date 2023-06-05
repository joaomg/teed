import csv
import os

import pyarrow.fs as fs
import yaml
from lxml import etree

from teed import bulkcm


def test_probe():
    """Test bulkcm.probe"""

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
    """Test bulkcm.split"""

    ofs = fs.LocalFileSystem()

    # remove tests/bulkcm_SubNetwork_1.xml if exists
    try:
        ofs.delete_file("tests/bulkcm_1.xml")
    except FileNotFoundError:
        pass

    # split bulkcm.xml
    # ignore SubNetwork with id "1"
    for sn_id, sn_file_path in bulkcm.split(
        "data/bulkcm.xml", "tests", subnetworks=["dummyNetwork"], output_fs=ofs
    ):
        assert sn_id == "1"
        assert sn_file_path is None
        assert not (os.path.exists("tests/bulkcm_1.xml"))

    # split bulkcm.xml
    # considered SubNetwork with id "1"
    for sn_id, sn_file_path in bulkcm.split(
        "data/bulkcm.xml", "tests", subnetworks=["1"], output_fs=ofs
    ):
        assert sn_id == "1"
        assert sn_file_path == "tests/bulkcm_1.xml"
        assert os.path.exists(sn_file_path)

    # split bulkcm.xml
    # considered all/any SubNetwork
    for sn_id, sn_file_path in bulkcm.split("data/bulkcm.xml", "tests", output_fs=ofs):
        assert sn_id == "1"
        assert sn_file_path == "tests/bulkcm_1.xml"
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


def test_parse_output_to_csv_local_filesystem():
    """Test bulkcm.parse"""

    ofs = fs.LocalFileSystem()

    # remove all .csv and .yml files from data
    try:
        import glob

        for csv_file in glob.iglob("data/*.csv"):
            ofs.delete_file(csv_file)

        for csv_file in glob.iglob("data/*.yml"):
            ofs.delete_file(csv_file)
    except FileNotFoundError:
        pass

    stream = bulkcm.BulkCmParser.stream_to_csv("data")
    bulkcm.parse("data/bulkcm.xml", "data", stream, output_fs=ofs)

    try:
        stream = bulkcm.BulkCmParser.stream_to_csv("data")
        bulkcm.parse("data/bulkcm_empty.xml", "data", stream, output_fs=ofs)
    except Exception as e:
        assert str(e) == "Document is empty, line 1, column 1 (bulkcm_empty.xml, line 1)"

    stream = bulkcm.BulkCmParser.stream_to_csv("data")
    bulkcm.parse("data/bulkcm_no_configData.xml", "data", stream, output_fs=ofs)

    stream = bulkcm.BulkCmParser.stream_to_csv("data")
    metadata, duration = bulkcm.parse(
        "data/bulkcm_with_header_footer.xml", "data", stream, output_fs=ofs
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
        "data/ManagedElement-2ce5d8fae91842f854b00844e05fdd6b.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == [
            "SubNetwork",
            "ManagedElement",
            "managedElementType",
            "userLabel",
            "vendorName",
            "userDefinedState",
            "locationName",
        ]
        assert list(reader) == [
            {
                "SubNetwork": "1",
                "ManagedElement": "1",
                "managedElementType": "RNC",
                "userLabel": "Paris RN1",
                "vendorName": "Company NN",
                "userDefinedState": "commercial",
                "locationName": "Champ de Mars",
            },
            {
                "SubNetwork": "1",
                "ManagedElement": "2",
                "managedElementType": "RNC",
                "userLabel": "Paris RN2",
                "vendorName": "Company NN",
                "userDefinedState": "commercial",
                "locationName": "Concorde",
            },
        ]

    with open(
        "data/ManagementNode-cb742e095d2f7bda9622720bf9237682.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == [
            "SubNetwork",
            "ManagementNode",
            "userLabel",
            "vendorName",
            "userDefinedState",
            "locationName",
        ]
        assert list(reader) == [
            {
                "SubNetwork": "1",
                "ManagementNode": "1",
                "userLabel": "Paris MN1",
                "vendorName": "Company NN",
                "userDefinedState": "commercial",
                "locationName": "Montparnasse",
            }
        ]

    with open(
        "data/SubNetwork-7c3cf0dd0368151b3df3dea6e9ec46ff.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == [
            "SubNetwork",
            "userLabel",
            "userDefinedNetworkType",
        ]
        assert list(reader) == [
            {
                "SubNetwork": "1",
                "userLabel": "Paris SN1",
                "userDefinedNetworkType": "UMTS",
            }
        ]

    # exlude all elements -> nothing is processed
    try:
        os.remove("data/vsDataUtranCell-762627b0939d1ac04dadef2b58f194c1.csv")
        os.remove("data/vsDataRncHandOver-430a850294cd3dd7ab5fac2a6e8b8c75.csv")
        os.remove("data/SubNetwork-e3c968f12ec1ae219a7e2f9d7829a67d.csv")
        os.remove("data/ManagedElement-e3c968f12ec1ae219a7e2f9d7829a67d.csv")
        os.remove("data/RncFunction-e3c968f12ec1ae219a7e2f9d7829a67d.csv")
    except FileNotFoundError:
        pass

    stream = bulkcm.BulkCmParser.stream_to_csv("data")
    metadata, duration = bulkcm.parse(
        "data/bulkcm_with_utrancell.xml",
        "data",
        stream,
        exclude_elements=["*"],
        output_fs=ofs,
    )

    assert not (
        os.path.exists("data/vsDataUtranCell-762627b0939d1ac04dadef2b58f194c1.csv")
    )
    assert not (
        os.path.exists("data/vsDataRncHandOver-430a850294cd3dd7ab5fac2a6e8b8c75.csv")
    )
    assert not (os.path.exists("data/SubNetwork-e3c968f12ec1ae219a7e2f9d7829a67d.csv"))
    assert not (
        os.path.exists("data/ManagedElement-e3c968f12ec1ae219a7e2f9d7829a67d.csv")
    )
    assert not (os.path.exists("data/RncFunction-e3c968f12ec1ae219a7e2f9d7829a67d.csv"))

    # exlude all elements except vsDataUtranCell
    try:
        os.remove("data/vsDataUtranCell-762627b0939d1ac04dadef2b58f194c1.csv")
        os.remove("data/vsDataRncHandOver-430a850294cd3dd7ab5fac2a6e8b8c75.csv")
        os.remove("data/SubNetwork-e3c968f12ec1ae219a7e2f9d7829a67d.csv")
        os.remove("data/ManagedElement-e3c968f12ec1ae219a7e2f9d7829a67d.csv")
        os.remove("data/RncFunction-e3c968f12ec1ae219a7e2f9d7829a67d.csv")
    except FileNotFoundError:
        pass

    stream = bulkcm.BulkCmParser.stream_to_csv("data")
    metadata, duration = bulkcm.parse(
        "data/bulkcm_with_utrancell.xml",
        "data",
        stream,
        include_elements=["vsDataUtranCell"],
        exclude_elements=["*"],
        output_fs=ofs,
    )

    assert os.path.exists("data/vsDataUtranCell-762627b0939d1ac04dadef2b58f194c1.csv")
    with open(
        "data/vsDataUtranCell-762627b0939d1ac04dadef2b58f194c1.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == [
            "SubNetwork",
            "ManagedElement",
            "RncFunction",
            "vsDataUtranCell",
            "sc",
            "pcpichpower",
        ]
        assert list(reader) == [
            {
                "SubNetwork": "1",
                "ManagedElement": "2",
                "RncFunction": "3",
                "vsDataUtranCell": "Cell4",
                "sc": "111",
                "pcpichpower": "222",
            }
        ]

    assert not (
        os.path.exists("data/vsDataRncHandOver-430a850294cd3dd7ab5fac2a6e8b8c75.csv")
    )
    assert not (os.path.exists("data/SubNetwork-e3c968f12ec1ae219a7e2f9d7829a67d.csv"))
    assert not (
        os.path.exists("data/ManagedElement-e3c968f12ec1ae219a7e2f9d7829a67d.csv")
    )
    assert not (os.path.exists("data/RncFunction-e3c968f12ec1ae219a7e2f9d7829a67d.csv"))

    stream = bulkcm.BulkCmParser.stream_to_csv("data")
    metadata, duration = bulkcm.parse(
        "data/bulkcm_with_vsdatacontainer.xml", "data", stream, output_fs=ofs
    )

    with open(
        "data/SubNetwork-97cdbf39c39074db55f07d505908bc4c.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == ["SubNetwork"]
        assert list(reader) == [{"SubNetwork": "1"}]

    with open(
        "data/ManagedElement-788834423bcf73489d85c4e44a093dc4.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == ["SubNetwork", "ManagedElement"]
        assert list(reader) == [{"SubNetwork": "1", "ManagedElement": "2"}]

    with open(
        "data/RncFunction-7e9e4244ee9d918874b4a371bc4fe70f.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == ["SubNetwork", "ManagedElement", "RncFunction"]
        assert list(reader) == [
            {"SubNetwork": "1", "ManagedElement": "2", "RncFunction": "3"}
        ]

    with open(
        "data/vsDataRncHandOver-945526f2dd4bbe6df48bd5893ef37fbf.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == [
            "SubNetwork",
            "ManagedElement",
            "RncFunction",
            "vsDataRncHandOver",
            "abcMin",
            "abcMax",
        ]
        assert list(reader) == [
            {
                "SubNetwork": "1",
                "ManagedElement": "2",
                "RncFunction": "3",
                "vsDataRncHandOver": "4",
                "abcMin": "12",
                "abcMax": "34",
            }
        ]

    stream = bulkcm.BulkCmParser.stream_to_csv("data")
    metadata, duration = bulkcm.parse(
        "data/bulkcm_with_utrancell.xml", "data", stream, output_fs=ofs
    )

    with open(
        "data/vsDataUtranCell-762627b0939d1ac04dadef2b58f194c1.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == [
            "SubNetwork",
            "ManagedElement",
            "RncFunction",
            "vsDataUtranCell",
            "sc",
            "pcpichpower",
        ]
        assert list(reader) == [
            {
                "SubNetwork": "1",
                "ManagedElement": "2",
                "RncFunction": "3",
                "vsDataUtranCell": "Cell4",
                "sc": "111",
                "pcpichpower": "222",
            }
        ]

    with open(
        "data/vsDataRncHandOver-8ebadf34e92ad0fb468fd0bed0aab89d.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == [
            "SubNetwork",
            "ManagedElement",
            "RncFunction",
            "vsDataUtranCell",
            "vsDataRncHandOver",
            "abcMin",
            "abcMax",
        ]
        assert list(reader) == [
            {
                "SubNetwork": "1",
                "ManagedElement": "2",
                "RncFunction": "3",
                "vsDataUtranCell": "Cell4",
                "vsDataRncHandOver": "5",
                "abcMin": "12",
                "abcMax": "34",
            }
        ]
