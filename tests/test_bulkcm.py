import os
import csv
from lxml import etree
from teed import bulkcm


def test_probe():
    """ Test bulkcm.probe """

    assert bulkcm.probe("data/bulkcm.xml") == {
        "SubNetwork(s)": ["SubNetwork 1 counting #2 xn:ManagedElement"],
        "dnPrefix": "DC=a1.companyNN.com",
    }


def test_split_by_subnetwork():
    """ Test bulkcm.split_by_subnetwork """

    for sn_id, sn in bulkcm.split_by_subnetwork("data/bulkcm.xml"):
        assert sn_id == "1"
        assert isinstance(sn, (etree._ElementTree))


def test_split_by_subnetwork_to_file():
    """ Test bulkcm.split_by_subnetwork_to_file """

    # remove tests/bulkcm_SubNetwork_1.xml if exists
    try:
        os.remove("tests/bulkcm_SubNetwork_1.xml")
    except FileNotFoundError:
        pass

    # creating the new tests/bulkcm_SubNetwork_1.xml
    for output_file_path in bulkcm.split_by_subnetwork_to_file(
        "data/bulkcm.xml", "tests"
    ):

        # check if tests/bulkcm_SubNetwork_1.xml exists
        assert os.path.exists(output_file_path)

    # compare contents with the input
    # they must be the same since there's
    # only a SubNetwork in bulkcm.xml
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
    target = etree.parse(output_file_path, parser=parser)

    assert etree.tostring(source) == etree.tostring(target)


def test_parse():
    """ Test bulkcm.parse """

    bulkcm.to_csv("data/bulkcm.xml", "data")
    bulkcm.to_csv("data/bulkcm_empty.xml", "data")
    bulkcm.to_csv("data/bulkcm_no_configData.xml", "data")

    metadata, nodes = bulkcm.to_csv("data/bulkcm_with_header_footer.xml", "data")

    assert metadata == {
        "dateTime": "2001-05-07T12:00:00+02:00",
        "fileFormatVersion": "32.615 V4.0",
        "senderName": "DC=a1.companyNN.com,SubNetwork=1,IRPAgent=1",
        "vendorName": "Company NN",
    }

    assert nodes == [
        {
            "node_id": "1",
            "node_name": "SubNetwork",
            "userDefinedNetworkType": "UMTS",
            "userLabel": "Paris SN1",
        },
        {
            "locationName": "Montparnasse",
            "node_id": "1",
            "node_name": "ManagementNode",
            "userDefinedState": "commercial",
            "userLabel": "Paris MN1",
            "vendorName": "Company NN",
        },
        {
            "locationName": "Champ de Mars",
            "managedElementType": "RNC",
            "node_id": "1",
            "node_name": "ManagedElement",
            "userDefinedState": "commercial",
            "userLabel": "Paris RN1",
            "vendorName": "Company NN",
        },
        {
            "locationName": "Concorde",
            "managedElementType": "RNC",
            "node_id": "2",
            "node_name": "ManagedElement",
            "userDefinedState": "commercial",
            "userLabel": "Paris RN2",
            "vendorName": "Company NN",
        },
    ]

    with open("data/ManagedElement.csv", newline="") as csv_file:
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

    with open("data/ManagementNode.csv", newline="") as csv_file:
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

    with open("data/SubNetwork.csv", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == ["node_id", "userLabel", "userDefinedNetworkType"]
        assert list(reader) == [
            {"node_id": "1", "userLabel": "Paris SN1", "userDefinedNetworkType": "UMTS"}
        ]
