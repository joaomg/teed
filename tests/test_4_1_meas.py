import os
import csv

from teed import meas


def test_meas_parse_program():

    pathname = "data/mdc*.xml"
    output_dir = "data"
    recursive = False

    try:
        os.remove("data/UtranCell-900-9995823c30bcf308b91ab0b66313e86a.csv")
    except FileNotFoundError:
        pass

    meas.parse(pathname, output_dir, recursive)

    with open(
        "data/UtranCell-900-9995823c30bcf308b91ab0b66313e86a.csv", newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)
        assert reader.fieldnames == [
            "ST",
            "NEDN",
            "LDN",
            "attTCHSeizures",
            "succTCHSeizures",
            "attImmediateAssignProcs",
            "succImmediateAssignProcs",
        ]

        assert list(reader) == [
            {
                "ST": "20000301140000",
                "NEDN": "DC=a1.companyNN.com,SubNetwork=1,IRPAgent=1,SubNetwork=CountryNN,MeContext=MEC-Gbg1,ManagedElement=RNC-Gbg-1",
                "LDN": "RncFunction=RF-1,UtranCell=Gbg-997",
                "attTCHSeizures": "234",
                "succTCHSeizures": "345",
                "attImmediateAssignProcs": "567",
                "succImmediateAssignProcs": "789",
            },
            {
                "ST": "20000301140000",
                "NEDN": "DC=a1.companyNN.com,SubNetwork=1,IRPAgent=1,SubNetwork=CountryNN,MeContext=MEC-Gbg1,ManagedElement=RNC-Gbg-1",
                "LDN": "RncFunction=RF-1,UtranCell=Gbg-998",
                "attTCHSeizures": "890",
                "succTCHSeizures": "901",
                "attImmediateAssignProcs": "123",
                "succImmediateAssignProcs": "234",
            },
            {
                "ST": "20000301140000",
                "NEDN": "DC=a1.companyNN.com,SubNetwork=1,IRPAgent=1,SubNetwork=CountryNN,MeContext=MEC-Gbg1,ManagedElement=RNC-Gbg-1",
                "LDN": "RncFunction=RF-1,UtranCell=Gbg-999",
                "attTCHSeizures": "456",
                "succTCHSeizures": "567",
                "attImmediateAssignProcs": "678",
                "succImmediateAssignProcs": "789",
            },
            {
                "ST": "20000301140000",
                "NEDN": "DC=a1.companyNN.com,SubNetwork=1,IRPAgent=1,SubNetwork=CountryNN,MeContext=MEC-Gbg1,ManagedElement=RNC-Gbg-1",
                "LDN": "RncFunction=RF-1,UtranCell=Gbg-997",
                "attTCHSeizures": "234",
                "succTCHSeizures": "345",
                "attImmediateAssignProcs": "567",
                "succImmediateAssignProcs": "789",
            },
            {
                "ST": "20000301140000",
                "NEDN": "DC=a1.companyNN.com,SubNetwork=1,IRPAgent=1,SubNetwork=CountryNN,MeContext=MEC-Gbg1,ManagedElement=RNC-Gbg-1",
                "LDN": "RncFunction=RF-1,UtranCell=Gbg-998",
                "attTCHSeizures": "890",
                "succTCHSeizures": "901",
                "attImmediateAssignProcs": "123",
                "succImmediateAssignProcs": "234",
            },
            {
                "ST": "20000301140000",
                "NEDN": "DC=a1.companyNN.com,SubNetwork=1,IRPAgent=1,SubNetwork=CountryNN,MeContext=MEC-Gbg1,ManagedElement=RNC-Gbg-1",
                "LDN": "RncFunction=RF-1,UtranCell=Gbg-999",
                "attTCHSeizures": "456",
                "succTCHSeizures": "567",
                "attImmediateAssignProcs": "678",
                "succImmediateAssignProcs": "789",
            },
        ]

    # test consume_ldn_natural_key
    # this consumer produces tabular
    # like CSV files, where the NEDN and LDN
    # are split into it's parts
    # the CSV size is much smaller
    # but the parser takes longer to run
    pathname = "data/mdc_c3_1.xml"
    output_dir = "data"
    recursive = False

    try:
        os.remove("data/UtranCell-900-5ff3d8a40d18614e53848f10f7a233c7.csv")
    except FileNotFoundError:
        pass

    meas.parse(
        pathname, output_dir, recursive, consume_target=meas.consume_ldn_natural_key
    )

    with open(
        "data/UtranCell-900-5ff3d8a40d18614e53848f10f7a233c7.csv", newline=""
    ) as csv_file:
        reader = csv.reader(csv_file)

        assert list(reader) == [
            [
                "ST",
                "DC",
                "SubNetwork",
                "IRPAgent",
                "SubNetwork",
                "MeContext",
                "ManagedElement",
                "RncFunction",
                "UtranCell",
                "attTCHSeizures",
                "succTCHSeizures",
                "attImmediateAssignProcs",
                "succImmediateAssignProcs",
            ],
            [
                "20000301140000",
                "a1.companyNN.com",
                "1",
                "1",
                "CountryNN",
                "MEC-Gbg1",
                "RNC-Gbg-1",
                "RF-1",
                "Gbg-997",
                "234",
                "345",
                "567",
                "789",
            ],
            [
                "20000301140000",
                "a1.companyNN.com",
                "1",
                "1",
                "CountryNN",
                "MEC-Gbg1",
                "RNC-Gbg-1",
                "RF-1",
                "Gbg-998",
                "890",
                "901",
                "123",
                "234",
            ],
            [
                "20000301140000",
                "a1.companyNN.com",
                "1",
                "1",
                "CountryNN",
                "MEC-Gbg1",
                "RNC-Gbg-1",
                "RF-1",
                "Gbg-999",
                "456",
                "567",
                "678",
                "789",
            ],
        ]
