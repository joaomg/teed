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
            "DN",
            "attTCHSeizures",
            "succTCHSeizures",
            "attImmediateAssignProcs",
            "succImmediateAssignProcs",
        ]

        assert list(reader) == [
            {
                "ST": "20000301140000",
                "DN": "DC=a1.companyNN.com,SubNetwork=1,IRPAgent=1,SubNetwork=CountryNN,MeContext=MEC-Gbg1,ManagedElement=RNC-Gbg-1,RncFunction=RF-1,UtranCell=Gbg-997",
                "attTCHSeizures": "234",
                "succTCHSeizures": "345",
                "attImmediateAssignProcs": "567",
                "succImmediateAssignProcs": "789",
            },
            {
                "ST": "20000301140000",
                "DN": "DC=a1.companyNN.com,SubNetwork=1,IRPAgent=1,SubNetwork=CountryNN,MeContext=MEC-Gbg1,ManagedElement=RNC-Gbg-1,RncFunction=RF-1,UtranCell=Gbg-998",
                "attTCHSeizures": "890",
                "succTCHSeizures": "901",
                "attImmediateAssignProcs": "123",
                "succImmediateAssignProcs": "234",
            },
            {
                "ST": "20000301140000",
                "DN": "DC=a1.companyNN.com,SubNetwork=1,IRPAgent=1,SubNetwork=CountryNN,MeContext=MEC-Gbg1,ManagedElement=RNC-Gbg-1,RncFunction=RF-1,UtranCell=Gbg-999",
                "attTCHSeizures": "456",
                "succTCHSeizures": "567",
                "attImmediateAssignProcs": "678",
                "succImmediateAssignProcs": "789",
            },
            {
                "ST": "20000301140000",
                "DN": "DC=a1.companyNN.com,SubNetwork=1,IRPAgent=1,SubNetwork=CountryNN,MeContext=MEC-Gbg1,ManagedElement=RNC-Gbg-1,RncFunction=RF-1,UtranCell=Gbg-997",
                "attTCHSeizures": "234",
                "succTCHSeizures": "345",
                "attImmediateAssignProcs": "567",
                "succImmediateAssignProcs": "789",
            },
            {
                "ST": "20000301140000",
                "DN": "DC=a1.companyNN.com,SubNetwork=1,IRPAgent=1,SubNetwork=CountryNN,MeContext=MEC-Gbg1,ManagedElement=RNC-Gbg-1,RncFunction=RF-1,UtranCell=Gbg-998",
                "attTCHSeizures": "890",
                "succTCHSeizures": "901",
                "attImmediateAssignProcs": "123",
                "succImmediateAssignProcs": "234",
            },
            {
                "ST": "20000301140000",
                "DN": "DC=a1.companyNN.com,SubNetwork=1,IRPAgent=1,SubNetwork=CountryNN,MeContext=MEC-Gbg1,ManagedElement=RNC-Gbg-1,RncFunction=RF-1,UtranCell=Gbg-999",
                "attTCHSeizures": "456",
                "succTCHSeizures": "567",
                "attImmediateAssignProcs": "678",
                "succImmediateAssignProcs": "789",
            },
        ]
