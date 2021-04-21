import csv
import hashlib
import os
from multiprocessing import Lock, Queue
from os import path
from queue import Empty

from teed import meas


def test_meas_parse():
    """Test meas.parse"""

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


def test_meas_parse_consume_ldn_natural_key():
    """
    Test consume_ldn_natural_key
    this consumer produces tabular
    like CSV files, where the NEDN and LDN
    are split into it's parts
    the CSV size is much smaller
    but the parser takes longer to run
    """

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


def test_meas_parse_consume_custom():
    """Plug a custom consume method into meas.parse"""

    def my_custom_consume(queue: Queue, lock: Lock, output_dir: str):
        """Identical to consume_ldn_natural_key consumer method.

        But simplified to create a subdirectory inside output_dir per ManagedElement

        And ignore the NEDN.

        The consumer implements ManagedElement partition creation.

        And serves as an example for creating custom consumers.

        Depending on the incoming data, a data engineer, can customize the CSV output.

        Making it quicker, smaller in the disk and appropriate for the subsequent data pipeline.

        To achive this the data engineer must have knowledge of the data and how the NEDN and LDN are built in the network.

        In order to create simplified object keys.
        """

        writers = {}  # maps the node_key to it's writer

        with lock:
            print(f"Consumer starting {os.getpid()}")

        while True:
            try:
                item = queue.get(block=True, timeout=0.05)

                # exit while loop on receiving DONE item
                if item == "DONE":
                    break

                if item == "STOP":
                    with lock:
                        print("Stop received!")

                    break

                # DC=a1.companyNN.com,SubNetwork=1,IRPAgent=1,SubNetwork=CountryNN,MeContext=MEC-Gbg1,ManagedElement=RNC-Gbg-1
                nedn = item["rows"][0][1]
                managed_element = nedn[nedn.find("ManagedElement=") :]

                # RncFunction=RF-1,UtranCell=Gbg-997
                ldn = item["rows"][0][2]
                ldn_list = eval(f"""['{ldn.replace(",","','").replace("=","','")}']""")

                ldn_keys = [item for i, item in enumerate(ldn_list) if i % 2 == 0]
                columns_keys = ldn_keys
                columns_values = item["mts"]
                gp = item["gp"]

                table_name = columns_keys[-1]
                table_hash = hashlib.md5(
                    "".join(columns_keys + columns_values).encode()
                ).hexdigest()
                table_key = f"{table_name}_{gp}_{table_hash}"

                csv_path = path.normpath(
                    f"{output_dir}{path.sep}{managed_element}{path.sep}{table_name}-{gp}-{table_hash}.csv"
                )

                if not (path.exists(csv_path)):
                    # create ManagedElement directory
                    dir_path = f"{output_dir}{path.sep}{managed_element}"
                    os.mkdir(dir_path)

                    # create new file
                    csv_file = open(csv_path, mode="w", newline="")

                    msg = f"Created {csv_path}"
                    with lock:
                        print(msg)

                    writer = csv.writer(csv_file)
                    # ST = measurement start time
                    # NEDN = network element distinguished name
                    # LDN = measured object distinguished name, within the context of the NEDN
                    header = ["ST"] + columns_keys + columns_values
                    writer.writerow(header)

                    writers[table_key] = writer

                elif table_key not in writers:
                    # append to end of file
                    csv_file = open(csv_path, mode="a", newline="")

                    msg = f"Append {csv_path}"
                    with lock:
                        print(msg)

                    writer = csv.writer(csv_file)
                    writers[table_key] = writer

                else:
                    # file and writer exist
                    # get previously created writer
                    writer = writers.get(table_key)

                # serialize rows to csv file
                for row in item["rows"]:
                    st = row.pop(0)
                    # we're removing the nedn and ignoring it, we're partitioning the output per ManagedElement
                    nedn = row.pop(0)
                    ldn = row.pop(0)
                    ldn_list = eval(
                        f"""['{ldn.replace(",","','").replace("=","','")}']"""
                    )
                    ldn_values = [item for i, item in enumerate(ldn_list) if i % 2 != 0]
                    writer.writerow([st] + ldn_values + row)

                # flush the data to disk
                csv_file.flush()

            except KeyboardInterrupt:
                with lock:
                    print("KeyboardInterrupt received, stopping!")

                if csv_file:
                    csv_file.flush()

                break

            except Empty:
                continue

    pathname = "data/mdc_c3_1.xml"
    output_dir = "data"
    recursive = False

    try:
        os.remove(
            "data/ManagedElement=RNC-Gbg-1/UtranCell-900-9250b00755cdcfa28421b7ddb6f76666.csv"
        )
        os.rmdir("data/ManagedElement=RNC-Gbg-1")
    except FileNotFoundError:
        pass

    meas.parse(pathname, output_dir, recursive, consume_target=my_custom_consume)

    with open(
        "data/ManagedElement=RNC-Gbg-1/UtranCell-900-9250b00755cdcfa28421b7ddb6f76666.csv",
        newline="",
    ) as csv_file:
        reader = csv.reader(csv_file)

        assert list(reader) == [
            [
                "ST",
                "RncFunction",
                "UtranCell",
                "attTCHSeizures",
                "succTCHSeizures",
                "attImmediateAssignProcs",
                "succImmediateAssignProcs",
            ],
            [
                "20000301140000",
                "RF-1",
                "Gbg-997",
                "234",
                "345",
                "567",
                "789",
            ],
            [
                "20000301140000",
                "RF-1",
                "Gbg-998",
                "890",
                "901",
                "123",
                "234",
            ],
            [
                "20000301140000",
                "RF-1",
                "Gbg-999",
                "456",
                "567",
                "678",
                "789",
            ],
        ]
