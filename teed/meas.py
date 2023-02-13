# python -m teed meas parse data/mdc_c3_1.xml data
# python -m teed meas parse data/meas_c4_1.xml data
# python -m teed meas parse "data/mdc*xml" data
# python -m teed meas parse "data/meas*xml" data

# python -m teed meas parse "benchmark/eric_rnc/A*xml" tmp
# Producer and consumer done, exiting.
# Duration(s): 20.945605682000178
# python -m teed meas parse "benchmark/eric_nodeb/A*xml" tmp
# Producer and consumer done, exiting.
# Duration(s): 15.023600272999829
# python -m teed meas parse "benchmark/*/A*xml" tmp --recursive
# Producer and consumer done, exiting.
# Duration(s): 35.324785770999824
# Using top we've seen low memory usage, even though two python processes are used: producer and consumer


import csv
import glob
import hashlib
from collections import OrderedDict
import os
import signal
import time
from datetime import datetime, timedelta
from multiprocessing import Lock, Process, Queue
from os import path
from queue import Empty
import typer

import pyarrow as pa
import pyarrow.dataset as ds


# import yaml
from lxml import etree

from teed import TeedException

program = typer.Typer()


def produce(queue: Queue, lock: Lock, pathname: str, recursive=False):
    """Fetch Meas/Mdc files from pathname glob and parse

    For each measData/md element create a table item

    and place it in the queue.

    Optionally search in the pathname subdirectories.
    """

    with lock:
        print(f"Producer starting {os.getpid()}")

    for file_path in glob.iglob(pathname, recursive=recursive):
        with open(file_path, mode="rb") as stream:
            with lock:
                print(f"Parsing {file_path}")

            metadata = {"file_path": file_path}

            for event, element in etree.iterparse(
                stream,
                events=("end",),
                tag=(
                    "{*}mfh",
                    "{*}md",
                    "{*}mff",
                ),
                no_network=True,
                remove_blank_text=True,
                remove_comments=True,
                remove_pis=True,
                huge_tree=True,
                recover=False,
            ):
                localName = etree.QName(element.tag).localname

                if localName == "md":
                    # <md>
                    #     <neid>
                    #         <neun>RNC Telecomville</neun>
                    #         <nedn>DC=a1.companyNN.com,SubNetwork=1,IRPAgent=1,SubNetwork=CountryNN,MeContext=MEC-Gbg1,ManagedElement=RNC-Gbg-1</nedn>
                    #     </neid>
                    #     <mi>
                    #     ...
                    neid = element.find("neid")
                    if neid is not None:
                        # neun = neid.find("neun").text
                        nedn = neid.find("nedn").text

                    for mi in element.iterfind("mi"):
                        table = {}
                        # <mi>
                        #   <mts>20210301141430</mts>
                        #   <gp>900</gp>
                        ts = mi.find("mts").text[:14]
                        gp = mi.find("gp").text

                        datetime_ts = datetime.strptime(ts, "%Y%m%d%H%M%S")
                        time_gp = timedelta(seconds=float(gp))
                        meas_ts = (datetime_ts - time_gp).strftime("%Y%m%d%H%M%S")

                        table["ts"] = ts
                        table["gp"] = gp

                        # ...
                        # <mt>attTCHSeizures</mt>
                        # <mt>succTCHSeizures</mt>
                        # <mt>attImmediateAssignProcs</mt>
                        # <mt>succImmediateAssignProcs</mt>
                        # ...
                        mts = []
                        for mt in mi.iterfind("mt"):
                            mts.append(mt.text)

                        table["mts"] = mts

                        # ...
                        # <mv>
                        #     <moid>RncFunction=RF-1,UtranCell=Gbg-997</moid>
                        #     <r>234</r>
                        #     <r>345</r>
                        #     <r>567</r>
                        #     <r>789</r>
                        # </mv>
                        # ...
                        table["rows"] = []
                        for mv in mi.iterfind("mv"):
                            ldn = mv.find("moid").text
                            row = [meas_ts, nedn, ldn]
                            for r in mv.iterfind("r"):
                                row.append(r.text)

                            table["rows"].append(row)

                        # if there're rows in table
                        # place it in the queue
                        if table["rows"] != [] and mts != []:
                            table_name = (ldn.split(",")[-1]).split("=")[0]  # UtranCell
                            with lock:
                                print(f"Placing {table_name}")

                            queue.put(table)
                        else:
                            # ignoring this mi
                            with lock:
                                print("Warning, ignoring mi element due to lack of data")
                                print(f"Number of mt's: #{len(mts)}")
                                print(f"First mt: {mts[0]}")

                elif localName == "mfh":
                    # <mfh>
                    #     <ffv>32.401 V5.0</ffv>
                    #     <sn>DC=a1.companyNN.com,SubNetwork=1,IRPAgent=1,SubNetwork=CountryNN,MeContext=MEC-Gbg1,ManagedElement=RNC-Gbg-1</sn>
                    #     <st>RNC</st>
                    #     <vn>Company NN</vn>
                    #     <cbt>20210301141500</cbt>
                    # </mfh>
                    metadata["encoding"] = (element.getroottree()).docinfo.encoding

                    for child in element:
                        metadata[child.tag] = child.text

                elif localName == "mff":
                    # <mff>
                    #   <ts>20210301143000</ts>
                    # </mff>
                    for child in element:
                        metadata[child.tag] = child.text

                element.clear(keep_tail=False)

    # place a DONE signal in the queue
    # the consumer will continue to execute
    # until this item/signal is received
    queue.put("DONE")


def consume_to_csv(queue: Queue, lock: Lock, output_dir: str):
    """Serialize tables received from queue to CSV file.

    Place the CSV file in the output dir (output_dir).

    Create file if doesn't exist and appends data.

    Take notice: it doesn't delete the file previously to the serialization.

    The CSV contain at least three columns, in this exact order: ST, NEDN and LDN.

    ST = measurement start time (YYYYMMDDHHMMSS)
    NEDN = network element distinguished name (A=a,B=b,C=c)
    LDN = measured object distinguished name, within the context of the NEDN (A=a,B=b,C=c)
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

            moid = item["rows"][0][2]  # RncFunction=RF-1,UtranCell=Gbg-997
            table_name = (moid.split(",")[-1]).split("=")[0]  # UtranCell
            columns_values = item["mts"]
            gp = item["gp"]

            table_hash = hashlib.md5("".join(columns_values).encode()).hexdigest()
            table_key = f"{table_name}_{gp}_{table_hash}"

            csv_path = path.normpath(
                f"{output_dir}{path.sep}{table_name}-{gp}-{table_hash}.csv"
            )

            if not (path.exists(csv_path)):
                # create new file
                csv_file = open(csv_path, mode="w", newline="")

                msg = f"Created {csv_path}"
                with lock:
                    print(msg)

                writer = csv.writer(csv_file)
                # ST = measurement start time
                # NEDN = network element distinguished name
                # LDN = measured object distinguished name, within the context of the NEDN
                header = ["ST", "NEDN", "LDN"] + columns_values
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
                writer.writerow(row)

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


def consume_ldn_natural_key_to_csv(queue: Queue, lock: Lock, output_dir: str):
    """Serialize tables received from queue to CSV file.

    Place the CSV file in the output dir (output_dir).

    Create file if doesn't exist and appends data.

    Take notice: it doesn't delete the file previously to the serialization.

    The CSV contain at least one columns: ST

    ST = measurement start time (YYYYMMDDHHMMSS)
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
            nedn_list = eval(f"""['{nedn.replace(",","','").replace("=","','")}']""")

            # RncFunction=RF-1,UtranCell=Gbg-997
            ldn = item["rows"][0][2]
            ldn_list = eval(f"""['{ldn.replace(",","','").replace("=","','")}']""")

            nedn_keys = [item for i, item in enumerate(nedn_list) if i % 2 == 0]
            ldn_keys = [item for i, item in enumerate(ldn_list) if i % 2 == 0]
            columns_keys = nedn_keys + ldn_keys
            columns_values = item["mts"]
            gp = item["gp"]

            table_name = columns_keys[-1]
            table_hash = hashlib.md5(
                "".join(columns_keys + columns_values).encode()
            ).hexdigest()
            table_key = f"{table_name}_{gp}_{table_hash}"

            csv_path = path.normpath(
                f"{output_dir}{path.sep}{table_name}-{gp}-{table_hash}.csv"
            )

            if not (path.exists(csv_path)):
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
                nedn = row.pop(0)
                nedn_list = eval(f"""['{nedn.replace(",","','").replace("=","','")}']""")
                ldn = row.pop(0)
                ldn_list = eval(f"""['{ldn.replace(",","','").replace("=","','")}']""")
                nedn_values = [item for i, item in enumerate(nedn_list) if i % 2 != 0]
                ldn_values = [item for i, item in enumerate(ldn_list) if i % 2 != 0]
                writer.writerow([st] + nedn_values + ldn_values + row)

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


def consume_ldn_natural_key_to_parquet(
    queue: Queue,
    lock: Lock,
    output_dir: str,
    nedn_ignore_before="SubNetwork",
    ldn_ignore_before="SubNetwork",
    node_expression=None,
    node_partition_by=False,
):
    """Serialize tables received from queue to Parquet file.

    Identical to the consume_ldn_natural_key_to_csv method.

    But writes the data to partitioned Parquet files.

    To simplify the dimensions and reduce the data size we can control which
    parts of the NEDN and LDN are split into columns.
    This is particularly important to guarantee that we won't have duplicate columns.

    For instance in the NEDN below we should ignore before the last SubNetwork:

    NEDN = DC=a1.companyNN.com,SubNetwork=1,IRPAgent=1,SubNetwork=CountryNN,MeContext=MEC-Gbg1,ManagedElement=RNC-Gbg-1

    Simplified NEDN =SubNetwork=CountryNN,MeContext=MEC-Gbg1,ManagedElement=RNC-Gbg-1

    Which is enough to identify the NE in the network.

    nedn_ignore_before: str -> ignores the NEDN before this member last occurrence
    ldn_ignore_before: str -> ignores the LDN before this member last occurrence
    node_expression: str -> use expression to calculate the node key and replace the nedn with it (reduces amount of data)
    node_partition_by: bool -> partition by node if True
    """

    if node_partition_by and not (node_expression):
        raise TeedException("We need a node_expression to partition the data by node!")

    def file_visitor(written_file):
        """ PyArrow file visitor method, called when a new file is created """

        print(f"path={written_file.path}")
        print(f"size={written_file.size} bytes")
        print(f"metadata={written_file.metadata}")

    def get_timestamp(my_datetime: str) -> list:
        """Return a list with the timestamp from a datetime %Y%m%d%H%M%S string"""
        return [datetime.strptime(my_datetime, "%Y%m%d%H%M%S")]

    def get_day_hh_mm(my_datetime: str) -> list:
        """Return list of day: date, hh: int and mm: int from a datetime %Y%m%d%H%M%S string """
        return [
            datetime.strptime(my_datetime[0:8], "%Y%m%d"),
            int(my_datetime[8:10]),
            int(my_datetime[10:12]),
        ]

    def get_day_hh(my_datetime: str) -> list:
        return [datetime.strptime(my_datetime[0:8], "%Y%m%d"), int(my_datetime[8:10])]

    def get_day(my_datetime: str) -> list:
        return [datetime.strptime(my_datetime[0:8], "%Y%m%d")]

    # map the granularity period to time partitioning
    gp_time_column = {
        300: ["day", "hh", "mm"],
        900: ["day", "hh", "mm"],
        3600: ["day", "hh"],
        86400: ["day"],
    }

    gp_time_parts = {
        300: get_day_hh_mm,
        900: get_day_hh_mm,
        3600: get_day_hh,
        86400: get_day,
    }

    gp_time_partitioning = {
        300: [
            pa.field("day", pa.date32()),
            pa.field("hh", pa.uint8()),
            pa.field("mm", pa.uint8()),
        ],
        900: [
            pa.field("day", pa.date32()),
            pa.field("hh", pa.uint8()),
            pa.field("mm", pa.uint8()),
        ],
        3600: [
            pa.field("day", pa.date32()),
            pa.field("hh", pa.uint8()),
        ],
        86400: [
            pa.field("day", pa.date32()),
        ],
    }

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

            # original nedn
            # DC=a1.companyNN.com,SubNetwork=1,IRPAgent=1,SubNetwork=CountryNN,MeContext=MEC-Gbg1,ManagedElement=RNC-Gbg-1
            #
            # nedn by ignoring everything before the last SubNetwork
            # SubNetwork=CountryNN,MeContext=MEC-Gbg1,ManagedElement=RNC-Gbg-1
            nedn = item["rows"][0][1]
            nedn = (
                nedn[nedn.rfind(nedn_ignore_before) :]
                if nedn_ignore_before in nedn
                else nedn
            )
            nedn_dict = OrderedDict(
                eval(f"""{{'{nedn.replace(",","','").replace("=","':'")}'}}""")
            )

            # original ldn RncFunction=RF-1,UtranCell=Gbg-997
            ldn = item["rows"][0][2]
            ldn = ldn[ldn.rfind(ldn_ignore_before) :] if ldn_ignore_before in ldn else ldn
            ldn_dict = OrderedDict(
                eval(f"""{{'{ldn.replace(",","','").replace("=","':'")}'}}""")
            )

            nedn_keys = (
                list(nedn_dict.keys()) if node_expression is None else ["Node"]
            )  # use node instead of the nedn values if node_expression is defined
            ldn_keys = list(ldn_dict.keys())
            columns_keys = nedn_keys + ldn_keys  # keys
            columns_values = item["mts"]  # columns
            gp = int(item["gp"])  # granularity period in seconds

            # identify a table by the its DN name, Node (if partitioned) and the granularity period
            # the parquet_path is a directory
            table_name = (
                f"{columns_keys[-1]}-Node-{gp}"
                if node_partition_by
                else f"{columns_keys[-1]}-{gp}"
            )
            parquet_path = path.normpath(f"{output_dir}{path.sep}{table_name}")

            # PyArrow table
            # the table column names
            table_columns = gp_time_column.get(gp, ["st"]) + columns_keys + columns_values

            # the table column values, one list per column
            table_data = [[] for _ in table_columns]
            for row in item["rows"]:
                st = row.pop(0)
                nedn = row.pop(0)
                nedn = (
                    nedn[nedn.rfind(nedn_ignore_before) :]
                    if nedn_ignore_before in nedn
                    else nedn
                )
                nedn_dict = OrderedDict(
                    eval(f"""{{'{nedn.replace(",","','").replace("=","':'")}'}}""")
                )
                ldn = row.pop(0)
                ldn = (
                    ldn[ldn.rfind(ldn_ignore_before) :]
                    if ldn_ignore_before in ldn
                    else ldn
                )
                ldn_dict = OrderedDict(
                    eval(f"""{{'{ldn.replace(",","','").replace("=","':'")}'}}""")
                )

                get_time_parts = gp_time_parts.get(gp, get_timestamp)
                nedn_values = (
                    list(nedn_dict.values())
                    if node_expression is None
                    else [
                        eval(node_expression)
                    ]  # use the node_expression value as node instead of the nedn values
                )
                ldn_values = list(ldn_dict.values())
                values = (
                    get_time_parts(st)
                    + nedn_values
                    + ldn_values
                    + [int(item) for item in row]
                )

                for i, value in enumerate(values):
                    table_data[i].append(value)

            # create the table schema
            # depending on the gp the date/time columns are different
            # if the granularity period isn't expected use timestamp
            # all column keys are string
            # all column values are unsigned integers
            table_schema = pa.schema(
                gp_time_partitioning.get(gp, [pa.field("st", pa.timestamp(unit="s"))])
                + [pa.field(column, pa.string()) for column in columns_keys]
                + [pa.field(column, pa.uint32()) for column in columns_values]
            )
            table = pa.table(data=table_data, schema=table_schema)

            # output data
            table_hash = hashlib.md5(
                "".join(columns_keys + columns_values).encode()
            ).hexdigest()

            # partitioning data
            partition_fields = (
                [pa.field("Node", pa.string())] + gp_time_partitioning.get(gp, None)
                if node_partition_by
                else gp_time_partitioning.get(gp, None)
            )
            part = ds.partitioning(pa.schema(partition_fields))

            ds.write_dataset(
                table,
                parquet_path,
                basename_template=table_hash + "-{i}.parquet",
                format="parquet",
                partitioning=part,
                file_visitor=file_visitor,
                existing_data_behavior="overwrite_or_ignore",
            )

        except KeyboardInterrupt:
            with lock:
                print("KeyboardInterrupt received, stopping!")

            break

        except Empty:
            continue


def handler_stop(signum, frame):
    """Stop signal handler"""

    raise TeedException(f"Signal handler called with signal {signum}")


def parse(
    pathname: str,
    output_dir: str,
    recursive: bool = False,
    consume=consume_to_csv,
    consume_kwargs={},
):
    """Go through the files in pathname, extracts data

    and places it in queue. The items in the queue are serialized

    to CSV files in disk by a daemon consumer process.

    The producer, the parent process, waits for the consumer to finish.

    This method is based on the example found in:

    https://stackoverflow.com/questions/11515944/how-to-use-multiprocessing-queue-in-python
    """

    # items queue
    queue = Queue()

    # control resource access using a lock
    lock = Lock()

    # Use signal handler
    signal.signal(signal.SIGTERM, handler_stop)

    try:

        # the consumer process
        consumer_proc = None

        consumer_proc = Process(
            target=consume,
            name="consumer",
            args=(queue, lock, output_dir),
            kwargs=consume_kwargs,
        )
        consumer_proc.daemon = True

        # Start the consumer
        # The Python VM will launch new independent processes for each Process object
        consumer_proc.start()

        # Go through the files retreived from pathname
        # and start producing items to the queue
        produce(queue, lock, pathname)

    except KeyboardInterrupt:
        queue.put("STOP")

    except TeedException as e:
        queue.put("STOP")
        print(e)

    # Wait for the consumer to end
    consumer_proc.join()

    print("Producer and consumer done, exiting.")


@program.command(name="parse")
def parse_program(pathname: str, output_dir: str, recursive: bool = False) -> None:
    """Parse Mdc files returneb by pathname glob and

    place it's content in output directories CSV files

    Command-line program for meas.parse function

    Parameters:
        meas/mdc pathname glob (str): pathname
        search files recursively in subdirectories (bool): recursive
        output directory (str): output_dir
    """

    try:
        start = time.perf_counter()
        parse(pathname, output_dir, recursive)
        duration = time.perf_counter() - start
        print(f"Duration(s): {duration}")
    except TeedException as e:
        typer.secho(f"Error parsing {pathname}")
        typer.secho(str(e), err=True, fg=typer.colors.RED, bold=True)
        exit(1)


if __name__ == "__main__":
    program()
