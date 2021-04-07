# python -m teed meas parse data/mdc_c3_1.xml data
# python -m teed meas parse data/meas_c4_1.xml data
# python -m teed meas parse "data/mdc*xml" data
# python -m teed meas parse "data/meas*xml" data

# python -m teed meas parse "benchmark/eric_rnc/A*xml" tmp
# Parent process exiting...
# Duration(s): 20.945605682000178
# python -m teed meas parse "benchmark/eric_nodeb/A*xml" tmp
# Parent process exiting...
# Duration(s): 15.023600272999829
# python -m teed meas parse "benchmark/*/A*xml" tmp --recursive
# Parent process exiting...
# Duration(s): 35.324785770999824
# Using top we've seen low memory usage, even though two python processes are used: producer and consumer


import csv
import glob
import hashlib
import os
import time
from datetime import datetime, timedelta
from multiprocessing import Lock, Process, Queue
from os import path

import typer

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
                        #   <mts>20000301141430</mts>
                        #   <gp>900</gp>
                        ts = mi.find("mts").text
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
                            moid = mv.find("moid").text
                            # create the DN from the concatenation of nedn and moid
                            dn = (f"{nedn},{moid}").strip(",")
                            row = [meas_ts, dn]
                            for r in mv.iterfind("r"):
                                row.append(r.text)

                            table["rows"].append(row)

                        # if there're rows in table
                        # place it in the queue
                        if table["rows"] != [] and mts != []:
                            table_name = (moid.split(",")[-1]).split("=")[0]  # UtranCell
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
                    #     <cbt>20000301140000</cbt>
                    # </mfh>
                    metadata["encoding"] = (element.getroottree()).docinfo.encoding

                    for child in element:
                        metadata[child.tag] = child.text

                elif localName == "mff":
                    # <mff>
                    #   <ts>20000301141500</ts>
                    # </mff>
                    for child in element:
                        metadata[child.tag] = child.text

                element.clear(keep_tail=False)


def consume(queue: Queue, lock: Lock, output_dir: str, csvs: Queue):
    writers = {}  # maps the node_key to it's writer

    with lock:
        print(f"Consumer starting {os.getpid()}")

    while True:
        table = queue.get()

        moid_1st = table["rows"][0][1]  # RncFunction=RF-1,UtranCell=Gbg-997
        table_name = (moid_1st.split(",")[-1]).split("=")[0]  # UtranCell
        columns = table["mts"]
        gp = table["gp"]

        table_hash = hashlib.md5("".join(columns).encode()).hexdigest()
        table_key = f"{table_name}_{gp}_{table_hash}"

        csv_path = path.normpath(
            f"{output_dir}{path.sep}{table_name}-{gp}-{table_hash}.csv"
        )

        if not (path.exists(csv_path)):
            # create new file
            csv_file = open(csv_path, mode="w", newline="")

            with lock:
                print(f"Created {csv_path}")

            writer = csv.writer(csv_file)
            header = ["ST", "DN"] + columns
            writer.writerow(header)

            writers[table_key] = writer
            csvs.put(csv_path)

        elif table_key not in writers:
            # append to end of file
            csv_file = open(csv_path, mode="a", newline="")

            with lock:
                print(f"Append {csv_path}")

            writer = csv.writer(csv_file)
            writers[table_key] = writer

        else:
            # file and writer exist
            # get previously created writer
            writer = writers.get(table_key)

        # serialize rows to csv file
        for row in table["rows"]:
            writer.writerow(row)

        # flush the data to disk
        csv_file.flush()


def parse(pathname: str, output_dir: str, recursive: bool = False) -> list:

    # Create the csv files Queue object
    csvs = Queue()

    # Create the procuder -> consumer Queue object
    queue = Queue()

    # Create a lock object to synchronize resource access
    lock = Lock()

    producer_proc = None
    consumer_proc = None

    producer_proc = Process(
        target=produce, name="producer", args=(queue, lock, pathname, recursive)
    )

    consumer_proc = Process(
        target=consume, name="consumer", args=(queue, lock, output_dir, csvs)
    )
    consumer_proc.daemon = True

    # Start the producer and consumer
    # The Python VM will launch new independent processes for each Process object
    producer_proc.start()
    consumer_proc.start()

    # Like threading, we have a join() method that synchronizes our program
    producer_proc.join()

    # Go through all the csv files created
    csvs_list = []
    while True:
        if csvs.empty():
            break

        csvs_list.append(csvs.get())

    print("Parent process exiting...")

    return csvs_list


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
