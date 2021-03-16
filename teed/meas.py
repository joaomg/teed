# python -m teed meas parse data/mdc_c3_1.xml data
# python -m teed meas parse data/mdc_c3_2.xml data
# python -m teed meas parse data/meas_c4_2.xml data
# python -m teed meas parse data/meas_c4_2.xml data
# python -m teed meas parse "data/mdc*xml" data
# python -m teed meas parse "data/meas*xml" data

import asyncio
import csv
import glob
import hashlib
import time
from os import path

import typer

# import yaml
from lxml import etree

from teed import TeedException

program = typer.Typer()


async def produce(queue, pathname, recursive=False):
    """Fetch Meas/Mdc files from pathname glob and parse

    For each measData/md element create a table item

    and place it in the queue.

    Optionally search in the pathname subdirectories.
    """

    for file_path in glob.iglob(pathname, recursive=recursive):
        with open(file_path, mode="rb") as stream:
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
                        # nedn = neid.find("nedn").text
                        pass

                    for mi in element.iterfind("mi"):
                        table = {}
                        # <mi>
                        #   <mts>20000301141430</mts>
                        #   <gp>900</gp>
                        ts = mi.find("mts").text
                        gp = mi.find("gp").text

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
                            row = [moid]
                            for r in mv.iterfind("r"):
                                row.append(r.text)

                            table["rows"].append(row)

                        # if there're rows in table
                        # place it in the queue
                        if table["rows"] != [] and mts != []:
                            table_name = (moid.split(",")[-1]).split("=")[0]  # UtranCell
                            print(f"Placing {table_name}")

                            await queue.put(table)
                        else:
                            # ignoring this mi
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


async def consume(output_dir: str, queue: asyncio.Queue):
    writers = {}  # maps the node_key to it's writer

    while True:
        table = await queue.get()

        moid_1st = table["rows"][0][0]  # RncFunction=RF-1,UtranCell=Gbg-997
        table_name = (moid_1st.split(",")[-1]).split("=")[0]  # UtranCell
        columns = table["mts"]

        table_hash = hashlib.md5("".join(columns).encode()).hexdigest()
        table_key = f"{table_name}_{table_hash}"

        csv_path = path.normpath(f"{output_dir}{path.sep}{table_name}-{table_hash}.csv")

        if not (path.exists(csv_path)):
            # create new file
            csv_file = open(csv_path, mode="w", newline="")

            print(f"Created {csv_path}")

            writer = csv.writer(csv_file)
            writer.writerow(columns)

            writers[table_key] = writer

        elif table_key not in writers:
            # append to end of file
            csv_file = open(csv_path, mode="a", newline="")

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

        queue.task_done()


async def parse(
    pathname: str, output_dir: str, recursive: bool = False, queue_size: int = 12
) -> tuple:

    """
    queue = asyncio.Queue(10)
    # schedule the consumer
    consumer = asyncio.ensure_future(consume(output_dir, queue))
    # run the producer and wait for completion
    await produce(queue, pathname, recursive)
    # wait until the consumer has processed all items
    await queue.join()
    # the consumer is still awaiting for an item, cancel it
    consumer.cancel()
    """

    # table queue
    queue = asyncio.Queue(queue_size)

    # futures
    producer = asyncio.ensure_future(produce(queue, pathname, recursive))
    consumer = asyncio.ensure_future(consume(output_dir, queue))

    # wait for futures
    done, pending = await asyncio.wait(
        [producer, consumer],
        timeout=35,
        return_when=asyncio.FIRST_COMPLETED,
    )

    # cancel the consumer, which is now idle
    # consumer.cancel()

    for task in done:
        print(task)

    for task in pending:
        print(task)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            print("main(): cancel_me is cancelled now")

    """
    if consumer.done():
        print(f"Error, exception in consumer: {consumer.exception()}")

    if producer.done():
        print(f"Error, exception in producer: {producer.exception()}")
    """


@program.command(name="parse")
def parse_program(pathname: str, output_dir: str, recursive: bool = False) -> None:
    """Parse Mdc/Meas files returneb by pathname glob and

    place it's content in output directories CSV files

    Command-line program for meas.parse function

    Parameters:
        meas/mdc pathname glob (str): pathname
        search files recursively in subdirectories (bool): recursive
        output directory (str): output_dir
    """

    try:
        start = time.perf_counter()
        asyncio.run(parse(pathname, output_dir, recursive))
        duration = time.perf_counter() - start
        print(f"Duration(s): {duration}")
    except TeedException as e:
        typer.secho(f"Error parsing {pathname}")
        typer.secho(str(e), err=True, fg=typer.colors.RED, bold=True)
        exit(1)


if __name__ == "__main__":
    program()
