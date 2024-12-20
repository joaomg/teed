# python -m teed bulkcm parse data/bulkcm_empty.xml data
# python -m teed bulkcm parse data/bulkcm_with_header_footer.xml data
# python -m teed bulkcm parse data/bulkcm_with_vsdatacontainer.xml data
# python -m teed bulkcm probe data/bulkcm_with_vsdatacontainer.xml

import csv
import hashlib
import os
from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime
from os import path
from pprint import pprint
from typing import Generator, List

import typer
import pyarrow.fs as fs
from pyarrow.lib import ArrowInvalid

from io import TextIOWrapper
import yaml
from lxml import etree

from teed import TeedException, file_path_parse, get_xml_encoding

program = typer.Typer()


def reverse_readline(filename, buf_size=8192):
    """A generator that returns the lines of a file in reverse order

    Retrieved from with some modification:

    http://stackoverflow.com/questions/2301789/read-a-file-in-reverse-order-using-python
    """

    with open(filename) as fh:
        segment = None
        offset = 0
        fh.seek(0, os.SEEK_END)
        file_size = remaining_size = fh.tell()
        while remaining_size > 0:
            offset = min(file_size, offset + buf_size)
            fh.seek(file_size - offset)
            buffer = fh.read(min(remaining_size, buf_size))
            remaining_size -= buf_size
            lines = buffer.split("\n")
            # the first line of the buffer is probably not a complete line so
            # we'll save it and append it to the last line of the next buffer
            # we read
            if segment is not None:
                # if the previous chunk starts right from the beginning of line
                # do not concact the segment to the last line of new chunk
                # instead, yield the segment first
                if buffer[-1] != "\n":
                    lines[-1] += segment
                else:
                    yield segment
            segment = lines[0]
            for index in range(len(lines) - 1, 0, -1):
                if len(lines[index]):
                    yield lines[index]

        # Don't yield None if the file was empty
        if segment is not None:
            yield segment


class BulkCmParser:
    """The parser target object that receives

    etree parse events for BulkCm parsing

    Collects bulkcm data into node dicts

    and sends them to the stream.

    Parameters:
        nodes stream (Generator): stream
    """

    def __init__(
        self,
        stream: Generator,
        include_elements: list = [],
        exclude_elements: list = [],
    ):
        # bulkcm general file data
        self._metadata = {}

        # configData
        self._dnPrefix = None

        # attributes
        self._is_attributes = False

        # element text buffer
        self._text = []

        self._node_attributes = {}
        self._node_queue = []
        self._node_path = {}
        self._nodes = []

        # vsData handling
        self._is_vs_data = False
        self._vs_data_type = None

        # move stream to the first yield
        # set it ready to receive nodes
        self._stream = stream
        next(stream)

        # include/exclude elements
        self._include_elements = list(include_elements)
        self._exclude_elements = list(exclude_elements)

    def start(self, tag, attrib):
        # flow-control using the element tag local name
        # tag = {http://www.3gpp.org/ftp/specs/archive/32_series/32.615#configData1}configData
        # localname = configData
        localname = etree.QName(tag).localname

        if localname == "attributes":
            # <xn:attributes>
            self._is_attributes = True

        elif localname == "vsDataType":
            self._vs_data_type = None

        elif localname == "vsDataFormatVersion":
            pass

        elif localname == "configData":
            # <configData dnPrefix="DC=a1.companyNN.com">
            self._dnPrefix = attrib.get("dnPrefix")

        elif localname == "fileHeader":
            # <fileHeader fileFormatVersion="32.615 V4.0" senderName="DC=a1.companyNN.com,SubNetwork=1,IRPAgent=1" vendorName="Company NN" />
            self._metadata.update(attrib)

        elif localname == "fileFooter":
            # <fileFooter dateTime="2001-05-07T12:00:00+02:00"/>
            self._metadata.update(attrib)

        elif localname == "bulkCmConfigDataFile":
            pass

        elif len(attrib) > 0:
            if "*" in self._exclude_elements and localname not in self._include_elements:
                if localname not in self._exclude_elements:
                    self._exclude_elements.append(localname)

            self._node_queue.append(localname)
            self._node_path[localname] = attrib.get("id").strip()
            self._nodes.append(
                {
                    "node_name": localname,
                    "node_path": deepcopy(self._node_path),
                    "node_values": {},
                }
            )

            if localname == "VsDataContainer":
                self._is_vs_data = True

        else:
            self._node_queue.append(localname)

    def end(self, tag):
        localname = etree.QName(tag).localname

        if localname == "attributes":
            # </xn:attributes>
            # not a attribute, localname is a node

            node = self._nodes.pop()
            if node["node_name"] not in self._exclude_elements:
                # node.update(self._node_attributes)
                node["node_values"] = self._node_attributes
                self._stream.send(node)

            self._node_attributes = {}
            self._is_attributes = False

        elif localname == "vsDataType":
            # replace the previous node_name
            vs_data_type = "".join(self._text)
            self._vs_data_type = vs_data_type
            vs_id = self._node_path.pop("VsDataContainer")

            # change VsDataContainer node_name to the vs_data_type
            # and change the node_path to vs_data_type
            # while preserving the vs_id
            # update the node_key in the latest node
            self._node_path[vs_data_type] = vs_id

            node = self._nodes[-1]
            node["node_name"] = vs_data_type
            node["node_path"] = deepcopy(self._node_path)

            self._text = []

            if (
                "*" in self._exclude_elements
                and vs_data_type not in self._include_elements
            ):
                if vs_data_type not in self._exclude_elements:
                    self._exclude_elements.append(vs_data_type)

        elif localname == "vsDataFormatVersion":
            self._text = []

        elif localname == "configData":
            pass

        elif localname == "fileHeader":
            pass

        elif localname == "fileFooter":
            pass

        elif localname == "bulkCmConfigDataFile":
            pass

        elif localname == "VsDataContainer":
            self._is_vs_data = False
            self._vs_data_type = None

            # end of node
            self._node_path.popitem()

        else:
            node = self._node_queue.pop()

            if localname == self._vs_data_type:
                # it's an enclosing element, ignore it
                # </un:vsDataRHO>
                pass

            elif self._is_attributes:
                # inside <xn:attributes>, node is an attribute
                self._node_attributes[node] = "".join(self._text)

            else:
                # end of node
                self._node_path.popitem()

            self._text = []

    def data(self, data):
        self._text.append(data.strip())

    def close(self):
        # send remaining nodes to stream
        for node in self._nodes:
            node_name = node.get("node_name")
            if node_name not in self._exclude_elements:
                self._stream.send(node)

        # send close signal to
        # the stream generator
        self._stream.close()

        return self._metadata

    @staticmethod
    def stream_to_csv(
        output_dir_or_bucket,
        output_fs: fs.FileSystem = fs.LocalFileSystem(),
    ) -> Generator[dict, None, None]:
        """Serialization of nodes to csv files using generator

        creates the CSV file in the output directory

        receives node dict by send/yield

        @@@ to be changed to producer/consumer using asyncio.Queue
        @@@ https://pymotw.com/3/asyncio/synchronization.html#queues

        Parameters:
            output directory (str): output_dir_or_bucket
            output filesystem (pyarrow.fs.FileSystem): output_fs
        """

        writer = None
        writers = {}  # maps the node_key to it's writer

        try:
            while True:
                node = yield
                node_name = node.pop("node_name")
                node_path = node.pop("node_path")
                node_values = node.pop("node_values")
                # @@@ this md5 hash is expensive, and runs for each node
                # @@@ analyze and find a more efficient method
                columns = list(node_path.keys()) + list(node_values.keys())
                node_hash = hashlib.md5("".join(columns).encode()).hexdigest()
                node_key = f"{node_name}-{node_hash}"

                if node_key not in writers:
                    # create new file
                    # using mode w truncate existing files
                    csv_path = output_fs.normalize_path(
                        f"{output_dir_or_bucket}{path.sep}{node_name}-{node_hash}.csv"
                    )
                    csv_bstream = output_fs.open_output_stream(csv_path, compression=None)

                    print(f"Created {csv_path}")
                    writer = csv.DictWriter(
                        TextIOWrapper(csv_bstream), fieldnames=columns
                    )
                    writer.writeheader()

                    writers[node_key] = writer

                else:
                    # get previously created writer
                    writer = writers.get(node_key)

                node_path.update(node_values)
                writer.writerow(node_path)

        except StopIteration:
            for writer in writers:
                writer.close()


def parse(
    file_uri: str,
    output_dir_or_bucket: str,
    stream: Generator,
    include_elements: list = [],
    exclude_elements: list = [],
    output_fs: fs.FileSystem = fs.LocalFileSystem(),
) -> tuple:
    """Parse BulkCm file and place it's content in output directories CSV files

    Parameters:
        file_uri (str): file_uri
        output directory (str): output_dir_or_bucket
        send parsed nodes to stream (Generator): stream
        elements to parse (list): include_elements
        elements to ignore (list): exclude_elements
        output filesystem (pyarrow.fs.FileSystem): output_fs

    Returns:
        bulkcm metadata and parsing duration (dict, timedelta): (metadata, duration)
    """

    try:
        # create input filesystem and path from the uri
        input_fs, input_path = fs.FileSystem.from_uri(file_uri)
    except ArrowInvalid:
        raise TeedException(f"Error, check if the {file_uri} uri exists .")

    # check if the file exists in the filesystem
    if input_fs.get_file_info(input_path).type == fs.FileType.NotFound:
        raise TeedException(f"Error, {file_uri} doesn't exists")

    if output_fs.get_file_info(output_dir_or_bucket).type == fs.FileType.NotFound:
        raise TeedException(
            f"Error, output directory {output_dir_or_bucket} doesn't exists"
        )

    parser = etree.XMLParser(
        target=BulkCmParser(stream, include_elements, exclude_elements),
        no_network=True,
        ns_clean=True,
        remove_blank_text=True,
        remove_comments=True,
        remove_pis=True,
        huge_tree=True,
        recover=False,
    )

    start = datetime.now()

    try:
        # parse the BulkCm file
        with input_fs.open_input_stream(input_path) as input_stream:
            metadata = etree.parse(input_stream, parser)

        # output metadata
        _, file_name_without_ext, _ = file_path_parse(file_uri)
        metadata_file_path = output_fs.normalize_path(
            f"{output_dir_or_bucket}{path.sep}{file_name_without_ext}_metadata.yml"
        )
        with output_fs.open_output_stream(metadata_file_path, compression=None) as out:
            with TextIOWrapper(out) as tout:
                yaml.dump(metadata, tout, default_flow_style=False)

    except etree.XMLSyntaxError as e:
        raise TeedException(e)

    finish = datetime.now()

    return (metadata, finish - start)


@program.command(name="parse")
def parse_program(
    file_path_or_uri: str,
    output_dir: str,
    include_elements: List[str] = typer.Option(
        [],
        "--include-element",
        "-ie",
        help="Parse element",
    ),
    exclude_elements: List[str] = typer.Option(
        [],
        "--exlude-element",
        "-ee",
        help="Ignore element",
    ),
) -> None:
    """Parse BulkCm file and place it's content in output directories CSV files

    Command-line program for bulkcm.parse function

    Parameters:
        bulkcm file path (str): local file path or PyArrow URI
        output directory (str): output_dir
        elements to parse (list): include_elements
        elements to ignore (list): exclude_elements
    """

    print(f"Parsing {file_path_or_uri}")

    # check if file_path_or_uri is a local file path of a URI
    if path.exists(file_path_or_uri):
        file_uri = f"file://{path.abspath(file_path_or_uri)}"
    else:
        file_uri = file_path_or_uri

    try:
        # stream to csv files
        stream_csv = BulkCmParser.stream_to_csv(output_dir)

        _, duration = parse(
            file_uri,
            output_dir,
            stream_csv,
            include_elements,
            exclude_elements,
        )
        print(f"Duration: {duration}")
    except TeedException as e:
        typer.secho(f"Error parsing {file_path_or_uri}")
        typer.secho(str(e), err=True, fg=typer.colors.RED, bold=True)
        exit(1)


def subnetwork_writer(
    sn: etree._Element,
    sn_file_path: str,
    subnetwork_ids: list,
    bulkCmConfigDataFile: dict,
    fileHeader: dict,
    configData: dict,
    fileFooter: dict,
    output_fs: fs.FileSystem,
):
    """Writes a SubNetwork to the output directory

    A new, valid, BulkCm file is created with a single SubNetwork
    """

    with output_fs.open_output_stream(sn_file_path, compression=None) as out_stream:
        with etree.xmlfile(out_stream, encoding=bulkCmConfigDataFile["encoding"]) as xf:
            xf.write_declaration()
            with xf.element("bulkCmConfigDataFile", nsmap=bulkCmConfigDataFile["nsmap"]):
                if fileHeader is not None:
                    xf.write(etree.Element("fileHeader", attrib=fileHeader["attrib"]))

                with xf.element("configData", attrib=configData["attrib"]):
                    # enter the xn:SubNetwork(s)
                    with ExitStack() as stack:
                        for sn_id in subnetwork_ids[:-1]:
                            stack.enter_context(xf.element("xn:SubNetwork", id=sn_id))

                        xf.write(sn)

                if fileFooter is not None:
                    xf.write(etree.Element("fileFooter", attrib=fileFooter["attrib"]))


def split_by_subnetwork(
    file_path_or_uri: str, output_dir_or_bucket: str, subnetworks: list = []
) -> Generator[tuple, None, None]:
    """Search and write a SubNetwork(s) ElementTree to new file(s).

    If subnetworks contains values, these are the SubNetwork id's

    which will be split to files. Other SubNetwork, not present in the list are to be ignore.

    By default subnetworks is empty. All SubNetwork are to be processed and split to a respective file.

    Parameters:
        bulkcm file path (str): file_path
        output directory (str): output_dir_or_bucket
        list of SubNetwork id's (list): subnetworks (if empty consider all SubNetwork's)

    Yields:
        Tuple with the SubNetwork id and file path: generator(sn_id, sn_file_path)

    Raise:
        TeedException (inside the split_by_subnetwork call)
    """

    # handle input
    if path.exists(file_path_or_uri):
        # local file
        file_path = file_path_or_uri
    else:
        # URI
        # copy file to local filesystem for splitting
        input_fs, file_path = fs.FileSystem.from_uri(file_path_or_uri)

        # create destination local dir
        output_dir = path.dirname(file_path)
        os.makedirs(output_dir, exist_ok=True)

        fs.copy_files(
            file_path,
            file_path,
            source_filesystem=input_fs,
            destination_filesystem=fs.LocalFileSystem(),
        )

    # handle output
    if path.exists(output_dir_or_bucket):
        # if the output_dir_or_bucket
        # exists as a directory in the
        # local filesystem
        output_fs = fs.LocalFileSystem()
        output_dir = output_dir_or_bucket
    else:
        # create the output filesystem from
        # the output_dir_or_bucket URI
        output_fs, output_dir = fs.FileSystem.from_uri(output_dir_or_bucket)

    # read file footer
    footer = []
    for line in reverse_readline(file_path, 1024):
        line = line.strip()
        if not (line.startswith("</configData")):
            # footer
            footer.append(line)
        else:
            # configData found, break loop
            break

    # extract the fileFooter dateTime attribute
    # <fileFooter dateTime="2017-10-04T00:39:15Z"/>
    fileFooter = None
    for line in footer:
        if line.startswith("<fileFooter"):
            dateTime = line[line.index('"') + 1 : line.rindex('"')]
            fileFooter = {"attrib": {"dateTime": dateTime}}
            break

    _, file_name_without_ext, file_ext = file_path_parse(file_path)

    with open(file_path, mode="rb") as stream:
        bulkCmConfigDataFile = None
        configData = None
        fileHeader = None
        fileFooter = None
        subnetwork_ids = []

        try:
            for event, element in etree.iterparse(
                stream,
                events=(
                    "start",
                    "end",
                ),
                tag=(
                    "{*}bulkCmConfigDataFile",
                    "{*}fileHeader",
                    "{*}configData",
                    "{*}SubNetwork",
                    "{*}fileFooter",
                ),
                no_network=True,
                remove_blank_text=True,
                remove_comments=True,
                remove_pis=True,
                huge_tree=True,
                recover=False,
            ):
                localName = etree.QName(element.tag).localname

                if event == "start" and localName == "SubNetwork":
                    sn_id = element.attrib.get("id")
                    subnetwork_ids.append(sn_id)

                elif event == "end" and localName == "SubNetwork":
                    # check if the subnetwork include filter
                    # is used and if the subnetwork is to be
                    # processed
                    if len(subnetworks) > 0:
                        sn_id = subnetwork_ids[-1]
                        if sn_id not in subnetworks:
                            subnetwork_ids.pop()
                            element.clear(keep_tail=False)

                            yield (sn_id, None)

                            # move to the next iterparse event
                            continue

                    # subnetwork will be split to it's file
                    sn_file_path = output_fs.normalize_path(
                        f"{output_dir}{path.sep}{file_name_without_ext}_{'_'.join(subnetwork_ids)}.{file_ext}"
                    )

                    subnetwork_writer(
                        element,
                        sn_file_path,
                        subnetwork_ids,
                        bulkCmConfigDataFile,
                        fileHeader,
                        configData,
                        fileFooter,
                        output_fs,
                    )

                    # yield the latest SubNetwork
                    yield (subnetwork_ids.pop(), sn_file_path)

                elif event == "start" and localName == "bulkCmConfigDataFile":
                    doc_encoding = (element.getroottree()).docinfo.encoding
                    encoding = (
                        doc_encoding
                        if doc_encoding is not None
                        else get_xml_encoding(file_path)
                    )
                    bulkCmConfigDataFile = {
                        "tag": element.tag,
                        "attrib": element.attrib,
                        "nsmap": element.nsmap,
                        "encoding": encoding,
                    }

                elif event == "start" and localName == "fileHeader":
                    fileHeader = {
                        "tag": element.tag,
                        "attrib": deepcopy(element.attrib),
                        "nsmap": element.nsmap,
                    }

                elif event == "start" and localName == "configData":
                    configData = {
                        "tag": element.tag,
                        "attrib": deepcopy(element.attrib),
                        "nsmap": element.nsmap,
                    }

                if event == "end":
                    element.clear(keep_tail=False)

        except etree.XMLSyntaxError as e:
            raise TeedException(e)


def split(
    file_path_or_uri: str, output_dir_or_bucket: str, subnetworks: List[str] = []
) -> None:
    """Split a BulkCm file by SubNetwork element using the split_by_subnetwork function.

    Write the SubNetwork(s) ElementTree to new file(s).

    Calls the bulkcm.split_by_subnetwork function and

    stores each SubNetwork id and file produced specific lists (sn_ids and sn_file_paths)

    Parameters:
        bulkcm file path (str): file_path
        output directory (str): output_dir
        list of SubNetwork id's (list): subnetworks (if empty consider all SubNetwork's)
    """

    sn_ids = []
    sn_file_paths = []

    for sn_id, sn_file_path in split_by_subnetwork(
        file_path_or_uri, output_dir_or_bucket, subnetworks
    ):
        sn_ids.append(sn_id)
        sn_file_paths.append(sn_file_path)

    return sn_ids, sn_file_paths


@program.command(name="split")
def split_program(
    file_path_or_uri: str,
    output_dir_or_bucket: str,
    subnetworks: List[str] = typer.Option(
        [],
        "--subnetwork",
        "-s",
        help="SubNetworks id's to be split to file",
    ),
) -> None:
    """Split a BulkCm file by SubNetwork element
    using the split_by_subnetwork function.

    Write the SubNetwork(s) ElementTree to new file(s).

    Command-line program for bulkcm.split function

    Parameters:
        bulkcm file path (str): file_path
        output directory (str): output_dir
        list of SubNetwork id's (list): subnetworks (if empty consider all SubNetwork's)
    """

    sn_count = 0
    sn_ignored = 0

    print(f"Splitting {file_path_or_uri} to {output_dir_or_bucket}")

    start = datetime.now()

    try:
        sn_ids, sn_file_paths = split(file_path_or_uri, output_dir_or_bucket, subnetworks)
        for i, sn_file_path in enumerate(sn_file_paths):
            sn_id = sn_ids[i]

            if sn_file_path is not None:
                # processed SubNetwork
                print(f"SubNetwork {sn_id} in {sn_file_path}")
                sn_count += 1

            else:
                # ignored SubNetwork
                print(f"Ignored SubNetwork {sn_id}")
                sn_ignored += 1

    except TeedException as e:
        typer.secho(str(e), err=True, fg=typer.colors.RED, bold=True)
        exit(1)

    finish = datetime.now()

    print(f"SubNetwork processed: #{sn_count}")
    print(f"SubNetwork ignored: #{sn_ignored}")
    print(f"Duration: {finish - start}")


def probe(
    file_uri: str,
    elements: list = [
        "ManagementNode",
        "MeContext",
        "ManagedElement",
        "ExternalGsmCell",
        "ExternalUtranCell",
    ],
) -> dict:
    """Probe a BulkCm file

    BulkCm XML format as described in ETSI TS 132 615
    https://www.etsi.org/deliver/etsi_ts/132600_132699/132615/09.02.00_60/ts_132615v090200p.pdf

    Analysis the BulkCm file and counts the number of elements inside the SubNetwork(s)

    Parameters:
        file_uri (str): file_uri (URI as in https://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html#pyarrow.fs.FileSystem.from_uri)
        list of elements to count (list): elements

    Returns:
        general BulkCm information (dict): bulkcm_info

    Raise:
        TeedException
    """

    # probing result is
    # stored in outcome dict
    bulkcm_info = {}

    try:
        # create input filesystem and path from the uri
        input_fs, path = fs.FileSystem.from_uri(file_uri)
    except ArrowInvalid:
        raise TeedException(f"Error, check if the {file_uri} uri exists .")

    # check if the file exists in the filesystem
    if input_fs.get_file_info(path).type == fs.FileType.NotFound:
        raise TeedException(f"Error, {file_uri} doesn't exists")

    # add elements to tags
    # consider any namespace {*}
    # and remove duplicates
    search_tags = [
        "{*}bulkCmConfigDataFile",
        "{*}fileHeader",
        "{*}configData",
        "{*}SubNetwork",
        "{*}fileFooter",
    ]

    for element in elements:
        search_tags.append(f"{{*}}{element}")

    search_tags = list(set(search_tags))

    with input_fs.open_input_stream(path) as stream:
        subnetworks = []
        try:
            for event, element in etree.iterparse(
                stream,
                events=(
                    "start",
                    "end",
                ),
                tag=search_tags,
                no_network=True,
                remove_blank_text=True,
                remove_comments=True,
                remove_pis=True,
                huge_tree=True,
                recover=False,
            ):
                localName = etree.QName(element.tag).localname

                if event == "end" and localName in elements:
                    if localName not in subnetworks[-1]:
                        subnetworks[-1][localName] = 1
                    else:
                        subnetworks[-1][localName] += 1

                elif event == "start" and localName == "SubNetwork":
                    sn_id = element.attrib.get("id")
                    subnetworks.append({"id": sn_id})

                elif event == "start" and localName == "bulkCmConfigDataFile":
                    doc_encoding = (element.getroottree()).docinfo.encoding
                    encoding = (
                        doc_encoding
                        if doc_encoding is not None
                        else get_xml_encoding(path)
                    )
                    bulkcm_info = {
                        "encoding": encoding,
                        "nsmap": element.nsmap,
                        "fileHeader": None,
                        "configData": [],
                        "fileFooter": None,
                    }

                elif event == "end" and localName == "configData":
                    cd = deepcopy(element.attrib)
                    cd["SubNetwork(s)"] = subnetworks
                    bulkcm_info["configData"].append(cd)

                    subnetworks = []

                elif event == "start" and localName == "fileHeader":
                    bulkcm_info["fileHeader"] = deepcopy(element.attrib)

                elif event == "start" and localName == "fileFooter":
                    bulkcm_info["fileFooter"] = deepcopy(element.attrib)

                if event == "end":
                    element.clear(keep_tail=True)

        except etree.XMLSyntaxError as e:
            raise TeedException(e)

    return bulkcm_info


@program.command(name="probe")
def probe_program(
    file_path_or_uri: str,
    elements: List[str] = typer.Option(
        [
            "ManagementNode",
            "MeContext",
            "ManagedElement",
            "ExternalGsmCell",
            "ExternalUtranCell",
        ],
        "--element",
        "-e",
        help="Count ocorrences of elements inside SubNetworks",
    ),
) -> None:
    """Probe a BulkCm file

    BulkCm XML format as described in ETSI TS 132 615
    https://www.etsi.org/deliver/etsi_ts/132600_132699/132615/09.02.00_60/ts_132615v090200p.pdf

    Analysis the BulkCm file and counts the number of elements inside the SubNetwork(s)

    It's the command-line program for bulkcm.probe function

    Parameters:
        file_path_or_uri (str): local file path or PyArrow URI
        list of elements to count (list): elements
        list of SubNetwork id's (list): subnetworks (if empty consider all SubNetwork's)
    """

    bulkcm_info = []

    print(f"Probing {file_path_or_uri}")
    print(f"Searching for {', '.join(elements)} elements inside SubNetworks")

    start = datetime.now()

    # check if file_path_or_uri is a local file path of a URI
    if path.exists(file_path_or_uri):
        file_uri = f"file://{path.abspath(file_path_or_uri)}"
    else:
        file_uri = file_path_or_uri

    try:
        bulkcm_info = probe(file_uri, elements)

    except TeedException as e:
        typer.secho(str(e), err=True, fg=typer.colors.RED, bold=True)
        exit(1)

    if bulkcm_info == []:
        # the bulkcm_info list return by probe
        # can't be empty, otherwise it's an
        # invalid BulkCm file
        typer.secho(
            "Invalid BulkCm file, no valid element has been found",
            err=True,
            fg=typer.colors.RED,
            bold=True,
        )
        exit(1)

    finish = datetime.now()

    pprint(bulkcm_info, compact=True, sort_dicts=False)
    print(f"Duration: {finish - start}")


if __name__ == "__main__":
    program()
