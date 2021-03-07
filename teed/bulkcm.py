#
# python -m teed bulkcm parse data/bulkcm_empty.xml data
# python -m teed bulkcm parse data/bulkcm_with_header_footer.xml data
# python -m teed bulkcm parse data/bulkcm_with_vsdatacontainer.xml data
# python -m teed bulkcm probe data/bulkcm_with_vsdatacontainer.xml

from os import path
from copy import deepcopy
from lxml import etree
from datetime import datetime
from typing import Generator
import csv
import typer
from teed import TeedException

program = typer.Typer()


class BulkCmParser:
    """ The parser target object that receives the etree parse events """

    def __init__(self, output_dir: str):

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
        self._nodes = []

        # vsData
        self._is_vs_data = False
        self._vs_data_type = None

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
            self._nodes.append(
                {"node_id": attrib.get("id").strip(), "node_name": localname}
            )
            self._node_queue.append(localname)

            if localname == "VsDataContainer":
                self._is_vs_data = True

        else:
            self._node_queue.append(localname)

    def end(self, tag):
        localname = etree.QName(tag).localname

        if localname == "attributes":
            # </xn:attributes>
            # not a attribute, localname is a node
            self._nodes[-1].update(self._node_attributes)
            self._node_attributes = {}

            self._is_attributes = False

        elif localname == "vsDataType":
            # replace the previous node_name
            vs_data_type = "".join(self._text)
            self._vs_data_type = vs_data_type
            self._nodes[-1]["node_name"] = vs_data_type
            self._text = []

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

        else:
            node = self._node_queue.pop()

            if localname == self._vs_data_type:
                # it's an enclosing element, ignore it
                # </un:vsDataRHO>
                pass

            elif self._is_attributes:
                # inside <xn:attributes>, node is an attribute
                self._node_attributes[node] = "".join(self._text)

            self._text = []

    def data(self, data):
        self._text.append(data.strip())

    def close(self):
        return self._metadata, self._nodes

    def nodes_to_csv(cls, nodes: list, output_dir: str) -> None:
        """A naive, iterative-approach, serialization of nodes to csv files

        @@@ to be changes to producer/consumer using asyncio.Queue
        @@@ https://pymotw.com/3/asyncio/synchronization.html#queues

        Parameters:
            node list of disct (list(dict)): nodes
            output directory (str): output_dir
        """

        csv_file = None
        writer = None
        previous_node_name = ""
        nodes = sorted(nodes, key=lambda k: k["node_name"])
        for node in nodes:
            node_name = node.pop("node_name")

            if previous_node_name != node_name:

                # flush and close previous csv_file
                if writer is not None:
                    csv_file.flush()
                    csv_file.close()

                # create new file
                # using mode w truncate existing files
                csv_path = f"{output_dir}{path.sep}{node_name}.csv"
                csv_file = open(csv_path, mode="w", newline="")

                print(f"Created {csv_path}")

                attributes = node.keys()
                writer = csv.DictWriter(csv_file, fieldnames=attributes)
                writer.writeheader()
                writer.writerow(node)
            else:
                # write node to the current writer
                writer.writerow(node)

            previous_node_name = node_name


def parse(file_path: str, output_dir: str) -> tuple:
    """Parse BulkCm file and place it's content in output directories CSV files

    Parameters:
        bulkcm file path (str): file_path
        output directory (str): output_dir

    Returns:
        bulkcm metadata, nodes and parsing duration (dict, [dict], timedelta): (metadata, nodes, duration)
    """

    print(f"Parsing {file_path}")
    if not (path.exists(file_path)):
        raise TeedException(f"Error, {file_path} doesn't exists")

    target_parser = BulkCmParser(output_dir)
    parser = etree.XMLParser(
        target=target_parser,
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
        metadata, nodes = etree.parse(file_path, parser)
    except etree.XMLSyntaxError as e:
        raise TeedException(e)

    # output the nodes list(dict) to the directory
    target_parser.nodes_to_csv(deepcopy(nodes), output_dir)

    finish = datetime.now()

    return (metadata, nodes, finish - start)


@program.command(name="parse")
def parse_program(file_path: str, output_dir: str) -> None:
    """Parse BulkCm file and place it's content in output directories CSV files

    Command-line program for bulkcm.parse function

    Parameters:
        bulkcm file path (str): file_path
        output directory (str): output_dir
    """

    try:
        metadata, nodes, duration = parse(file_path, output_dir)
        print(f"Duration: {duration}")
    except TeedException as e:
        typer.secho(f"Error parsing {file_path}")
        typer.secho(str(e), err=True, fg=typer.colors.RED, bold=True)
        exit(1)


def split_by_subnetwork(file_path: str) -> Generator[tuple, None, None]:
    """Split a BulkCm file by SubNetwork element
    Yields a (subnetwork_id, ElementTree) tupple for each SubNetwork element found in the BulkCm path file

    BulkCm XML format as described in ETSI TS 132 615
    https://www.etsi.org/deliver/etsi_ts/132600_132699/132615/09.02.00_60/ts_132615v090200p.pdf

    Parameters:
        file_path (str): file_path

    Yields:
        Tuple with the SubNetwork id and it's ElementTree: generator(sn_id, sn_tree)

    Raise:
        TeedException
    """

    print(f"\nSpliting the BulkCm file by SubNetwork: {file_path}")

    if not (path.exists(file_path)):
        raise TeedException(f"Error, {file_path} doesn't exists")

    try:
        parser = etree.XMLParser(
            no_network=True,
            ns_clean=True,
            remove_blank_text=True,
            remove_comments=True,
            remove_pis=True,
            huge_tree=True,
            recover=False,
        )
        tree = etree.parse(source=file_path, parser=parser)
    except etree.XMLSyntaxError as e:
        raise TeedException(e)

    root = tree.getroot()
    nsmap = root.nsmap

    cd = root.find("./configData", namespaces=nsmap)
    if cd is None:
        raise TeedException("Error, file doesn't have a configData element")

    h = root.find("./header", namespaces=nsmap)
    f = root.find("./footer", namespaces=nsmap)

    for sn in cd.iterfind("./xn:SubNetwork", namespaces=nsmap):
        sn_id = sn.get("id")

        sn_bulkcm = etree.Element(root.tag, attrib=root.attrib, nsmap=nsmap)
        sn_cd = etree.Element(cd.tag, attrib=cd.attrib, nsmap=nsmap)

        # header is optional
        if h is not None:
            sn_bulkcm.append(deepcopy(h))

        # configData is mandatory
        sn_bulkcm.append(sn_cd)

        # SubNetwork
        sn_cd.append(sn)

        # footer is optional
        if f is not None:
            sn_bulkcm.append(deepcopy(f))

        # a valid BulkCm ElementTree
        # with the SubNetwork
        sn_tree = etree.ElementTree(sn_bulkcm)

        yield (sn_id, sn_tree)

        sn = None


def split_by_subnetwork_to_file(
    file_path: str, output_dir: str
) -> Generator[tuple, None, None]:
    """Split a BulkCm file by SubNetwork element
    using the split_by_subnetwork function.

    Write the SubNetwork(s) ElementTree to new file(s).

    Parameters:
        bulkcm file path (str): file_path
        output directory (str): output_dir

    Yields:
        Tuple with the SubNetwork id and file path: generator(sn_id, sn_file_path)

    Raise:
        TeedException (inside the split_by_subnetwork call)
    """

    file_name = path.basename(file_path)
    file_name_without_ext = file_name.split(".")[0]
    file_ext = file_name.split(".")[1]

    for sn_id, sn in split_by_subnetwork(file_path):
        sn_file_path = path.normpath(
            f"{output_dir}{path.sep}{file_name_without_ext}_SubNetwork_{sn_id}.{file_ext}"
        )
        sn.write(
            sn_file_path,
            encoding=sn.docinfo.encoding,
            xml_declaration=True,
        )

        yield (sn_id, sn_file_path)


@program.command(name="split")
def split_program(file_path: str, output_dir: str) -> None:
    """Split a BulkCm file by SubNetwork element
    using the split_by_subnetwork function.

    Write the SubNetwork(s) ElementTree to new file(s).

    Command-line program for bulkcm.split function

    Parameters:
        bulkcm file path (str): file_path
        output directory (str): output_dir
    """

    sn_count = 0

    try:
        for sn_id, sn_file_path in split_by_subnetwork_to_file(file_path, output_dir):
            print(f"\nSubNetwork {sn_id} to {sn_file_path}")
            sn_count = +1

    except TeedException as e:
        typer.secho(str(e), err=True, fg=typer.colors.RED, bold=True)
        exit(1)

    print(f"\n#SubNetwork found: #{sn_count}")


def print_seq(items: list, indent: str = "\t") -> None:
    """Print items in sequence using a indent"""

    for item in items:
        print(f"{indent}{item[0]}: {item[1]}")


def probe(file_path: str) -> dict:
    """Probe a BulkCm file

    BulkCm XML format as described in ETSI TS 132 615
    https://www.etsi.org/deliver/etsi_ts/132600_132699/132615/09.02.00_60/ts_132615v090200p.pdf

    Prints to console the namespaces, SubNetwork and number of ManagedElement

    Parameters:
        file_path (str): file_path

    Returns:
        config data (dict): cd

    Raise:
        TeedException
    """

    print(f"Probing {file_path}")
    if not (path.exists(file_path)):
        raise TeedException(f"Error, {file_path} doesn't exists")

    try:

        tag_count = {"ManagementNode": 0, "MeContext": 0, "ManagedElement": 0}
        for event, element in etree.iterparse(
            file_path,
            tag=(
                "{*}bulkCmConfigDataFile",
                "{*}fileHeader",
                "{*}configData",
                "{*}SubNetwork",
                "{*}ManagementNode",
                "{*}MeContext",
                "{*}ManagedElement",
                "{*}fileFooter",
            ),
            events=(
                "start",
                "end",
            ),
            no_network=True,
            remove_blank_text=True,
            remove_comments=True,
            remove_pis=True,
            huge_tree=True,
            recover=False,
        ):

            localname = etree.QName(element.tag).localname

            if event == "start" and localname in [
                "ManagementNode",
                "MeContext",
                "ManagedElement",
            ]:
                tag_count[localname] += 1

            elif event == "start" and localname == "bulkCmConfigDataFile":
                print("\nbulkCmConfigDataFile")
                print_seq(element.nsmap.items(), "\t")

            elif event == "start" and localname == "configData":
                print("configData")
                print_seq(element.attrib.items(), "\t")

                cd = deepcopy(element.attrib)
                cd["SubNetwork(s)"] = []

            elif event == "start" and localname == "SubNetwork":
                print("\tSubNetwork")
                print_seq(element.attrib.items(), "\t")

                # SubNetwork id
                sn_id = element.attrib.get("id")

            elif event == "end" and localname == "SubNetwork":
                print_seq(tag_count.items(), "\t\t")

                sn_items = list(tag_count.items())
                cd["SubNetwork(s)"].append(f"SubNetwork {sn_id} with {sn_items}")

            elif event == "start" and localname == "fileHeader":
                print("fileHeader")
                print_seq(element.attrib.items(), "\t")

            elif event == "start" and localname == "fileFooter":
                print("fileFooter")
                print_seq(element.attrib.items(), "\t")

            element.clear(keep_tail=True)

    except etree.XMLSyntaxError as e:
        raise TeedException(e)

    return cd

    """
    try:
        parser = etree.XMLParser(
            no_network=True,
            ns_clean=True,
            remove_blank_text=True,
            remove_comments=True,
            remove_pis=True,
            huge_tree=True,
            recover=False,
        )
        tree = etree.parse(source=file_path, parser=parser)
    except etree.XMLSyntaxError as e:
        raise TeedException(e)

    # bulkCmConfigDataFile
    # fetch the namespaces mapping from bulkCmConfigDataFile root
    root = tree.getroot()
    nsmap = root.nsmap

    print("\nNamespaces found:")
    pprint(nsmap, indent=3)

    c = tree.find("configData", namespaces=nsmap)
    if c is None:
        raise TeedException("Error, file doesn't have a configData element")

    for c in tree.iterfind(".//configData", namespaces=nsmap):
        dnPrefix = c.get("dnPrefix")
        cd = {"dnPrefix": dnPrefix}
        cd["SubNetwork(s)"] = []
        print(f"\nconfigData, distinguished name prefix: {dnPrefix}")

        for sn in tree.iterfind(".//xn:SubNetwork", namespaces=nsmap):
            sn_id = sn.get("id")
            print(f"\tSubNetwork id: {sn_id}")

            me_count = 0
            for me in sn.iterfind(".//xn:ManagedElement", namespaces=nsmap):
                me_count += 1

            cd["SubNetwork(s)"].append(
                f"SubNetwork {sn_id} counting #{me_count} xn:ManagedElement"
            )
            print(f"\t#ManagedElement: {me_count}")

    return cd
    """


@program.command(name="probe")
def probe_program(file_path: str) -> None:
    """Probe a BulkCm file

    BulkCm XML format as described in ETSI TS 132 615
    https://www.etsi.org/deliver/etsi_ts/132600_132699/132615/09.02.00_60/ts_132615v090200p.pdf

    Prints to console the namespaces, SubNetwork and number of ManagedElement

    Command-line program for bulkcm.probe function

    Parameters:
        file_path (str): file_path
    """

    try:
        cd = probe(file_path)
        if cd is None:
            # the configData dict return by probe
            # can't be empty, otherwise it's an
            # invalid BulkCm file
            exit(1)
    except TeedException as e:
        typer.secho(str(e), err=True, fg=typer.colors.RED, bold=True)
        exit(1)


if __name__ == "__main__":
    program()
