#
# python bulkcm.py data/bulkcm.xml
# python teed/bulkcm.py data/bulkcm_with_header_footer.xml data
# python -m teed bulkcm-parse data/bulkcm_with_header_footer.xml data
# python -m teed bulkcm-parse data/bulkcm_with_vsdatacontainer.xml data

import sys
from os import path
from copy import deepcopy
from pprint import pprint
from lxml import etree
import csv


class BulkCmParser:
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
        self._is_previous_format_version = False

    def start(self, tag, attrib):
        # remove the namespace from the tag
        # tag = {http://www.3gpp.org/ftp/specs/archive/32_series/32.615#configData1}configData
        # tag_nons = configData
        tag_nons = tag.split("}")[-1]

        if tag_nons == "attributes":
            # <xn:attributes>
            self._is_attributes = True

        elif tag_nons == "vsDataType":
            pass

        elif tag_nons == "vsDataFormatVersion":
            pass

        elif tag_nons == "configData":
            # <configData dnPrefix="DC=a1.companyNN.com">
            self._dnPrefix = attrib.get("dnPrefix")

        elif tag_nons == "fileHeader":
            # <fileHeader fileFormatVersion="32.615 V4.0" senderName="DC=a1.companyNN.com,SubNetwork=1,IRPAgent=1" vendorName="Company NN" />
            self._metadata.update(attrib)

        elif tag_nons == "fileFooter":
            # <fileFooter dateTime="2001-05-07T12:00:00+02:00"/>
            self._metadata.update(attrib)

        elif tag_nons == "bulkCmConfigDataFile":
            pass

        elif len(attrib) > 0:
            self._nodes.append(
                {"node_id": attrib.get("id").strip(), "node_name": tag_nons}
            )
            self._node_queue.append(tag_nons)

            if tag_nons == "VsDataContainer":
                self._is_vs_data = True

        else:
            self._node_queue.append(tag_nons)

    def end(self, tag):
        tag_nons = tag.split("}")[-1]

        if tag_nons == "attributes":
            # </xn:attributes>
            # not a attribute, tag_nons is a node
            self._nodes[-1].update(self._node_attributes)
            self._node_attributes = {}

            self._is_attributes = False

        elif tag_nons == "vsDataType":
            # replace the previous node_name
            self._nodes[-1]["node_name"] = "".join(self._text)
            self._text = []

        elif tag_nons == "vsDataFormatVersion":
            self._text = []

        elif tag_nons == "configData":
            pass

        elif tag_nons == "fileHeader":
            pass

        elif tag_nons == "fileFooter":
            pass

        elif tag_nons == "bulkCmConfigDataFile":
            pass

        elif tag_nons == "VsDataContainer":
            self._is_vs_data = False
            vs_container = (self._nodes[-1]).popitem()
            pprint(self._nodes[-1])
            pprint(vs_container)

        else:
            node = self._node_queue.pop()

            if self._is_attributes and self._is_vs_data:
                # inside <xn:VsDataContainer id="1"><xn:attributes>, tag_nons is an attribute
                self._node_attributes[f"vs_{node}"] = "".join(self._text)

            elif self._is_attributes:
                # inside <xn:attributes>, tag_nons is an attribute
                self._node_attributes[node] = "".join(self._text)

            self._text = []

    def data(self, data):
        self._text.append(data.strip())

    def close(self):
        print("metadata")
        pprint(self._metadata)
        print("dnPrefix")
        pprint(self._dnPrefix)
        print("nodes")
        pprint(self._nodes)

        return self._metadata, self._nodes


def nodes_to_csv(nodes, output_dir):
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

            attributes = node.keys()
            writer = csv.DictWriter(csv_file, fieldnames=attributes)
            writer.writeheader()
            writer.writerow(node)
        else:
            # write node to the current writer
            writer.writerow(node)

        previous_node_name = node_name


def to_csv(file_path: str, output_dir: str):
    """ Parse BulkCm file and place it's content in output directories CSV files """

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

    try:
        # parse the BulkCm file
        metadata, nodes = etree.parse(file_path, parser)

        # output the nodes list(dict) to the directory
        nodes_to_csv(deepcopy(nodes), output_dir)

        return metadata, nodes

    except etree.XMLSyntaxError as e:
        print(f"Error parsing {file_path}")
        print(e)


def split_by_subnetwork(file_path: str):
    """Split a BulkCm file by SubNetwork element
    Yields a (subnetwork_id, ElementTree) tupple for each SubNetwork element found in the BulkCm path file.
    The SubNetwork ElementTree's contains

    BulkCm XML format as described in ETSI TS 132 615
    https://www.etsi.org/deliver/etsi_ts/132600_132699/132615/09.02.00_60/ts_132615v090200p.pdf

    Parameters:
        file_path (str): file_path

    Yields:
        gen(subnetwork_id, ElementTree): list stream
    """

    print(f"\nSpliting the BulkCm file by SubNetwork: {file_path}")

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
        print(e)
        exit(1)

    root = tree.getroot()
    nsmap = root.nsmap

    cd = root.find("./configData", namespaces=nsmap)
    if cd is None:
        print("Error, file doesn't have a configData element.")
        exit(1)

    h = root.find("./header", namespaces=nsmap)
    f = root.find("./footer", namespaces=nsmap)

    for sn in cd.iterfind("./xn:SubNetwork", namespaces=nsmap):
        sn_id = sn.get("id")

        new_bulkcm = etree.Element(root.tag, attrib=root.attrib, nsmap=nsmap)
        new_cd = etree.Element(cd.tag, attrib=cd.attrib, nsmap=nsmap)

        # header is optional
        if h is not None:
            new_bulkcm.append(deepcopy(h))

        # configData is mandatory
        new_bulkcm.append(new_cd)

        # SubNetwork
        new_cd.append(sn)

        # footer is optional
        if f is not None:
            new_bulkcm.append(deepcopy(f))

        new_tree = etree.ElementTree(new_bulkcm)

        yield (sn_id, new_tree)


def split_by_subnetwork_to_file(file_path: str, output_dir: str = "."):
    """Split a BulkCm file by SubNetwork element
    using the split_by_subnetwork function.

    Write the SubNetwork(s) ElementTree to new file(s).

    Parameters:
        file_path (str): file_path

    Yields:
        gen(file_path): list stream
    """

    file_name = path.basename(file_path)
    file_name_without_ext = file_name.split(".")[0]
    file_ext = file_name.split(".")[1]
    # dir_name = path.dirname(file_path)

    sn_count = 0
    for sn_id, sn in split_by_subnetwork(file_path):
        sn_file_path = path.normpath(
            f"{output_dir}{path.sep}{file_name_without_ext}_SubNetwork_{sn_id}.{file_ext}"
        )
        sn.write(
            sn_file_path,
            encoding=sn.docinfo.encoding,
            xml_declaration=True,
        )

        print(f"\nSubNetwork {sn_id} to {sn_file_path}")
        sn_count = +1

        yield file_path

    print(f"\n#SubNetwork found: #{sn_count}")


def probe(file_path: str):
    """Probe a BulkCm file

    BulkCm XML format as described in ETSI TS 132 615
    https://www.etsi.org/deliver/etsi_ts/132600_132699/132615/09.02.00_60/ts_132615v090200p.pdf

    Prints to console the namespaces, SubNetwork and number of ManagedElement

    Parameters:
        file_path (str): file_path
    Returns:
        config data (dict): cd
    """

    print(f"\nProbing the BulkCm file: {file_path}")

    try:
        parser = etree.XMLParser(remove_blank_text=True)
        tree = etree.parse(source=file_path, parser=parser)
    except etree.XMLSyntaxError as e:
        print(e)
        exit(1)

    # bulkCmConfigDataFile
    # fetch the namespaces mapping from bulkCmConfigDataFile root
    root = tree.getroot()
    nsmap = root.nsmap

    print("\nNamespaces:")
    pprint(nsmap, indent=3)

    c = tree.find("configData", namespaces=nsmap)
    if c is None:
        print("Error, file doesn't have a configData element.")
        exit(1)

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


if __name__ == "__main__":

    print(f"\nExecuting {sys.argv[0]}")
    print(f"Number of arguments: {len(sys.argv)} arguments")
    print(f"Argument List: {sys.argv}")

    file_path = sys.argv[1]
    output_dir = sys.argv[2]

    #
    # Parse BulkCm file and place it's content in output directories CSV files
    #
    to_csv(file_path, output_dir)

    #
    # Simple probing of an BulkCm file
    #
    # probe(file_path)

    #
    # Split each SubNetwork to a distinct lxml.etreeElementTree
    #
    # dir_name = path.dirname(file_path)
    # split_by_subnetwork_to_file(file_path, dir_name)
