import os
from os import path
from lxml import etree, objectify
from datetime import datetime
from copy import deepcopy
from contextlib import ExitStack


def reverse_readline(filename, buf_size=8192):
    """
    A generator that returns the lines of a file in reverse order
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


in_file_path = "benchmark/UTRAN_TOPOLOGY_20171004.xml"
file_name = os.path.basename(in_file_path)

# read file footer
footer = []
for line in reverse_readline(in_file_path, 1024):
    line = line.strip()
    if not (line.startswith("</configData")):
        # footer
        footer.append(line)
    else:
        # configData found, break loop
        break

# extract the fileFooter dateTime attribute
# <fileFooter dateTime="2017-10-04T00:39:15Z"/>
footer_attrib = {"dateTime": None}
for line in footer:
    if line.startswith("<fileFooter"):
        footer_attrib["dateTime"] = line[line.index('"') + 1 : line.rindex('"')]
        break


def sn_writer(subnetwork_ids, bulkCmConfigDataFile, configData, fileHeader):
    element = None
    snw_id = subnetwork_ids[-1]
    with etree.xmlfile(
        f"tmp/UTRAN_TOPOLOGY_20171004_SubNetwork_{snw_id}.xml", encoding=encoding
    ) as xf:

        xf.write_declaration()
        with xf.element("bulkCmConfigDataFile", nsmap=bulkCmConfigDataFile.get("nsmap")):
            xf.write(etree.Element("fileHeader", attrib=fileHeader["attrib"]))
            with xf.element("configData", attrib=configData["attrib"]):

                # enter the xn:SubNetwork(s) 
                with ExitStack() as stack:
                    for sn_id in subnetwork_ids:
                        stack.enter_context(xf.element("xn:SubNetwork", id=sn_id))

                    try:
                        while True:
                            element = yield
                            xf.write(element)
                            xf.flush()
                    except GeneratorExit:
                        # called on sn_w.close()
                        # needed to resume sn_writer
                        # write the fileFooter element
                        # below
                        pass

            xf.write(etree.Element("fileFooter", attrib=footer_attrib))


start = datetime.now()

with open("benchmark/UTRAN_TOPOLOGY_20171004.xml", mode="rb") as in_stream:

    bulkCmConfigDataFile = None
    configData = None
    fileHeader = None
    fileFooter = None
    subnetwork_ids = []
    sn_w = None

    for event, element in etree.iterparse(
        in_stream,
        events=(
            "start",
            "end",
        ),
        tag=(
            "{*}bulkCmConfigDataFile",
            "{*}fileHeader",
            "{*}configData",
            "{*}SubNetwork",
            "{*}MeContext",
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

        if event == "end" and localName == "MeContext":
            sn_w.send(element)

        elif event == "start" and localName == "SubNetwork":
            sn_id = element.attrib.get("id")
            subnetwork_ids.append(sn_id)

            print(sn_id)

            if sn_w is not None:
                sn_w.close()

            sn_w = sn_writer(subnetwork_ids, bulkCmConfigDataFile, configData, fileHeader)
            next(sn_w)

        elif event == "end" and localName == "SubNetwork":
            sn_id = subnetwork_ids.pop()

        elif event == "start" and localName == "bulkCmConfigDataFile":
            encoding = (element.getroottree()).docinfo.encoding
            bulkCmConfigDataFile = {
                "tag": element.tag,
                "attrib": element.attrib,
                "nsmap": element.nsmap,
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
            element.clear(keep_tail=True)

    finish = datetime.now()

    duration = finish - start

    print(duration)