import os
import csv
import datetime
from io import TextIOWrapper

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as fs
import pytest
from lxml import etree

from teed import bulkcm, meas

# file store credentials
# create/fetch in MinIO
ACCESS_KEY = os.environ.get("ACCESS_KEY")
SECRET_KEY = os.environ.get("SECRET_KEY")

try:
    # access the local MinIO instance
    # with permission to create buckets
    # allow_bucket_creation
    ofs = fs.S3FileSystem(
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        scheme="http",
        endpoint_override="localhost:9000",
        allow_bucket_creation=True,
    )
    ofs.create_dir("data")
    filestore_running = True
except Exception as e:
    # file store isn't available in localhost:9000
    filestore_running = False

    # output the Exception, just to understand what went wrong...
    # place a failed assert somewhere below to see this
    print(e)

# output to MinIO file store
ofs = fs.S3FileSystem(
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    scheme="http",
    endpoint_override="localhost:9000",
)


@pytest.mark.skipif(
    filestore_running == False,
    reason="Needs the MinIO file store running in http://localhost:9000",
)
def test_bulkcm_probe_file_from_file_store():
    # probe the data/bulkcm.xml file in MinIO localhost:9000

    # delete remote file and recreate it
    try:
        ofs.delete_file("data/bulkcm.xml")
    except FileNotFoundError:
        pass

    fs.copy_files(
        "./data/bulkcm.xml",
        "data/bulkcm.xml",
        source_filesystem=fs.LocalFileSystem(),
        destination_filesystem=ofs,
    )

    # probe remote file
    assert bulkcm.probe(
        f"s3://{ACCESS_KEY}:{SECRET_KEY}@data/bulkcm.xml?scheme=http&endpoint_override=localhost:9000"
    ) == {
        "encoding": "UTF-8",
        "nsmap": {
            None: "http://www.3gpp.org/ftp/specs/archive/32_series/32.615#configData",
            "xn": "http://www.3gpp.org/ftp/specs/archive/32_series/32.625#genericNrm",
        },
        "fileHeader": None,
        "configData": [
            {
                "dnPrefix": "DC=a1.companyNN.com",
                "SubNetwork(s)": [{"id": "1", "ManagementNode": 1, "ManagedElement": 2}],
            }
        ],
        "fileFooter": None,
    }


@pytest.mark.skipif(
    filestore_running == False,
    reason="Needs the MinIO file store running in http://localhost:9000",
)
def test_bulkcm_split_output_from_and_to_file_store():
    # input file is a URI
    # on the file store
    input_uri = f"s3://{ACCESS_KEY}:{SECRET_KEY}@data/bulkcm.xml?scheme=http&endpoint_override=localhost:9000"

    # create filesystem from data bucket URI
    ifs, _ = fs.FileSystem.from_uri(input_uri)

    # delete remote file data/bulkcm.xml
    # and copy from local filesystem
    try:
        ifs.delete_file("data/bulkcm.xml")
    except FileNotFoundError:
        pass

    fs.copy_files(
        "./data/bulkcm.xml",
        "data/bulkcm.xml",
        source_filesystem=fs.LocalFileSystem(),
        destination_filesystem=ifs,
    )

    # output split xml to directory
    # bulkcm.xml-split-output in bucket data
    # on the localhost:9000 file store
    output_uri = f"s3://{ACCESS_KEY}:{SECRET_KEY}@data/bulkcm.xml-split-output?scheme=http&endpoint_override=localhost:9000"

    # create filesystem from data bucket URI
    ofs, _ = fs.FileSystem.from_uri(output_uri)

    # delete remote dir and contents
    try:
        ofs.delete_file("data/bulkcm.xml-split-output")
    except FileNotFoundError:
        pass

    # split bulkcm.xml
    # considered all/any SubNetwork
    sn_ids, sn_file_paths = bulkcm.split(input_uri, output_uri)

    sn_id = sn_ids.pop()
    sn_file_path = sn_file_paths.pop()

    assert sn_id == "1"
    assert sn_file_path == "data/bulkcm.xml-split-output/bulkcm_1.xml"
    assert (ofs.get_file_info(sn_file_path)).type == fs.FileType.File

    # compare contents with the input
    # they must be the same since there's
    # only a SubNetwork in bulkcm.xml
    # using ns_clean we ignore the extra ns
    # placed in the SubNetwork elements
    parser = etree.XMLParser(
        no_network=True,
        ns_clean=True,
        remove_blank_text=True,
        remove_comments=True,
        remove_pis=True,
        huge_tree=True,
        recover=False,
    )
    source = etree.parse("data/bulkcm.xml", parser=parser)
    target = etree.parse(ofs.open_input_stream(sn_file_path), parser=parser)

    assert etree.tostring(source) == etree.tostring(target)


@pytest.mark.skipif(
    filestore_running == False,
    reason="Needs the MinIO file store running in http://localhost:9000",
)
def test_bulkcm_split_by_subnetwork_output_to_file_store():
    output_uri = f"s3://{ACCESS_KEY}:{SECRET_KEY}@data/bulkcm.xml-split-output?scheme=http&endpoint_override=localhost:9000"

    # create filesystem from data bucket URI
    ofs, _ = fs.FileSystem.from_uri(output_uri)

    # delete remote dir and contents
    try:
        ofs.delete_file("data/bulkcm.xml-split-output")
    except FileNotFoundError:
        pass

    # split bulkcm.xml
    # considered all/any SubNetwork
    for sn_id, sn_file_path in bulkcm.split_by_subnetwork("data/bulkcm.xml", output_uri):
        assert sn_id == "1"
        assert sn_file_path == "data/bulkcm.xml-split-output/bulkcm_1.xml"
        assert (ofs.get_file_info(sn_file_path)).type == fs.FileType.File

    # compare contents with the input
    # they must be the same since there's
    # only a SubNetwork in bulkcm.xml
    # using ns_clean we ignore the extra ns
    # placed in the SubNetwork elements
    parser = etree.XMLParser(
        no_network=True,
        ns_clean=True,
        remove_blank_text=True,
        remove_comments=True,
        remove_pis=True,
        huge_tree=True,
        recover=False,
    )
    source = etree.parse("data/bulkcm.xml", parser=parser)
    target = etree.parse(ofs.open_input_stream(sn_file_path), parser=parser)

    assert etree.tostring(source) == etree.tostring(target)


@pytest.mark.skipif(
    filestore_running == False,
    reason="Needs the MinIO file store running in http://localhost:9000",
)
def test_bulkcm_parse_output_to_csv_file_store():
    # output to MinIO file store
    ofs = fs.S3FileSystem(
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        scheme="http",
        endpoint_override="localhost:9000",
    )

    # delete data/bulkcm_with_utrancell
    try:
        ofs.delete_dir("data/bulkcm_with_utrancell")
    except:
        pass

    # create data/bulkcm_with_utrancell
    try:
        ofs.create_dir("data/bulkcm_with_utrancell")
    except:
        pass

    # output csv data to S3 MinIO file store
    stream = bulkcm.BulkCmParser.stream_to_csv(
        "data/bulkcm_with_utrancell", output_fs=ofs
    )

    # ignoring metadata and duration
    _, _ = bulkcm.parse(
        "data/bulkcm_with_utrancell.xml",
        "data/bulkcm_with_utrancell",
        stream,
        output_fs=ofs,
    )

    with ofs.open_input_stream(
        "data/bulkcm_with_utrancell/vsDataUtranCell-762627b0939d1ac04dadef2b58f194c1.csv"
    ) as csv_stream:
        with TextIOWrapper(csv_stream) as csv_buffer:
            reader = csv.DictReader(csv_buffer)
            assert reader.fieldnames == [
                "SubNetwork",
                "ManagedElement",
                "RncFunction",
                "vsDataUtranCell",
                "sc",
                "pcpichpower",
            ]
            assert list(reader) == [
                {
                    "SubNetwork": "1",
                    "ManagedElement": "2",
                    "RncFunction": "3",
                    "vsDataUtranCell": "Cell4",
                    "sc": "111",
                    "pcpichpower": "222",
                }
            ]

    with ofs.open_input_stream(
        "data/bulkcm_with_utrancell/vsDataRncHandOver-8ebadf34e92ad0fb468fd0bed0aab89d.csv"
    ) as csv_stream:
        with TextIOWrapper(csv_stream) as csv_buffer:
            reader = csv.DictReader(csv_buffer)
            assert reader.fieldnames == [
                "SubNetwork",
                "ManagedElement",
                "RncFunction",
                "vsDataUtranCell",
                "vsDataRncHandOver",
                "abcMin",
                "abcMax",
            ]
            assert list(reader) == [
                {
                    "SubNetwork": "1",
                    "ManagedElement": "2",
                    "RncFunction": "3",
                    "vsDataUtranCell": "Cell4",
                    "vsDataRncHandOver": "5",
                    "abcMin": "12",
                    "abcMax": "34",
                }
            ]


@pytest.mark.skipif(
    filestore_running == False,
    reason="Needs the MinIO file store running in http://localhost:9000",
)
def test_meas_parse_output_to_parquet_file_store():
    """
    Use parquet consume method in meas.parse.

    Output parquet files to a local running MinIO filestore.

    By default it partitions the data using the granularity period.

    But in this test we also partition by Node.
    """

    pathname = "data/mdc*.xml"
    output_dir_or_bucket = "data"

    # output to MinIO file store
    ofs = fs.S3FileSystem(
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        scheme="http",
        endpoint_override="localhost:9000",
    )

    # partition by time
    try:
        ofs.delete_dir("data/UtranCell-900")
    except FileNotFoundError:
        pass

    meas.parse(
        pathname,
        output_dir_or_bucket,
        recursive=False,
        consume=meas.consume_ldn_natural_key_to_parquet,
        consume_kwargs={
            "nedn_ignore_before": "SubNetwork",
            "ldn_ignore_before": "SubNetwork",
            "node_expression": None,
            "node_partition_by": False,
            "output_fs": ofs,
        },
    )

    # read the UtranCell-900 dataset using the expected partitioning method
    dataset = ds.dataset(
        "data/UtranCell-900",
        format="parquet",
        filesystem=ofs,
        partitioning=ds.partitioning(
            pa.schema(
                [
                    pa.field("day", pa.date32()),
                    pa.field("hh", pa.uint8()),
                    pa.field("mm", pa.uint8()),
                ]
            )
        ),
    )
    table = dataset.to_table()

    # assert table schema
    table_schema = [field.strip() for field in str(table.schema).split("\n")]
    assert table_schema == [
        "SubNetwork: string",
        "MeContext: string",
        "ManagedElement: string",
        "RncFunction: string",
        "UtranCell: string",
        "attTCHSeizures: int64",
        "succTCHSeizures: int64",
        "attImmediateAssignProcs: int64",
        "succImmediateAssignProcs: int64",
        "day: date32[day]",
        "hh: uint8",
        "mm: uint8",
    ]

    # assert table data
    assert table.to_pylist() == [
        {
            "SubNetwork": "CountryNN",
            "MeContext": "MEC-Gbg1",
            "ManagedElement": "RNC-Gbg-1",
            "RncFunction": "RF-1",
            "UtranCell": "Gbg-997",
            "attTCHSeizures": 234,
            "succTCHSeizures": 345,
            "attImmediateAssignProcs": 567,
            "succImmediateAssignProcs": 789,
            "day": datetime.date(2021, 3, 1),
            "hh": 14,
            "mm": 15,
        },
        {
            "SubNetwork": "CountryNN",
            "MeContext": "MEC-Gbg1",
            "ManagedElement": "RNC-Gbg-1",
            "RncFunction": "RF-1",
            "UtranCell": "Gbg-998",
            "attTCHSeizures": 890,
            "succTCHSeizures": 901,
            "attImmediateAssignProcs": 123,
            "succImmediateAssignProcs": 234,
            "day": datetime.date(2021, 3, 1),
            "hh": 14,
            "mm": 15,
        },
        {
            "SubNetwork": "CountryNN",
            "MeContext": "MEC-Gbg1",
            "ManagedElement": "RNC-Gbg-1",
            "RncFunction": "RF-1",
            "UtranCell": "Gbg-999",
            "attTCHSeizures": 456,
            "succTCHSeizures": 567,
            "attImmediateAssignProcs": 678,
            "succImmediateAssignProcs": 789,
            "day": datetime.date(2021, 3, 1),
            "hh": 14,
            "mm": 15,
        },
    ]

    # replace nedn by Node expression
    # partition time
    try:
        ofs.delete_dir("data/UtranCell-900")
    except FileNotFoundError:
        pass

    meas.parse(
        pathname,
        output_dir_or_bucket,
        recursive=False,
        consume=meas.consume_ldn_natural_key_to_parquet,
        consume_kwargs={
            "nedn_ignore_before": "SubNetwork",
            "ldn_ignore_before": "SubNetwork",
            "node_expression": "nedn_dict.pop('ManagedElement')",
            "node_partition_by": False,
            "output_fs": ofs,
        },
    )

    # read the UtranCell-900 dataset using the expected partitioning method
    dataset = ds.dataset(
        "data/UtranCell-900",
        format="parquet",
        filesystem=ofs,
        partitioning=ds.partitioning(
            pa.schema(
                [
                    pa.field("day", pa.date32()),
                    pa.field("hh", pa.uint8()),
                    pa.field("mm", pa.uint8()),
                ]
            )
        ),
    )
    table = dataset.to_table()

    # assert table schema
    table_schema = [field.strip() for field in str(table.schema).split("\n")]
    assert table_schema == [
        "Node: string",
        "RncFunction: string",
        "UtranCell: string",
        "attTCHSeizures: int64",
        "succTCHSeizures: int64",
        "attImmediateAssignProcs: int64",
        "succImmediateAssignProcs: int64",
        "day: date32[day]",
        "hh: uint8",
        "mm: uint8",
    ]

    # assert table data
    assert table.to_pylist() == [
        {
            "Node": "RNC-Gbg-1",
            "RncFunction": "RF-1",
            "UtranCell": "Gbg-997",
            "attTCHSeizures": 234,
            "succTCHSeizures": 345,
            "attImmediateAssignProcs": 567,
            "succImmediateAssignProcs": 789,
            "day": datetime.date(2021, 3, 1),
            "hh": 14,
            "mm": 15,
        },
        {
            "Node": "RNC-Gbg-1",
            "RncFunction": "RF-1",
            "UtranCell": "Gbg-998",
            "attTCHSeizures": 890,
            "succTCHSeizures": 901,
            "attImmediateAssignProcs": 123,
            "succImmediateAssignProcs": 234,
            "day": datetime.date(2021, 3, 1),
            "hh": 14,
            "mm": 15,
        },
        {
            "Node": "RNC-Gbg-1",
            "RncFunction": "RF-1",
            "UtranCell": "Gbg-999",
            "attTCHSeizures": 456,
            "succTCHSeizures": 567,
            "attImmediateAssignProcs": 678,
            "succImmediateAssignProcs": 789,
            "day": datetime.date(2021, 3, 1),
            "hh": 14,
            "mm": 15,
        },
    ]

    # replace nedn by Node expression
    # partition by Node and time
    try:
        ofs.delete_dir("data/UtranCell-Node-900")
    except FileNotFoundError:
        pass

    meas.parse(
        pathname,
        output_dir_or_bucket,
        recursive=False,
        consume=meas.consume_ldn_natural_key_to_parquet,
        consume_kwargs={
            "nedn_ignore_before": "SubNetwork",
            "ldn_ignore_before": "SubNetwork",
            "node_expression": "nedn_dict.pop('ManagedElement')",
            "node_partition_by": True,
            "output_fs": ofs,
        },
    )

    # read the UtranCell-900 dataset using the expected partitioning method
    dataset = ds.dataset(
        "data/UtranCell-Node-900",
        format="parquet",
        filesystem=ofs,
        partitioning=ds.partitioning(
            pa.schema(
                [
                    pa.field("Node", pa.string()),
                    pa.field("day", pa.date32()),
                    pa.field("hh", pa.uint8()),
                    pa.field("mm", pa.uint8()),
                ]
            )
        ),
    )
    table = dataset.to_table()

    # assert table schema
    table_schema = [field.strip() for field in str(table.schema).split("\n")]
    assert table_schema == [
        "RncFunction: string",
        "UtranCell: string",
        "attTCHSeizures: int64",
        "succTCHSeizures: int64",
        "attImmediateAssignProcs: int64",
        "succImmediateAssignProcs: int64",
        "Node: string",
        "day: date32[day]",
        "hh: uint8",
        "mm: uint8",
    ]

    # assert table data
    assert table.to_pylist() == [
        {
            "RncFunction": "RF-1",
            "UtranCell": "Gbg-997",
            "attTCHSeizures": 234,
            "succTCHSeizures": 345,
            "attImmediateAssignProcs": 567,
            "succImmediateAssignProcs": 789,
            "Node": "RNC-Gbg-1",
            "day": datetime.date(2021, 3, 1),
            "hh": 14,
            "mm": 15,
        },
        {
            "RncFunction": "RF-1",
            "UtranCell": "Gbg-998",
            "attTCHSeizures": 890,
            "succTCHSeizures": 901,
            "attImmediateAssignProcs": 123,
            "succImmediateAssignProcs": 234,
            "Node": "RNC-Gbg-1",
            "day": datetime.date(2021, 3, 1),
            "hh": 14,
            "mm": 15,
        },
        {
            "RncFunction": "RF-1",
            "UtranCell": "Gbg-999",
            "attTCHSeizures": 456,
            "succTCHSeizures": 567,
            "attImmediateAssignProcs": 678,
            "succImmediateAssignProcs": 789,
            "Node": "RNC-Gbg-1",
            "day": datetime.date(2021, 3, 1),
            "hh": 14,
            "mm": 15,
        },
    ]
