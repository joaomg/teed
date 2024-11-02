# meas

## Description

The meas module deliveres parsing of 32.401 specification, version 5.5.0 (2005-03) XML files.

Currently implementing for the DTD based XML format:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="MeasDataCollection.xsl"?>
<!DOCTYPE mdc SYSTEM "MeasDataCollection.dtd">
<mdc>
    <mfh>
        ...
    </mfh>
    <md>
        ...
    </md>
    ...
    <md>
        ...
    </md>
    <mff>
        <ts>...</ts>
    </mff>
</mdc>
```

### Files samples retrieved

https://www.etsi.org/deliver/etsi_ts/132400_132499/132401/05.05.00_60/ts_132401v050500p.pdf

### The latest specification

https://www.etsi.org/deliver/etsi_ts/132400_132499/132401/16.00.00_60/ts_132401v160000p.pdf

The file format DTD XML file format is still valid.

```shell
(env) joaomg@mypc:~/teed$ python -m teed meas
Usage: teed meas [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  parse  Parse Mdc files returneb by pathname glob and place it's...
(env) joaomg@mypc:~/teed$
```

### Parse is the only meas subcommand

```shell
(env) joaomg@mypc:~/teed$ python -m teed meas parse --help
Usage: teed meas parse [OPTIONS] PATHNAME OUTPUT_DIR_OR_BUCKET

  Parse Mdc files returneb by pathname glob and

  place it's content in output directories CSV files

  Command-line program for meas.parse function

  Parameters: meas/mdc pathname glob (str): pathname search files
  recursively in subdirectories (bool): recursive output directory (or bucket for object storage)
  (str): output_dir_or_bucket

Arguments:
  PATHNAME    [required]
  OUTPUT_DIR_OR_BUCKET  [required]

Options:
  --recursive / --no-recursive  [default: False]
  --help                        Show this message and exit.
(env) joaomg@mypc:~/teed$
```

### Parsing a single file

```shell
env) joaomg@mypc:~/teed$ python -m teed meas parse data/mdc_c3_1.xml data
Producer starting 8170
Parsing data/mdc_c3_1.xml
Consumer starting 8171
Placing UtranCell
Append data/UtranCell-900-9995823c30bcf308b91ab0b66313e86a.csv
Producer and consumer done, exiting.
Duration(s): 0.011379859000044235
```

### Parsing all mdc\*xml files and output CSVs to the same directory (data)

```shell
(env) joaomg@mypc:~/teed$ python -m teed meas parse "data/mdc*xml" data
Producer starting 8203
Parsing data/mdc_c3_2.xml
Consumer starting 8204
Placing UtranCell
Parsing data/mdc_c3_1.xml
Append data/UtranCell-900-9995823c30bcf308b91ab0b66313e86a.csv
Placing UtranCell
Producer and consumer done, exiting.
Duration(s): 0.011365840000507887
(env) joaomg@mypc:~/teed$
```

### Using the meas module

Parse all mdc\*xml files in data directory output the CSV files to the same directory.

By default the consumer, the method that writes the output files, outputs the resulting data to CSV.

```python
>>> from teed import meas
>>> meas.parse("data/mdc*xml", "data")
Producer starting 143
Consumer starting 145
Parsing data/mdc_c3_1.xml
Placing UtranCell
Parsing data/mdc_c3_2.xml
Placing UtranCell
Append data/UtranCell-900-9995823c30bcf308b91ab0b66313e86a.csv
Producer and consumer done, exiting.
```

Parse all mdc\*xml files in data directory and output to Parquet, partitioning by time (gp defines the partition structure).

300 -> day | hour | minute (every 5 minutes)
900 -> day | hour | minute (every 15 minutes)
3600 -> day | hour
86400 -> day

```python
>>> from teed import meas
>>> meas.parse("data/mdc_c3_1.xml", "data", recursive=False, consume=meas.consume_ldn_natural_key_to_parquet)
Producer starting 1464
Consumer starting 1474
Parsing data/mdc_c3_1.xml
Placing UtranCell
path=data/UtranCell-900/ef8cafe693802c8e82481c808313c4c4-0.parquet
size=4026 bytes
metadata=<pyarrow._parquet.FileMetaData object at 0x7f2db8dfc2c0>
  created_by: parquet-cpp-arrow version 11.0.0
  num_columns: 10
  num_rows: 3
  num_row_groups: 1
  format_version: 1.0
  serialized_size: 0
Producer and consumer done, exiting.
```

Parse all mdc\*xml files in data directory and output to Parquet, partitioning by node and time.

Create a Node column from the NEDN data. Identify the network node by this column.

```python
>>> from teed import meas
>>> meas.parse(
    "data/mdc*xml",
    "data",
    recursive=False,
    consume=meas.consume_ldn_natural_key_to_parquet,
    consume_kwargs={
        "nedn_ignore_before": "SubNetwork",
        "ldn_ignore_before": "SubNetwork",
        "node_expression": "nedn_dict.pop('ManagedElement')",
        "node_partition_by": True,
    },
)
>>> import pyarrow as pa
>>> import pyarrow.dataset as ds
>>> dataset = ds.dataset(
    "data/UtranCell-Node-900",
    format="parquet",
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
>>> table = dataset.to_table()
>>> print(table)
pyarrow.Table
RncFunction: string
UtranCell: string
attTCHSeizures: uint32
succTCHSeizures: uint32
attImmediateAssignProcs: uint32
succImmediateAssignProcs: uint32
Node: string
day: date32[day]
hh: uint8
mm: uint8
----
RncFunction: [["RF-1","RF-1","RF-1"]]
UtranCell: [["Gbg-997","Gbg-998","Gbg-999"]]
attTCHSeizures: [[234,890,456]]
succTCHSeizures: [[345,901,567]]
attImmediateAssignProcs: [[567,123,678]]
succImmediateAssignProcs: [[789,234,789]]
Node: [["RNC-Gbg-1","RNC-Gbg-1","RNC-Gbg-1"]]
day: [[2021-03-01,2021-03-01,2021-03-01]]
hh: [[14,14,14]]
mm: [[15,15,15]]
>>>
```

## References

### Performance measurement: File format definition

(3GPP TS 32.432 version 16.0.0 Release 16)

https://www.etsi.org/deliver/etsi_ts/132400_132499/132432/16.00.00_60/ts_132432v160000p.pdf

### Performance measurement: eXtensible Markup Language (XML) file format definition

(3GPP TS 32.435 version 16.0.0 Release 16)

https://www.etsi.org/deliver/etsi_ts/132400_132499/132435/16.00.00_60/ts_132435v160000p.pdf

### Performance measurement: Abstract Syntax Notation 1 (ASN.1) file format definition

(3GPP TS 32.436 version 16.0.0 Release 16)

https://www.etsi.org/deliver/etsi_ts/132400_132499/132436/16.00.00_60/ts_132436v160000p.pdf
