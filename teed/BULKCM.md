# bulkcm

The BulkCm module supports probing, spliting and parsing of Bulkcm 32.615 specification, version 9.2.0 (2011-01) XML files:

```xml
<?xml version='1.0' encoding='UTF-8'?>
<bulkCmConfigDataFile xmlns="http://www.3gpp.org/ftp/specs/archive/32_series/32.615#configData"
    xmlns:xn="http://www.3gpp.org/ftp/specs/archive/32_series/32.625#genericNrm"
    xmlns:un="http://www.3gpp.org/ftp/specs/archive/32_series/32.645#utranNrm"
    xmlns:vsRHO11="http://www.companyNN.com/xmlschemas/NNRncHandOver.1.1">
    <configData dnPrefix="DC=a1.companyNN.com">
        <xn:SubNetwork id="1">
            <xn:ManagedElement id="1">
                <un:RncFunction id="1">
                    <xn:VsDataContainer id="1">
                        <xn:attributes>
                            <xn:vsDataType>vsDataRncHandOver</xn:vsDataType>
                            <xn:vsDataFormatVersion>NNRncHandOver.1.1</xn:vsDataFormatVersion>
                            <vsRHO11:vsDataRncHandOver>
                                <vsRHO11:abcMin>12</vsRHO11:abcMin>
                                <vsRHO11:abcMax>34</vsRHO11:abcMax>
                            </vsRHO11:vsDataRncHandOver>
                        </xn:attributes>
                    </xn:VsDataContainer>
                </un:RncFunction>
            </xn:ManagedElement>
        </xn:SubNetwork>
    </configData>
</bulkCmConfigDataFile>
```

Specification in ETSI and 3GPP:

https://www.etsi.org/deliver/etsi_ts/132600_132699/132615/09.02.00_60/ts_132615v090200p.pdf

https://portal.3gpp.org/desktopmodules/Specifications/SpecificationDetails.aspx?specificationId=2086

```shell
(env) joaomg@mypc:~/teed$ python -m teed bulkcm

Usage: teed bulkcm [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  parse  Parse BulkCm file and place its content in output directories CSV...
  probe  Probe a BulkCm file BulkCm XML format as described in ETSI TS 132...
  split  Split a BulkCm file by SubNetwork element using the...
(env) joaomg@mypc:~/teed$
```

Each bulkcm subcommand has distinct options.

Which can be explorer by calling the command with help.

```shell
(env) joaomg@mypc:~/teed$ python -m teed bulkcm probe --help

Usage: teed bulkcm probe [OPTIONS] FILE_PATH_OR_URI

  Probe a BulkCm file
  ...
```

To analyze a local file, probe its content:

```shell
(env) joaomg@mypc:~/teed$ python -m teed bulkcm probe data/bulkcm.xml

Probing data/bulkcm.xml
Searching for ManagementNode, MeContext, ManagedElement, ExternalGsmCell, ExternalUtranCell elements inside SubNetworks
file:///mnt/c/joaomg/teed/data/bulkcm.xml
{'encoding': 'UTF-8',
 'nsmap': {None: 'http://www.3gpp.org/ftp/specs/archive/32_series/32.615#configData',
           'xn': 'http://www.3gpp.org/ftp/specs/archive/32_series/32.625#genericNrm'},
 'fileHeader': None,
 'configData': [{'dnPrefix': 'DC=a1.companyNN.com',
                 'SubNetwork(s)': [{'id': '1',
                                    'ManagementNode': 1,
                                    'ManagedElement': 2}]}],
 'fileFooter': None}
Duration: 0:00:00.004816
```

The probe command accepts the relative and absolute path.

And also a URI, as defined in the [PyArrow FileSystem.from_uri method](https://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html#pyarrow.fs.FileSystem.from_uri).

Which can be used to analyze files in a remote file store, such as S3.

For example, to probe a file in a local MinIO instance:

```shell
(env) joaomg@mypc:~/teed$ python -m teed bulkcm probe "s3://1atuJoRDF8iy2BR40Yv6:6EKNs22XJvMX7RiXWMwW84xxO1ppnStkA6C6kEDh@data/bulkcm.xml?scheme=http&endpoint_override=localhost:9000"

Probing s3://1atuJoRDF8iy2BR40Yv6:6EKNs22XJvMX7RiXWMwW84xxO1ppnStkA6C6kEDh@data/bulkcm.xml?scheme=http&endpoint_override=localhost:9000
Searching for ManagementNode, MeContext, ManagedElement, ExternalGsmCell, ExternalUtranCell elements inside SubNetworks
{'encoding': 'UTF-8',
 'nsmap': {None: 'http://www.3gpp.org/ftp/specs/archive/32_series/32.615#configData',
           'xn': 'http://www.3gpp.org/ftp/specs/archive/32_series/32.625#genericNrm'},
 'fileHeader': None,
 'configData': [{'dnPrefix': 'DC=a1.companyNN.com',
                 'SubNetwork(s)': [{'id': '1',
                                    'ManagementNode': 1,
                                    'ManagedElement': 2}]}],
 'fileFooter': None}
Duration: 0:00:00.093180
```
