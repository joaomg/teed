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
env) joaomg@mypc:~/teed$ python -m teed bulkcm
Usage: teed bulkcm [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  parse  Parse BulkCm file and place it's content in output directories CSV...
  probe  Probe a BulkCm file BulkCm XML format as described in ETSI TS 132...
  split  Split a BulkCm file by SubNetwork element using the...
(env) joaomg@mypc:~/teed$ 
```

Each bulkcm subcommand has distinct options:

```shell
(env) joaomg@mypc:~/teed$ python -m teed bulkcm probe --help
Usage: teed bulkcm probe [OPTIONS] FILE_PATH

  Probe a BulkCm file

  BulkCm XML format as described in ETSI TS 132 615 https://www.etsi.org/del
  iver/etsi_ts/132600_132699/132615/09.02.00_60/ts_132615v090200p.pdf

  Prints to console the namespaces, SubNetwork and number of ManagedElement

  Command-line program for bulkcm.probe function

  Parameters:     file_path (str): file_path

Arguments:
  FILE_PATH  [required]

Options:
  --help  Show this message and exit.
(env) joaomg@mypc:~/teed$ 
```