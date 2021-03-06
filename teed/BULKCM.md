# bulkcm
The BulkCm module supports probing, spliting and parsing of Bulkcm 32.615 specification, version 9.2.0 (2011-01) XML files.

https://www.etsi.org/deliver/etsi_ts/132600_132699/132615/09.02.00_60/ts_132615v090200p.pdf
https://portal.3gpp.org/desktopmodules/Specifications/SpecificationDetails.aspx?specificationId=2086

```
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

```
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