# teed
Telco engineering data library

Probe and transform raw telco files into CSV.

### A simple BulkCm file parsing
```
(env) joaomg@hp:~/teed$ python -m teed bulkcm parse data/bulkcm.xml data
Parsing data/bulkcm.xml
Created data/ManagedElement.csv
Created data/ManagementNode.csv
Created data/SubNetwork.csv
Time: 0:00:00.000856
(env) joaomg@hp:~/teed$
```

### Install from source
```
git clone https://github.com/joaomg/teed.git
cd teed
pip install -e .
```

### Probe a file
``` shell
python -m teed bulkcm probe data/bulkcm_with_vsdatacontainer.xml
```

### Parse a file and output it's content to a directory
```
python -m teed bulkcm parse data/bulkcm_empty.xml data
python -m teed bulkcm parse data/bulkcm_with_header_footer.xml data
python -m teed bulkcm parse data/bulkcm_with_vsdatacontainer.xml data
```
### Bulkcm 32.615 specification, version 9.2.0 (2011-01)
https://www.etsi.org/deliver/etsi_ts/132600_132699/132615/09.02.00_60/ts_132615v090200p.pdf
https://portal.3gpp.org/desktopmodules/Specifications/SpecificationDetails.aspx?specificationId=2086
