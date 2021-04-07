# teed
Telco engineering data library

Probe and transform raw telco files into CSV.

### A simple BulkCm file parsing

```shell
(env) joaomg@mypc:~/teed$ python -m teed bulkcm parse data/bulkcm.xml data
Parsing data/bulkcm.xml
Created data/ManagedElement.csv
Created data/ManagementNode.csv
Created data/SubNetwork.csv
Time: 0:00:00.000856
(env) joaomg@mypc:~/teed$
```

### Install from source
```shell
git clone https://github.com/joaomg/teed.git
cd teed
pip install -e .
```

### Probe a file

```shell
python -m teed bulkcm probe data/bulkcm_with_vsdatacontainer.xml
```

### Parse a file output it's content to CSV files

```shell
python -m teed bulkcm parse data/bulkcm_empty.xml data
python -m teed bulkcm parse data/bulkcm_with_header_footer.xml data
python -m teed bulkcm parse data/bulkcm_with_vsdatacontainer.xml data
```

### Usage

Beside command-line teed can be used as a library:
```python
from teed import bulkcm, meas

## bulkcm
stream = bulkcm.BulkCmParser.stream_to_csv("data")
bulkcm.parse("data/bulkcm.xml", "data", stream)

## meas 
meas.parse("data/mdc*xml", "data")
```

The bulkcm parser extracts content from a single file. 

While the meas parser, in a single run, can process any number of XML files using wildcards and directory recursion.

The bulkcm and meas parsers also differ on CSV file creation:

- bulkcm deletes previously existing CSV files

- meas appends to existing CSV files

Take notice of these differences when calling the parsers from shell. 

Or using them in data pipelines.

### Documentation

[bulkcm](teed/BULKCM.md)

[meas](teed/MEAS.md)

### License 

The teed library is licensed under:

GNU Affero General Public License v3.0

### On ETSI references and usage rights

https://www.etsi.org/intellectual-property-rights

https://www.etsi.org/images/files/IPR/etsi-ipr-policy.pdf
