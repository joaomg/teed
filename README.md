# teed
Telco engineering data library

Probe and transform raw telco files into CSV.

### A simple BulkCm file parsing
```
(env) joaomg@mypc:~/teed$ python -m teed bulkcm parse data/bulkcm.xml data
Parsing data/bulkcm.xml
Created data/ManagedElement.csv
Created data/ManagementNode.csv
Created data/SubNetwork.csv
Time: 0:00:00.000856
(env) joaomg@mypc:~/teed$
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

### Usage
Beside command-line teed can be used as a library:
```
from teed import bulkcm
stream = bulkcm.BulkCmParser.stream_to_csv("data")
bulkcm.parse("data/bulkcm.xml", "data", stream)
```
