## Package and publish teed

Sequence of commands how to package teed and publish to PYPI.

Based on the guide:

https://packaging.python.org/tutorials/packaging-projects/

### Package

Using setuptools and wheel.

```shell
cd teed
source env/bin/activate
python -m pip install --upgrade pip
pip install setuptools wheel
python setup.py sdist bdist_wheel
```

### Publish

We're using twine, see the above mentioned guide for details.

```shell
pip install --upgrade twine
```

Upload new package to testpypi.

As username place `__token__` and as password the testpypi `token`.

Which starts with the `pypi-` chars (take care to include these chars when submitting the token).

```shell
rm -Rf dist
python setup.py sdist bdist_wheel
twine upload --repository testpypi dist/*
```

### Deploy from testpypi

Create a new virtual environment, testenv.

And activate it.

```shell
cd ~
python3 -m virtualenv testenv
source testenv/bin/activate
```

Activate testenv and install dependencies from PyPi.

These specific versions aren't available from the test PyPi.

```shell
pip install pyyaml>=5.4.1
pip install lxml>=4.6.3
pip install typer>=0.3.2
```

Install teed from the test Python Package Index (https://test.pypi.org/simple/)

```shell
pip install -i https://test.pypi.org/simple/ teed==0.0.8.2
```

### Use teed

In console:

```shell
python -m teed bulkcm parse data/bulkcm.xml data
```

As a python library:

```python
from teed import meas
meas.parse("data/mdc*xml", "data")
```
