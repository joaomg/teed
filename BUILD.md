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

We're using twine, see 

```shell
pip install --upgrade twine
twine upload --repository testpypi dist/*
```
