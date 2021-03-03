import io
import os
import gzip
import zipfile


# Helpers


def read_asset(*paths):
    dirname = os.path.dirname(__file__)
    return io.open(os.path.join(dirname, "assets", *paths)).read().strip()


# General


VERSION = read_asset("VERSION")
COMPRESSION_FORMATS = ["zip", "gz"]
UNDEFINED = object()


# Defaults


# Backports


# It can be removed after dropping support for Python 3.6 and Python 3.7
COMPRESSION_EXCEPTIONS = (
    (zipfile.BadZipFile, gzip.BadGzipFile)
    if hasattr(gzip, "BadGzipFile")
    else (zipfile.BadZipFile)
)
