from .config import VERSION as __version__

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


# Defaults


# Backports
