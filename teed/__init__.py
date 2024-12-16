import re
from os import path

from .config import VERSION as __version__

# Exception


class TeedException(Exception):
    """General TeedException"""

    pass


# Utils


def file_path_parse(file_path: str) -> tuple:
    """From a file path return it's
    name, name without extension and extension

    Parameters:
        path to the file (str): file_path

    Returns:
        tuple with the
        base file name
        file name without it's extension
        the file extension
        (str, str, str): file_name, file_name_without_ext, file_ext
    """

    file_name = path.basename(file_path)
    file_name_without_ext = file_name.split(".")[0]
    file_ext = file_name.split(".")[1]

    return (file_name, file_name_without_ext, file_ext)


def get_xml_encoding(file_path):
    """Read the XML encoding declaration, default to UTF-8"""
    # Read only the first 100 bytes or so for efficiency
    with open(file_path, "rb") as f:
        first_bytes = f.read(100).decode("ascii", errors="ignore")

    # Use a regex to match the encoding in the XML declaration
    match = re.search(r'encoding=["\'](.*?)["\']', first_bytes)
    return (
        match.group(1) if match else "utf-16"
    )  # Default to 'utf-8' if no encoding found


# Helpers


def read_asset(*paths):
    dirname = path.dirname(__file__)
    return open(path.join(dirname, "assets", *paths)).read().strip()


# General


VERSION = read_asset("VERSION")
COMPRESSION_FORMATS = ["zip", "gz"]


# Defaults


# Backports
