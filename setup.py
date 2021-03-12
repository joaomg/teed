import os
import io
from setuptools import setup, find_packages


# Helpers


def read(*paths):
    """Read a text file."""
    basedir = os.path.dirname(__file__)
    fullpath = os.path.join(basedir, *paths)
    contents = io.open(fullpath, encoding="utf-8").read().strip()
    return contents


# Prepare


PACKAGE = "teed"
NAME = PACKAGE.replace("_", "-")
TESTS_REQUIRE = [
    "black",
    "pylama",
    "pytest",
    "pytest-cov",
]
EXTRAS_REQUIRE = {
    "dev": TESTS_REQUIRE,
}
INSTALL_REQUIRES = ["lxml>=4.6.2", "typer>=0.3.2", "pyyaml>=5.4.1"]
README = read("README.md")
VERSION = read(PACKAGE, "assets", "VERSION")
PACKAGES = find_packages(exclude=["tests"])
ENTRY_POINTS = {"console_scripts": ["teed = teed.__main__:program"]}


# Run


setup(
    name=NAME,
    version=VERSION,
    packages=PACKAGES,
    include_package_data=True,
    install_requires=INSTALL_REQUIRES,
    tests_require=TESTS_REQUIRE,
    extras_require=EXTRAS_REQUIRE,
    entry_points=ENTRY_POINTS,
    zip_safe=False,
    long_description=README,
    long_description_content_type="text/markdown",
    description="",
    author="joaomg",
    author_email="joaomg@gmail.com",
    url="https://github.com/joaomg/teed",
    keywords=[
        "telco data engineering",
        "telco cm",
        "telco pm",
        "telco bulkcm",
        "frictionless data",
        "data package",
        "tabular data package",
    ],
    classifiers=[
        "Development Status :: 0 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
