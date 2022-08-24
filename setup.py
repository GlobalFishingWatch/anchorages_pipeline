#!/usr/bin/env python

from setuptools import find_packages
from setuptools import setup

setup(
    name="pipe_anchorages",
    version="3.3.0",
    packages=find_packages(exclude=["test*.*", "tests"]),
    package_data={
        "": ["data/*.pickle", "data/port_lists/*.csv", "data/EEZ/*"],
    },
)
