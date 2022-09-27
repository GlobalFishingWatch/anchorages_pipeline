#!/usr/bin/env python

from setuptools import find_packages, setup

setup(
    name="pipe_anchorages",
    version="3.3.0",
    packages=find_packages(exclude=["test*.*", "tests"]),
    package_data={
        "": ["data/port_lists/*.csv", "data/EEZ/*"],
    },
)
