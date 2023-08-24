#!/usr/bin/env python

from setuptools import find_packages, setup

setup(
    name="pipe_anchorages",
    version="4.1.1",
    packages=find_packages(exclude=["test*.*", "tests"]),
    package_data={
        "": ["data/port_lists/*.csv", "data/EEZ/*"],
    },
)
