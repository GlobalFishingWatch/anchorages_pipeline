#!/usr/bin/env python

from setuptools import find_packages
from setuptools import setup

setup(
    name='pipe_anchorages',
    packages=find_packages(exclude=['test*.*', 'tests'])
)

