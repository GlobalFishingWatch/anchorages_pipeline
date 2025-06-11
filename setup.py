#!/usr/bin/env python

import setuptools

setuptools.setup(
    name="pipe_anchorages",
    version="4.3.3",
    author="Global Fishing Watch.",
    description=("Pipe for processing anchorages and generating anchorages events."),
    long_description_content_type="text/markdown",
    url="https://github.com/GlobalFishingWatch/anchorages_pipeline",
    packages=setuptools.find_packages(exclude=["test*.*", "tests"]),
    package_data={
        "": ["data/port_lists/*.csv", "data/EEZ/*"],
    },
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "apache-beam[gcp]~=2.49.0",
        "jinja2-cli~=0.8.0",
        "pyyaml~=6.0.0",
        "Fiona~=1.8.0",
        "Shapely~=1.7.0",
        "s2sphere~=0.2.0",
        "Unidecode~=1.2.0",
    ],
    entry_points={
        "console_scripts": [
            "pipe = main:main",
        ]
    },
)
