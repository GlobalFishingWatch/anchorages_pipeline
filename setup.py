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
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "apache-beam[gcp]<3",
        "jinja2<4",
        "pyyaml<6",
        "Fiona<2",
        "Shapely<2",
        "s2sphere<1",
        "Unidecode<2",
    ],
    entry_points={
        "console_scripts": [
            "pipe = main:main",
        ]
    },
)
