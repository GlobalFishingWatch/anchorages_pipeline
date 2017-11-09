import os
import setuptools

REQUIRED_PACKAGES = [
    's2sphere',
    'ujson',
    'fiona',
    'shapely'
    ]


if os.path.exists('VERSION'):
    with open('VERSION') as f:
        version = f.read().strip()
else:
    # When installed on dataflow instance for example
    version = 'UNAVAILABLE'

    
setuptools.setup(
    name='anchorages',
    version=version,
    description='anchorage pipeline.',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    package_data={'anchorages': ['*.pickle', '*.csv'],
                                 '': ['VERSION']}
    )