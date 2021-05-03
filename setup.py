import os
import codecs
import setuptools
from pipe_tools.beam.requirements import requirements as DATAFLOW_PINNED_DEPENDENCIES

package = __import__('pipe_anchorages')

DEPENDENCIES = [
    'more_itertools',
    'statistics',
    'pytz',
    's2sphere',
    'ujson==1.35',
    'fiona',
    'shapely',
    'pyyaml',
    'unidecode',
    'numpy',
    "pipe-tools==3.2.0",
    "jinja2-cli",
    "tzlocal==1.5.1",
    "cython"
]

setuptools.setup(
    name='pipe-anchorages',
    author=package.__author__,
    author_email=package.__email__,
    description=package.__doc__.strip(),
    url=package.__source__,
    version=package.__version__,
    license="Apache 2.0",
    include_package_data=True,
    install_requires=DEPENDENCIES + DATAFLOW_PINNED_DEPENDENCIES,
    packages=setuptools.find_packages(),
    zip_safe=True,
    package_data={
        '': ['data/*.pickle', 'data/port_lists/*.csv'],
    }
    )
