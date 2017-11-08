import setuptools

REQUIRED_PACKAGES = [
    's2sphere',
    'ujson',
    'fiona',
    'shapely'
    ]


with open('VERSION') as f:
    version = f.read().strip()

    
setuptools.setup(
    name='anchorages',
    version='0.0.1',
    description='anchorage pipeline.',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    package_data={'anchorages': ['*.pickle', '*.csv']},
    )