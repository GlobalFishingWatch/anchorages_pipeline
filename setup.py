import setuptools

package = __import__('pipe_anchorages')

DEPENDENCIES = [
    'fiona',
    'shapely'
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
    install_requires=DEPENDENCIES,
    packages=setuptools.find_packages(),
    zip_safe=True,
    package_data={
        '': ['data/*.pickle', 'data/port_lists/*.csv'],
    }
    )
