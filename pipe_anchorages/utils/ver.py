import re


def get_pipe_ver():
    """Returns the version of the package."""
    with open("setup.py") as rf:
        return re.search(r'version="([0-9\.]*)"', rf.read()).group(1)
