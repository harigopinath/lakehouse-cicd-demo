import os
from setuptools import setup

VERSION = "0.0.1-snapshot"
if "LAKEHOUSE_VERSION" in os.environ:
    VERSION = os.environ["LAKEHOUSE_VERSION"]

setup(
    name='lakehouseLibs',
    version=VERSION,
    packages=['.lakehouseLibs']
)
