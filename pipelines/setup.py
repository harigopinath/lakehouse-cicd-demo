import os
from setuptools import setup

VERSION = "0.0.1-snapshot"
if "LAKEHOUSE_VERSION" in os.environ:
    VERSION = os.environ["LAKEHOUSE_VERSION"]

setup(
    name='lakehousePipelines',
    version=VERSION,
    packages=['.lakehousePipelines'],
    entry_points={
        'console_scripts': [
            'run_who_bronze = lakehousePipelines.run_who_bronze:main',
            'run_who_silver = lakehousePipelines.run_who_silver:main',
        ]
    }
)
