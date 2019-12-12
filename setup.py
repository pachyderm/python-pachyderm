#!/usr/bin/env python

import json
from glob import glob
from os.path import basename
from os.path import dirname
from os.path import join
from os.path import splitext

from setuptools import find_packages
from setuptools import setup

with open(join(dirname(__file__), "README.md"), "r") as f:
    readme = f.read()

with open(join(dirname(__file__), "version.json"), "r") as f:
    j = json.load(f)
    version = j["python-pachyderm"]

setup(
    name='python-pachyderm',
    version=version,
    license='Apache 2.0',
    description='Python Pachyderm Client',
    long_description_content_type='text/markdown',
    long_description=readme,
    author='Joe Doliner',
    author_email='jdoliner@pachyderm.io',
    url='https://github.com/pachyderm/python-pachyderm',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    py_modules=[splitext(basename(path))[0] for path in glob('src/*.py')],
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        # complete classifier list: http://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
    keywords=['pachyderm',],
    install_requires=[
        'protobuf>=3.8.0', 'grpcio>=1.21.1', 'certifi>=2019.09.11'
    ],
    test_suite='tests',
    tests_require=['pytest'],
    setup_requires=['pytest-runner'],
    python_requires='>=3.4',
)
