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

with open(join(dirname(__file__), "src", "python_pachyderm", "version.py"), "w") as f:
    f.write("__version__ = '{}'\n".format(version))

setup(
    name="python-pachyderm",
    version=version,
    license="Apache 2.0",
    description="Python Pachyderm Client",
    long_description_content_type="text/markdown",
    long_description=readme,
    author="Joe Doliner",
    author_email="jdoliner@pachyderm.io",
    url="https://github.com/pachyderm/python-pachyderm",
    packages=find_packages("src"),
    package_dir={"": "src"},
    py_modules=[splitext(basename(path))[0] for path in glob("src/*.py")],
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        # complete list: http://pypi.python.org/pypi?%3Aaction=list_classifiers
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: Implementation :: CPython",
    ],
    keywords=[
        "pachyderm",
    ],
    install_requires=[
        "protobuf>=3.17.1",
        "grpcio>=1.38.0",
        "grpcio-health-checking>=1.38.0",
    ],
    extras_require={
        "system_certs": ["certifi>=2019.11.28"],
        "DEV": [
            # certs
            "certifi>=2019.11.28",
            # code formatting/linting
            "black>=21.5b0",
            "flake8>=3.9.1",
            "pre-commit>=2.12.1",
            # testing
            "pytest>=5.3.4",
            "pytest-mock>=3.6.1",
            "pytest-timeout>=1.4.2",
            "tox>=3.23.1",
            # docs
            "numpydoc>=1.1.0",
            "Sphinx==4.3.0",
            "sphinx-rtd-theme==1.0.0",
            "myst-parser==0.15.2",
            # releasing
            "twine==3.6.0",
        ],
        "test": [
            "pytest==5.3.4",
            "pytest-mock>=3.6.1",
            "pytest-timeout>=1.4.2",
            "pytest-runner==5.2",
            "protobuf>=3.11.2",
            "grpcio>=1.26.0",
            "certifi>=2019.11.28",
        ],
    },
    test_suite="tests",
    setup_requires=["pytest-runner"],
    python_requires=">=3.6",
)
