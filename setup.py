#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import io
from glob import glob
from os.path import basename
from os.path import dirname
from os.path import join
from os.path import splitext

from setuptools import find_packages
from setuptools import setup


def read(*names, **kwargs):
    return io.open(
        join(dirname(__file__), *names),
        encoding=kwargs.get('encoding', 'utf8')
    ).read()

def get_version():
    with open("VERSION", "r") as f:
        version = f.read().strip()
    with open("BUILD_NUMBER", "r") as f:
        build = f.read().strip()

    if build != "1":
        return version + "-" + build
    else:
        return version

setup(
    name='python-pachyderm',
    version=get_version(),
    license='Apache 2.0',
    description='Python Pachyderm Client',
    long_description_content_type='text/markdown',
    long_description=read('README.md'),
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
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
    keywords=['pachyderm',],
    install_requires=[
        'protobuf>=3.8.0', 'grpcio>=1.21.1'
    ],
    test_suite='tests',
    tests_require=['pytest'],
    setup_requires=['pytest-runner'],
    python_requires='>=3.4',
)
