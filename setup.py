#!/usr/bin/env python3

import setuptools

name = 'dell_storage_api'
version = '0.1'
author = 'SK-CERT'

scripts = ['bin/dell-storage-client',]
requires = [
    'requests',
    'texttable',
    'pylint',
    'mypy',
]

packages = setuptools.find_packages()

setuptools.setup(
    name=name,
    version=version,
    install_requires=requires,
    packages=packages,
    scripts=scripts
)
