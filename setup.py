#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function

import io
import re
from glob import glob
from os.path import basename
from os.path import dirname
from os.path import join
from os.path import splitext

from setuptools import find_packages
from setuptools import setup

def read(*names, **kwargs):
    with io.open(
        join(dirname(__file__), *names),
        encoding=kwargs.get('encoding', 'utf8')
    ) as fh:
        return fh.read()

setup(
    name='cmapBQ',
    use_scm_version={
        'local_scheme': 'dirty-tag',
        'write_to': 'cmapBQ/_version.py',
        'fallback_version': '0.1.0',
    },
    description="Toolkit for interacting with Google BigQuery and CMAP datasets",
    author='Anup Jonchhe',
    author_email='anup@broadinstitute.org',
    url='https://github.com/cmap/cmapBQ',
    packages=find_packages(),
    py_modules=[splitext(basename(path))[0] for path in glob('./*.py')],
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        # complete classifier list: http://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Operating System :: Unix',
        'Operating System :: POSIX',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
    project_urls={
        'Documentation': 'https://cmapBQ.readthedocs.io/',
        #'Homepage': 'https://github.com/cmap/cmapBQ/',
        'Issue Tracker': 'https://github.com/cmap/cmapBQ/issues/',
    },
    install_requires=[
        'cmapPy',
        'pandas',
        'google-cloud-bigquery',
        'google-cloud-bigquery-storage',
        'google-cloud-storage',
        'pandas-gbq',
        'pyyaml',
        'dacite',
        'pyarrow',
    ],
    setup_requires=[
        'setuptools_scm>=3.3.1',
    ],
    entry_points={
        'console_scripts': [
            'cmapBQ = cmapBQ.__main__:run',
        ]
    },
)
