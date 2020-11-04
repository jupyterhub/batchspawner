#!/usr/bin/env python
# coding: utf-8

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

#-----------------------------------------------------------------------------
# Minimal Python version sanity check (from IPython/Jupyterhub)
#-----------------------------------------------------------------------------

from __future__ import print_function

import os
import sys

from setuptools import setup
from glob import glob

pjoin = os.path.join
here = os.path.abspath(os.path.dirname(__file__))

# Get the current package version.
version_ns = {}
with open(pjoin(here, 'version.py')) as f:
    exec(f.read(), {}, version_ns)

with open(pjoin(here, 'README.md'), encoding='utf-8') as f:
    long_desc = f.read()

setup_args = dict(
    name                = 'batchspawner',
    scripts             = glob(pjoin('scripts', '*')),
    packages            = ['batchspawner'],
    version             = version_ns['__version__'],
    description         = """Batchspawner: A spawner for Jupyterhub to spawn notebooks using batch resource managers.""",
    long_description    = long_desc,
    long_description_content_type = 'text/markdown',
    author              = "Michael Milligan, Andrea Zonca, Mike Gilbert",
    author_email        = "milligan@umn.edu",
    url                 = "http://jupyter.org",
    license             = "BSD",
    platforms           = "Linux, Mac OS X",
    python_requires     = '~=3.3',
    keywords            = ['Interactive', 'Interpreter', 'Shell', 'Web', 'Jupyter'],
    classifiers         = [
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
    ],
    project_urls        = {
        'Bug Reports':      'https://github.com/jupyterhub/batchspawner/issues',
        'Source':           'https://github.com/jupyterhub/batchspawner/',
        'About Jupyterhub': 'http://jupyterhub.readthedocs.io/en/latest/',
        'Jupyter Project':  'http://jupyter.org',
    }
)

# setuptools requirements
if 'setuptools' in sys.modules:
    setup_args['install_requires'] = install_requires = []
    with open('requirements.txt') as f:
        for line in f.readlines():
            req = line.strip()
            if not req or req.startswith(('-e', '#')):
                continue
            install_requires.append(req)


def main():
    setup(**setup_args)

if __name__ == '__main__':
    main()
