#!/usr/bin/env python
# -*- mode: python -*-
# -*- coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Provide the code necessary for packaging and installing avro-python3.

The avro-python3 software is designed for Python 3, but this file and the packaging software supports Python 2.7.

https://pypi.org/project/avro-python3/
"""

import distutils.command.clean
import distutils.dir_util
import distutils.errors
import distutils.file_util
import distutils.log
import fnmatch
import glob
import os
import subprocess

import setuptools

_HERE = os.path.dirname(os.path.abspath(__file__))
_AVRO_DIR = os.path.join(_HERE, 'avro')
_VERSION_FILE_NAME = 'VERSION.txt'

def _is_distribution():
    """Tests whether setup.py is invoked from a distribution.

    Returns:
        True if setup.py runs from a distribution.
        False otherwise, ie. if setup.py runs from a version control work tree.
    """
    # If a file PKG-INFO exists as a sibling of setup.py,
    # assume we are running as source distribution:
    return os.path.exists(os.path.join(_HERE, 'PKG-INFO'))


def _generate_package_data():
    """Generate package data.

    This data will already exist in a distribution package,
    so this function only runs for local version control work tree.
    """
    distutils.log.info('Generating package data')

    # Avro top-level source directory:
    root_dir = os.path.dirname(os.path.dirname(_HERE))

    # Create a PEP440 compliant version file.
    version_file_path = os.path.join(root_dir, 'share', _VERSION_FILE_NAME)
    with open(version_file_path) as vin:
        version = vin.read().replace('-', '+')
    with open(os.path.join(_AVRO_DIR, _VERSION_FILE_NAME), 'w') as vout:
        vout.write(version)

    # Copy necessary avsc files:
    avsc_files = (
        (('schemas', 'org', 'apache', 'avro', 'ipc', 'HandshakeRequest.avsc'), ''),
        (('schemas', 'org', 'apache', 'avro', 'ipc', 'HandshakeResponse.avsc'), ''),
        (('test', 'schemas', 'interop.avsc'), ('tests',)),
    )

    for src, dst in avsc_files:
        src = os.path.join(root_dir, 'share', *src)
        dst = os.path.join(_AVRO_DIR, *dst)
        distutils.file_util.copy_file(src, dst)


class CleanCommand(distutils.command.clean.clean):
    """A command to clean up install artifacts and replaceable, generated files."""

    def _replaceable(self):
        """Get the list of files to delete."""
        for name in ('dist', 'avro_python3.egg-info', os.path.join(_AVRO_DIR, _VERSION_FILE_NAME)):
            if os.path.exists(name):
                yield name
        for root, dirnames, filenames in os.walk(_AVRO_DIR):
            if '__pycache__' in dirnames:
                dirnames.remove('__pycache__')
                yield os.path.join(root, '__pycache__')
            for name in fnmatch.filter(filenames, '*.avsc'):
                yield os.path.join(root, name)

    def run(self):
        super().run()
        for name in self._replaceable():
            if self.dry_run:
                distutils.log.info('Would remove %s', name)
            elif os.path.isdir(name):
                # distutils logs this for us
                distutils.dir_util.remove_tree(name)
            else:
                distutils.log.info('Removing %s', name)
                os.remove(name)


class GenerateInteropDataCommand(setuptools.Command):
    """A command to generate Avro files for data interop test."""

    user_options = [
      ('schema-file=', None, 'path to input Avro schema file'),
      ('output-path=', None, 'path to output Avro data files'),
    ]

    def initialize_options(self):
        self.schema_file = os.path.join(os.getcwd(), 'interop.avsc')
        self.output_path = os.getcwd()

    def finalize_options(self):
        pass

    def run(self):
        from avro.tests import gen_interop_data
        gen_interop_data.generate(self.schema_file, self.output_path)


class LintCommand(setuptools.Command):
    """Run pycodestyle on all your modules"""
    description = __doc__
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        # setuptools does not seem to make pycodestyle available
        # in the pythonpath, so we do it ourselves.
        try:
            env = {'PYTHONPATH': next(glob.iglob('.eggs/pycodestyle-*.egg'))}
        except StopIteration:
            env = None  # pycodestyle is already installed
        try:
            subprocess.run(['python3', '-m', 'pycodestyle', '.'], env=env, check=True)
        except subprocess.CalledProcessError:
            raise distutils.errors.DistutilsError("pycodestyle exited with a nonzero exit code.")


def main():
    if not _is_distribution():
        _generate_package_data()

    setuptools.setup(cmdclass={
        "clean": CleanCommand,
        "generate_interop_data": GenerateInteropDataCommand,
        "lint": LintCommand,
    })


if __name__ == '__main__':
    main()
