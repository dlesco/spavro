#! /usr/bin/env python

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
from sys import version_info
from distutils.extension import Extension

try:
    from Cython.Build import cythonize
    USE_CYTHON = True
except ImportError:
    USE_CYTHON = False

install_requires = []
if version_info[:2] <= (2, 5):
    install_requires.append('simplejson >= 2.0.9')

ext = '.pyx' if USE_CYTHON else '.c'
extensions = [Extension("spavro.fast_binary", ["src/spavro/fast_binary" + ext])]


if USE_CYTHON:
    extensions = cythonize(extensions)

setup(
  name='spavro',
  version='1.0',
  packages=['spavro'],
  package_dir={'': 'src'},
  scripts=["./scripts/avro"],
  include_package_data=True,
  package_data={'spavro': ['LICENSE.txt', 'NOTICE.txt']},
  # Project uses simplejson, so ensure that it gets installed or upgraded
  # on the target machine
  install_requires=install_requires,
  # spavro C extensions
  ext_modules=extensions,
  # metadata for upload to PyPI
  author='Michael Kowalchik',
  author_email='mikepk@pluralsight.com',
  description='Spavro is a (sp)eedier avro serialization library forked from the official Apache Python implementation.',
  license='Apache License 2.0',
  keywords='avro serialization rpc data',
  url='http://avro.apache.org/',
  extras_require={
    'snappy': ['python-snappy'],
  },
  tests_require=['nose'],
  test_suite='nose.collector'
)
