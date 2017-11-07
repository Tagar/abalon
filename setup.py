# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from setuptools import setup, find_packages

import imp
import os

version = imp.load_source(
    'airflow.version', os.path.join('abalon', 'version.py')).version

# http://setuptools.readthedocs.io/en/latest/setuptools.html

setup(name='abalon',
      version=version,
      packages=find_packages(exclude=['tests*']),

      # install_requires=['docutils>=0.3'],

      # metadata for upload to PyPI
      description='Various utility functions for Apache Spark (pySpark)',
      url='https://github.com/Tagar/abalon',
      author='Ruslan Dautkhanov',
      author_email='Dautkhanov@gmail.com',
      license='Apache-2.0',
    )
