
from setuptools import setup, find_packages

# http://setuptools.readthedocs.io/en/latest/setuptools.html

setup(name='abalon',
      version='1.2.2',
      packages=find_packages(),

      # install_requires=['docutils>=0.3'],

      # metadata for upload to PyPI
      description='Various utility functions for Apache Spark (pySpark)',
      url='https://github.com/Tagar/abalon',
      author='Ruslan Dautkhanov',
      author_email='Dautkhanov@gmail.com',
      license='Apache-2.0',
    )
