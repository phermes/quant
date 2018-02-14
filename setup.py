from setuptools import setup, find_packages

setup(name='fph',
      version='0.1',
      description='fph',
      # url='http://github.com/storborg/funniest',
      author='Pascal Hermes',
      # author_email='tomtommertens2@gmail.com',
      license='MIT',
      packages=find_packages(),
      install_requires=[
          'numpy', 'pandas', 'argparse', 'scipy', 'dask','selenium'
      ],
      include_package_data=True,
      zip_safe=False)
