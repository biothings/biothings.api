import os
from setuptools import setup, find_packages
from subprocess import check_output
from subprocess import CalledProcessError

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

# Calculate version
try:
    # make the biothings version the commit hash
    __version__ = check_output("git rev-parse HEAD", shell=True).decode('utf-8').strip('\n')
except CalledProcessError:
    # of course it's 0.0.2, what else would it be?
    __version__ = '0.0.2'

setup(
    name="biothings",
    version=__version__,
    author="Cyrus Afrasiabi, Chunlei Wu, Sebastien Lelong, Kevin Xin",
    author_email="cyrus@scripps.edu",
    description="Python package for biothings framework",
    license="BSD",
    keywords="biology annotation web service client api",
    url="https://github.com/SuLab/biothings.api",
    packages=find_packages(),
    include_package_data=True,
    scripts=['biothings/bin/biothings-admin.py'],
    long_description=read('README.md'),
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: BSD License",
        "Operating System :: POSIX",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        "Intended Audience :: Science/Research",
        "Topic :: Utilities",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
    ],
    install_requires=[
        'requests>=2.3.0',
        'tornado',
    ],
)
