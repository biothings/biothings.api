import os
from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name="biothings",
    version="0.0.1",
    author="Chunlei Wu, Cyrus Afrasiabi",
    author_email="cwu@scripps.edu",
    description="Python package for biothings framework",
    license="BSD",
    keywords="biology annotation web service client api",
    url="https://github.com/SuLab/biothings.api",
    packages=['biothings'],
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
    ],
)
