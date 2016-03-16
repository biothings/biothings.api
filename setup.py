import os, glob
from setuptools import setup, find_packages

# Include the templates directory this way
data_files = []
directories = glob.glob('biothings/conf/')

for directory in directories:
    files = glob.glob(directory+'*')
    data_files.append((directory, files))

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

EXCLUDE_FROM_PACKAGES = ['biothings.bin', 'biothings.conf']

# Dynamically calculate the version based on biothings.VERSION.
version = __import__('biothings').get_version()

setup(
    name="biothings",
    version=version,
    author="Cyrus Afrasiabi, Chunlei Wu",
    author_email="cyrus@scripps.edu",
    description="Python package for biothings framework",
    license="BSD",
    keywords="biology annotation web service client api",
    url="https://github.com/SuLab/biothings.api",
    packages=find_packages(exclude=EXCLUDE_FROM_PACKAGES),
    data_files=data_files,
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
