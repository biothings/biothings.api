from setuptools import setup, find_packages

setup(
    name='biothings_pytest_plugin',
    version='0.1',
    packages=find_packages(where='biothings_pytest_plugin'),
    package_dir={'': 'biothings_pytest_plugin'},
    entry_points={
        'pytest11': [
            'biothings_pytest_plugin = biothings_pytest_plugin.plugin',
        ],
    },
    description='A pytest plugin for biothings',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/biothings/biothings.api/tree/master/biothings/tests/biothings_pytest_plugin',
    author='The BioThings Team',
    author_email='cwu@scripps.edu',
    license='Apache Software License (Apache License, Version 2.0)',
    install_requires=[
        'pytest',
        'boto3',
        'requests',
    ],
    python_requires='>=3.8',
)
