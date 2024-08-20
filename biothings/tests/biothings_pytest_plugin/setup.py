from setuptools import setup

setup(
    name='biothings-pytest-plugin',
    version='0.1',
    py_modules=['biothings_pytest_plugin'],
    entry_points={
        'pytest11': [
            'biothings = biothings_pytest_plugin',
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
