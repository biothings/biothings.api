# BioThings SDK

[![Downloads](https://pepy.tech/badge/biothings)](https://pepy.tech/project/biothings)
[![biothings package](https://badge.fury.io/py/biothings.svg)](https://pypi.python.org/pypi/biothings)
[![biothings_version](https://img.shields.io/pypi/pyversions/biothings.svg)](https://pypi.python.org/pypi/biothings)
[![biothings_version](https://img.shields.io/pypi/format/biothings.svg)](https://pypi.python.org/pypi/biothings)
[![biothings_version](https://img.shields.io/pypi/status/biothings.svg)](https://pypi.python.org/pypi/biothings)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-v2.0%20adopted-ff69b4.svg)](CODE_OF_CONDUCT.md)
[![Build Status](https://github.com/biothings/biothings.api/actions/workflows/test-build.yml/badge.svg)](https://github.com/biothings/biothings.api/actions/workflows/test-build.yml)
[![Tests Status](https://github.com/biothings/biothings.api/actions/workflows/run-tests.yml/badge.svg)](https://github.com/biothings/biothings.api/actions/workflows/run-tests.yml)
[![Documentation Status](https://readthedocs.org/projects/biothingsapi/badge/?version=latest)](https://docs.biothings.io/en/latest/?badge=latest)

## Quick Summary

BioThings SDK provides a Python-based toolkit to build high-performance data APIs (or web services) from a single data source or multiple data sources. It has the particular focus on building data APIs for biomedical-related entities, a.k.a "BioThings" (such as genes, genetic variants, drugs, chemicals, diseases, etc.).

Documentation about BioThings SDK can be found at https://docs.biothings.io

## Introduction

### What's BioThings?

We use "**BioThings**" to refer to objects of any biomedical entity-type
represented in the biological knowledge space, such as genes, genetic
variants, drugs, chemicals, diseases, etc.

### BioThings SDK

SDK represents "Software Development Kit". BioThings SDK provides a
[Python-based](https://www.python.org/) toolkit to build
high-performance data APIs (or web services) from a single data source
or multiple data sources. It has the particular focus on building data
APIs for biomedical-related entities, a.k.a "*BioThings*", though it's
not necessarily limited to the biomedical scope. For any given
"*BioThings*" type, BioThings SDK helps developers to aggregate
annotations from multiple data sources, and expose them as a clean and
high-performance web API.

The BioThings SDK can be roughly divided into two main components: data
hub (or just "hub") component and web component. The hub component
allows developers to automate the process of monitoring, parsing and
uploading your data source to an
[Elasticsearch](https://www.elastic.co/products/elasticsearch) backend.
From here, the web component, built on the high-concurrency [Tornado Web
Server](http://www.tornadoweb.org/en/stable/) , allows you to easily
setup a live high-performance API. The API endpoints expose
simple-to-use yet powerful query features using [Elasticsearch's
full-text query capabilities and query
language](https://www.elastic.co/guide/en/elasticsearch/reference/2.4/query-dsl-query-string-query.html#query-string-syntax).

### BioThings API

We also use "*BioThings API*" (or *BioThings APIs*) to refer to an API
(or a collection of APIs) built with BioThings SDK. For example, both
our popular [MyGene.Info](http://mygene.info/) and
[MyVariant.Info](http://myvariant.info/) APIs are built and maintained
using this BioThings SDK.

### BioThings Studio

*BioThings Studio* is a buildin, pre-configured environment used to build and
administer a BioThings API. At its core is the *Hub*, a backend service responsible for maintaining data up-to-date, producing data releases and
update API frontends.

## Installing BioThings SDK

You can install the latest stable BioThings SDK release with pip from
[PyPI](https://pypi.python.org/pypi), like:

    # default to install web requirements only for running an API
    pip install biothings
    # or include additional requirements useful for running an API on production
    # like msgpack, sentry-sdk pacakages
    pip install biothings[web_extra]

    # install hub requirements for running a hub (including CLI)
    pip install biothings[hub]

    # install CLI-only requirements if you only need CLI for develop a data plugin
    pip install biothings[cli]

    # need support for docker data plugin
    pip install biothings[hub,docker]
    # or if use ssh protocol to connect to a remote docker server
    pip install biothings[hub,docker_ssh]

    # just install everything for dev purpose
    pip install biothings[dev]

You can check more details for the optional dependecy packages directly in [setup.py](setup.py) file.

You can install the latest development version of BioThings SDK directly
from our github repository like:

    pip install git+https://github.com/biothings/biothings.api.git#egg=biothings

    # from a branch or commit
    pip install git+https://github.com/biothings/biothings.api.git@0.12.x#egg=biothings

    # include optional dependecies
    pip install git+https://github.com/biothings/biothings.api.git@0.12.x#egg=biothings[web_extra]

    # can be very useful to install in “editable” mode:
    pip install -e git+https://github.com/biothings/biothings.api.git@0.12.x#egg=biothings[web_extra]

Alternatively, you can download the source code, or clone the [BioThings
SDK repository](https://github.com/biothings/biothings.api) and run:

    pip install .
    # or
    pip install .[web_extra]

## Get started to build a BioThings API

We recommend to follow [this tutorial](https://docs.biothings.io/en/latest/tutorial/studio.html) to develop your first BioThings API in our pre-configured **BioThings Studio** development environment.

## Documentation

The latest documentation is available at https://docs.biothings.io.

## How to contribute

Please check out this [Contribution Guidelines](CONTRIBUTING.md) and [Code of Conduct](CODE_OF_CONDUCT.md) document.

## Active and past contributors

Please see [Contributors](Contributors.md)
