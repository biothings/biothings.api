#!/usr/bin/env python
''' Generates a python client for a biothing service. '''

import os
import biothings
import argparse
from string import Template

template_dir = os.path.join(os.path.split(biothings.__file__)[0], 'conf', 'client_template')

def generate_client(args):

# create settings dict
    settings_dict = {
        "package_name": 'my' + args.obj.lower(),
        "annotation_endpoint": args.obj,
        "full_url": "My" + args.obj.title() + '.info',
        "client_class_name": "My" + args.obj.title() + "Info",
        "default_server_url": "http://my" + args.obj.lower() + ".info/v1",
        "annotation_type": args.obj.lower(),
        "client_user_agent_header": "my" + args.obj.lower() + ".py",
        "default_cache_name": "my" + args.obj.lower() + "_cache"
    }

    # make command line args into dict
    clargs = dict([i.split('=', maxsplit=1) for i in args.o])

    # override any key value pairs from the command line
    settings_dict.update(clargs)
    
    # Make top level directory
    pdir = os.path.join(os.path.abspath(args.p), settings_dict["package_name"])
    os.mkdir(pdir)

    with open(os.path.join(pdir, '__init__.py'), 'w') as output, open(
        os.path.join(template_dir, '__init__.py-tpl'), 'r') as inp:
        output.write(Template(inp.read()).substitute(settings_dict))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('obj', help="object type.  from this, a default settings dictionary will be created")
    parser.add_argument('-p', default=os.path.abspath("."), help="path where the client package will be written, default '.'")
    parser.add_argument('-o', default=[], nargs="*", help="override any of the default settings with key=value pairs")
    n=parser.parse_args()
    generate_client(n)
