#!/usr/bin/env python
import os
import biothings
import argparse
from string import Template

def log(msg):
    print("\n******************")
    print(msg)
    print("******************\n")
    return

def main(args):
    # now only es is supported as a biothing backend...
    if args.b == 'es':
        subdir = 'es_project_template'
    else:
        pass

    # get the template dir for the proper backend
    template_dir = os.path.join(os.path.split(biothings.__file__)[0], 'conf', 'biothings_templates', subdir)

    if args.verbose:
        log("Template directory: {}".format(template_dir))

    # generate template settings
    settings_dict = {
        "src_package": 'my' + args.obj.lower(),
        "settings_class": "My" + args.obj.title() + "WebSettings",
        "annotation_endpoint": args.obj.lower(),
        "query_endpoint": "query",
        "annotation_handler_name": args.obj.title() + "Handler",
        "query_handler_name": "QueryHandler",
        "es_doctype": args.obj.lower(),
        "base_url": "My" + args.obj.title() + ".info",
        "nosetest_settings_class": args.obj.title(),
        "nosetest_envar": "M" + args.obj.upper()[0] + '_HOST'
    }

    if args.l:
        log("List of available settings:\n{}".format(list(settings_dict.keys())))
        return

    # store cwd
    cwd = os.getcwd()

    # make command line args into dict
    clargs = dict([i.split('=', maxsplit=1) for i in args.o])

    # override any key value pairs from the command line
    settings_dict.update(clargs)

    if args.verbose:
        log("Creating {} project template using {}".format(args.b, settings_dict))
    
    # Make top level directory
    pdir = os.path.join(os.path.abspath(args.path), settings_dict["src_package"])
    if args.verbose:
        log("Creating directory structure . . . {}".format(pdir))
    os.mkdir(pdir)

    # Start to template files out
    os.chdir(template_dir)

    # Template files out
    for (index, (dirpath, dirnames, filenames)) in enumerate(list(os.walk('.'))):
        thisdir = os.path.join(pdir, dirpath)
        os.mkdir(thisdir)
        for fi in [f for f in filenames if f.endswith('-tpl')]:
            with open(os.path.join(thisdir, fi[:-4]), 'w') as outfile, open(
                os.path.join(os.path.abspath(dirpath), fi), 'r') as infile:
                outfile.write(Template(infile.read()).substitute(settings_dict))
    os.chdir(cwd)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('obj', help="biothing type (e.g. gene, variant, drug, etc).  Default settings are created from this")
    parser.add_argument('path', help="path where skeleton project should be placed")
    parser.add_argument('-o', default=[], nargs="*", help="override any of the default settings with key=value pairs, to see default settings list, use -l")
    parser.add_argument('-l', default=False, dest="l", action="store_true")
    parser.add_argument('-b', default='es', help="type of database used for this biothing project")
    parser.add_argument('-v', default=False, dest="verbose", action="store_true")
    args = parser.parse_args()
    main(args)
