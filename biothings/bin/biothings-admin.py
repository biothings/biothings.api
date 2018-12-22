#!/usr/bin/env python
import os
import argparse
from string import Template

import biothings


settings_dict = {
    "src_package": "",
    "settings_class": "",
    "annotation_endpoint": "",
    "query_endpoint": "",
    "annotation_handler_name": "",
    "query_handler_name": "",
    "es_doctype": "",
    "base_url": "",
    "nosetest_settings_class": "",
    "nosetest_envar": ""
}


def main(args):
    # now only es is supported as a biothing backend...
    if args.b == 'es':
        subdir = 'es_project_template'
    else:
        pass

    # get the template dir for the proper backend
    template_dir = os.path.join(os.path.split(biothings.__file__)[0], 'conf', 'biothings_templates', subdir)

    if args.verbose:
        print("Template directory: {}".format(template_dir))

    # generate template settings
    settings_dict["src_package"] = 'my' + args.obj.lower()
    settings_dict["settings_class"] = "My" + args.obj.title() + "WebSettings"
    settings_dict["annotation_endpoint"] = args.obj.lower()
    settings_dict["query_endpoint"] = "query"
    settings_dict["annotation_handler_name"] = args.obj.title() + "Handler"
    settings_dict["query_handler_name"] = "QueryHandler"
    settings_dict["es_doctype"] = args.obj.lower()
    settings_dict["base_url"] = "My" + args.obj.title() + ".info"
    settings_dict["nosetest_settings_class"] = args.obj.title()
    settings_dict["nosetest_envar"] = "M" + args.obj.upper()[0] + '_HOST'

    # store cwd
    cwd = os.getcwd()

    # make command line args into dict
    clargs = dict([i.split('=', maxsplit=1) for i in args.o])

    # override any key value pairs from the command line
    settings_dict.update(clargs)

    if args.verbose:
        print("Creating {} project template using {}".format(args.b, settings_dict))

    # Make top level directory
    pdir = os.path.abspath(args.path)
    if args.verbose:
        print("Creating directory structure at: {}".format(pdir))

    # Start to template files out
    os.chdir(template_dir)

    # Template files out
    for (index, (dirpath, dirnames, filenames)) in enumerate(list(os.walk('.'))):
        if dirpath == '.':
            thisdir = os.path.join(pdir, settings_dict['src_package'])
        else:
            thisdir = os.path.join(pdir, settings_dict['src_package'], dirpath)
        os.mkdir(thisdir)
        for fi in [f for f in filenames if f.endswith('-tpl')]:
            with open(os.path.join(thisdir, fi[:-4]), 'w') as outfile,\
                    open(os.path.join(os.path.abspath(dirpath), fi), 'r') as infile:
                outfile.write(Template(infile.read()).substitute(settings_dict))
    os.chdir(cwd)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="A tool to create a skeleton biothings project",
        epilog="Available settings:\n{}".format(list(settings_dict.keys()))
    )
    parser.add_argument('obj', help="biothing type (e.g. gene, variant, drug, etc).  Default settings are created from this")
    parser.add_argument('path', help="path where skeleton project should be placed")
    parser.add_argument('-o', default=[], nargs="*", help="override any of the default settings with key=value pairs, to see default settings list, use -l")
    parser.add_argument('-b', default='es', help="type of database used for this biothing project")
    parser.add_argument('-v', default=False, dest="verbose", action="store_true", help="Verbose")
    args = parser.parse_args()
    main(args)
