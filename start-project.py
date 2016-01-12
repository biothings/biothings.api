import os
import re
import sys
from string import Template
from shutil import copytree

def transform_name(s, d):
    return Template(re.sub(r"\}\}", "}", re.sub(r"\{\{", "${", s))).substitute(d)

def usage():
    return "Usage: python start-project.py < path to project directory > < object name > < OPTIONAL key=value pairs to override template variables >"

def main(args):
    try:
        pdir = os.path.abspath(args[1])
        biothing_name = args[2]
        clargs = dict([arg.split('=', maxsplit=1) for arg in sys.argv[3:]])
    except:
        print("Valid project destination directory and object name must be included.")
        print(usage())
        sys.exit(1)

    cwd = os.getcwd()
    template_dir = os.path.join(os.getcwd(), 'biothings_templates')

    if not os.path.exists(template_dir):
        print("Could not find template directory.  Exiting.")
        sys.exit(1)

    # create settings dict
    settings_dict = {
        "src_package": 'my' + biothing_name.lower(),
        "settings_class": "My" + biothing_name.title() + "Settings",
        "annotation_endpoint": biothing_name.lower(),
        "query_endpoint": "query",
        "annotation_handler_name": biothing_name.title() + "Handler",
        "query_handler_name": "QueryHandler",
        "es_doctype": biothing_name.lower(),
        "base_url": "My" + biothing_name.title() + ".info"
    }

    # override any key value pairs from the command line
    settings_dict.update(clargs)

    os.chdir(template_dir)

    # Template files out
    for (index, (dirpath, dirnames, filenames)) in enumerate(list(os.walk(template_dir))):
        thisdir = os.path.join(pdir, transform_name(os.path.relpath(dirpath), settings_dict))
        if index > 0:
            os.mkdir(thisdir)
        for fi in filenames:
            f = open(os.path.join(thisdir, transform_name(fi, settings_dict)), 'w')
            g = open(os.path.join(os.path.abspath(dirpath), fi), 'r')
            f.write(Template(g.read()).substitute(settings_dict))
            f.close()
            g.close()

    os.chdir(cwd)
    copytree(os.path.abspath('biothings'), os.path.join(pdir, settings_dict['src_package'], 'src', 'biothings'))

if __name__ == '__main__':
    main(sys.argv)