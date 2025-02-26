"""
A utility to monitor source tree and trigger the sphinx_build when changes are detected.

Run it from the root of the src tree in a console as:

    python auto_rebuild.py
"""

import os.path
import subprocess

import tornado.autoreload
import tornado.ioloop

# an alternative:
# watchmedo shell-command --pattern="*.rst;*.py" --recursive --command="make html" .

included_ext = [".py", ".rst", ".css", ".html"]
doc_dir = "./docs"
src_dir = "."


def build():
    """callback function when a src file is changed."""
    subprocess.call("make html".split(), cwd=doc_dir)
    # restart dev server if needed (by touch a file)
    # subprocess.call('touch ../src/index.py'.split())


def watch_src(src_path):
    """Watch src files with the specified file extensions."""
    for dirname, _subdirs, fnames in os.walk(src_path):
        for fn in fnames:
            for ext in included_ext:
                if fn.endswith(ext):
                    f_path = os.path.join(dirname, fn)
                    tornado.autoreload.watch(f_path)
                    # print f_path, os.path.exists(f_path)


def main():
    tornado.autoreload.add_reload_hook(build)
    watch_src(src_dir)
    loop = tornado.ioloop.IOLoop.instance()
    tornado.autoreload.start()
    loop.start()


if __name__ == "__main__":
    main()
