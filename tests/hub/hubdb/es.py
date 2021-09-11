"""
(biothings) PS C:\Users\...\biothings.api\tests\hub\hubdb> python

Python 3.9.5 (tags/v3.9.5:0a7dcbd, May  3 2021, 17:27:52) [MSC v.1928 64 bit (AMD64)] on win32
Type "help", "copyright", "credits" or "license" for more information.

>>> from biothings.hub import config
>>> from biothings.utils import hub_db
>>> src_build = hub_db.get_src_build()
>>> src_build.insert_one({"_id": "one", "comment": "hi"})
>>> src_build.find_one({"comment": "hi"})
>>> src_build.replace_one({"_id": "one"}, {"extra": "ok"})

As of 9/10/2021, hub_db module relies on biothings.hub import,
this makes unit testing a single backend without a config file
difficult. The command above is the easiest approach to run it.

"""
