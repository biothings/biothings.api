"""
    Biothings API

    Support running biothings.web as a module

    >> python -m biothings.web
    >> python -m biothings.web --dir=~/mygene.info/src
    >> python -m biothings.web --dir=~/mygene.info/src --conf=config_web
    >> python -m biothings.web --conf=biothings.web.settings.default

    See more supported parameters in biothings.web.launcher.

"""

from biothings.web.launcher import main

main()
