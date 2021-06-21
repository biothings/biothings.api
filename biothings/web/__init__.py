"""
    Biothings Web API 
    
    # TODO

    Run API:

    1a. with default settings:

        # serve all data on localhost:9200
        # may be used for testing and development

        from biothings.web import BiothingsAPI
        api = BiothingsAPI()
        api.start()

    1b. with a customized config module:

        # allow detailed configuration of apis
        # this is similar to how the hub starts web apis

        from biothings.web import BiothingsAPI
        import config
        api = BiothingsAPI(config)
        api.start()

    2. with default command line options:

        # by defualt looks for a 'config.py' under cwd
        # similar to discovery and crawler biothings apps

        from biothings.web.index_base import main
        if __name__ == '__main__':
            main()

    3. with application template framework. # TODO

    On top of the common ways described above, you can specify
    the customized config module in the following ways:

    1. a python module already imported

    2. a fully qualified name to import, for example:
     - 'config'
     - 'app.config.dev'

    3. a file path to a python module, for example:
     - '../config.py'
     - '/home/ubuntu/mygene/config.py'
     - 'C:\\Users\\Biothings\\mygene\\config.py'

    4. explicitly specify None or '' to use default.

    See below for additional configurations like
    using an external asyncio event loop.
"""



