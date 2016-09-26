import sys
import os.path
import time
import random
import importlib

src_path = os.path.split(os.path.split(os.path.abspath(__file__))[0])[0]
sys.path.append(src_path)


def main(source):
    '''
    Example:
        python -m dataload/start ensembl
        python -m dataload/start entrez
        python -m dataload/start pharmgkb

    '''
    import biothings, config
    biothings.config_for_app(config)
    import dataload
    import biothings.dataload as btdataload

    # package1.module1.Class1
    default_klass = "biothings.dataload.uploader.SourceStorage"
    klass_path = getattr(config,"SOURCE_STORAGE_CLASS",default_klass)
    if not klass_path:
        klass_path = default_klass
    str_mod,str_klass = ".".join(klass_path.split(".")[:-1]),klass_path.split(".")[-1]
    mod = importlib.import_module(str_mod)
    klass = getattr(mod,str_klass)
    if "." in source:
        # partial upload of a datasource (only a sub-datasource)
        uploader = klass([source])
    else:
        if source not in dataload.__sources_dict__:
            raise ValueError('Unknown source "%s". Should be one of %s' % (source, dataload.__sources_dict__.keys()))
        # full upload of a datasource
        uploader = klass(dataload.__sources_dict__[source])
    uploader.register_sources()
    uploader.upload_all()

if __name__ == '__main__':
    main(sys.argv[1])
