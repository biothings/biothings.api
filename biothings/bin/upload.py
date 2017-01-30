import sys
import os.path
import time
import random
import importlib
from biothings.utils.common import get_class_from_classpath

src_path = os.path.split(os.path.split(os.path.abspath(__file__))[0])[0]
sys.path.append(src_path)


def main(source):

    import biothings, config
    biothings.config_for_app(config)
    import dataload
    import biothings.dataload as btdataload

    # package1.module1.Class1
    default_klass = "biothings.dataload.uploader.SourceManager"
    klass_path = getattr(config,"SOURCE_MANAGER_CLASS",default_klass)
    if not klass_path:
        klass_path = default_klass
    klass = get_class_from_classpath(klass_path)
    uploader = klass()
    uploader.register_sources(dataload.__sources_dict__)
    uploader.upload_src(source)

if __name__ == '__main__':
    main(sys.argv[1])
