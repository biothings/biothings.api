import importlib
import os
import sys
import traceback
import types
from pathlib import Path
from typing import Union

from biothings import config
from biothings.hub.manager import BaseManager, UnknownResource, ResourceError
from biothings.utils.hub_db import get_src_conn
from biothings.utils.manager import JobManager

logger = config.logger


class SourceManagerError(Exception):
    pass


class BaseSourceManager(BaseManager):
    """
    Base class to provide source management: discovery, registration
    Actual launch of tasks must be defined in subclasses
    """

    # define the class manager will look for. Set in a subclass
    SOURCE_CLASS = None

    def __init__(self, job_manager: JobManager, poll_schedule=None, datasource_path: Union[str, Path] = None):
        super().__init__(job_manager, poll_schedule)

        if datasource_path is None:
            datasource_path = "dataload.sources"
        else:
            datasource_path = Path(datasource_path).resolve().absolute()
        self.default_src_path = datasource_path

        self.conn = get_src_conn()

    def filter_class(self, klass):
        """
        Gives opportunity for subclass to check given class and decide to
        keep it or not in the discovery process. Returning None means "skip it".
        """
        # keep it by default
        return klass

    def register_classes(self, klasses):
        """
        Register each class in self.register dict. Key will be used
        to retrieve the source class, create an instance and run method from it.
        It must be implemented in subclass as each manager may need to access
        its sources differently,based on different keys.
        """
        raise NotImplementedError("implement me in sub-class")

    def find_module_classes(self, source_module: types.ModuleType, fail_on_notfound: bool = True):
        """
        Given a python module, return a list of classes in this module, matching
        SOURCE_CLASS (must inherit from)

        Control Flow:
        1) First attempt to find classes explicitly defined within the plugin package
        2) If not found, then attempt to search within the package module directly
        """
        # try to find a uploader class in the module
        klasses = []
        for attr in dir(source_module):
            src_attribute = getattr(source_module, attr)
            # not interested in classes coming from biothings.hub.*, these would typically come
            # from "from biothings.hub.... import aclass" statements and would be incorrectly registered
            # we only look for classes defined straight from the actual module
            if (
                isinstance(src_attribute, type)
                and issubclass(src_attribute, self.__class__.SOURCE_CLASS)
                and not src_attribute.__module__.startswith("biothings.hub")
            ):
                klass = src_attribute
                if not self.filter_class(klass):
                    continue
                logger.debug("Found a class based on %s: '%s'", self.__class__.SOURCE_CLASS.__name__, klass)
                klasses.append(klass)

        if not klasses:
            try:
                src_m_path = source_module.__path__[0]
                for d in os.listdir(src_m_path):
                    if d.endswith("__pycache__"):
                        continue
                    modpath = os.path.join(source_module.__name__, d).replace(".py", "").replace(os.path.sep, ".")
                    try:
                        m = importlib.import_module(modpath)
                        klasses.extend(self.find_module_classes(m, fail_on_notfound))
                    except Exception as gen_exc:
                        logger.exception(gen_exc)
                        # (SyntaxError, ImportError) is not sufficient to catch
                        # all possible failures, for example a ValueError
                        # in module definition..
                        logger.debug("Couldn't import %s: %s", modpath, gen_exc)
                        continue
            except TypeError as e:
                logger.warning("Can't register source '%s', something's wrong with path: %s", source_module, e)

        if not klasses:
            if fail_on_notfound:
                raise UnknownResource(
                    f"Can't find a class based on {self.__class__.SOURCE_CLASS} in module '{source_module}'"
                )
        return klasses

    def register_source(self, src: Union[types.ModuleType, str, dict], fail_on_notfound: bool = True):
        """
        Register a new data source. src can be a module where some classes
        are defined. It can also be a module path as a string, or just a source name
        in which case it will try to find information from default path.
        https://peps.python.org/pep-0451/
        """
        logger.info("Attempting to load module %s", src)

        if src in sys.modules:
            logger.warning("%s module discovered in sys.modules", src)

        if isinstance(src, str):
            try:
                source_module = importlib.import_module(src)
                source_module = importlib.reload(source_module)
            except ImportError:
                logger.debug("Can't import module %s, attempting %s.%s", src, self.default_src_path, src)
                try:
                    source_module = importlib.import_module(f"{self.default_src_path}.{src}")
                    source_module = importlib.reload(source_module)
                except ImportError as inner_import_err:
                    import_err_msg = f"Can't import module '{self.default_src_path}.{src}'"
                    logger.exception(inner_import_err)
                    logger.error(import_err_msg)
                    raise UnknownResource(import_err_msg) from inner_import_err
                except Exception as gen_exc:
                    logger.exception(gen_exc)
                    search_err_message = f"Unable to import module '{self.default_src_path}.{src}'"
                    logger.error(search_err_message)
                    raise UnknownResource(search_err_message) from gen_exc

        elif isinstance(src, dict):
            # source has several other sub sources
            if len(src) != 1:
                raise SourceManagerError(f"Should have only one element in source dict '{src}'")

            _, sub_srcs = list(src.items())[0]
            for subsrc in sub_srcs:
                self.register_source(subsrc, fail_on_notfound)
            return
        elif isinstance(src, type.ModuleType):
            source_module = src

        klasses = self.find_module_classes(source_module, fail_on_notfound)
        logger.debug("Found classes to register: %s", repr(klasses))
        self.register_classes(klasses)

    def register_sources(self, sources: list):

        if isinstance(sources, str):
            raise SourceManagerError(f"Expected sources argument formatted as a list. Received string: {sources}")

        self.register.clear()
        for src in sources:
            try:
                self.register_source(src, fail_on_notfound=True)
            except (UnknownResource, ResourceError) as register_error:
                logger.exception(register_error)
                logger.error(traceback.format_exc())
                logger.warning("Unable to register source {src}. Skipping source registration ...")
