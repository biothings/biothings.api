import asyncio
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


def describe_error(error):
    return "%s: %s" % (type(error).__name__, error)


class SourcesFailed(ResourceError):
    """Raised by commands run on several sources (eg. dump_all) when some of them failed"""

    def __init__(self, action, summary, errors):
        self.summary = summary  # source name => what happened to it (see wait_for_sources())
        self.errors = errors  # source name => error
        failed = "; ".join("%s (%s)" % (name, describe_error(error)) for name, error in errors.items())
        message = "%s failed for %s of %s sources: %s" % (action, len(errors), len(summary), failed)
        done = [name for name, outcome in summary.items() if name not in errors and outcome != "skipped"]
        skipped = [name for name, outcome in summary.items() if outcome == "skipped"]
        if done:
            message += ". Done: %s" % ", ".join(done)
        if skipped:
            message += ". Skipped: %s" % ", ".join(skipped)
        super().__init__(message)


def wait_for_sources(action, jobs, raise_on_error=False):
    """
    Wait for the jobs run on several sources ({source name: [jobs]}, eg. by dump_all()). A source
    failing doesn't stop the others, unless raise_on_error: its error is logged when it happens, and
    once they're all done, SourcesFailed tells which sources failed, and how. Otherwise, returns what
    happened to each source: its job's result, "done" (no result), or "skipped" (no job, eg. a
    disabled dumper). Meanwhile, the returned task's "progress" (a list of lines) tells which
    sources are done, or failed (followed by terminals, see HubShell.refresh_commands()).
    """
    started = [name for name, source_jobs in jobs.items() if source_jobs]
    skipped = [name for name, source_jobs in jobs.items() if not source_jobs]
    progress = ["%s %s%s" % (action, ", ".join(started), " (skipped: %s)" % ", ".join(skipped) if skipped else "")]

    async def wait():
        sources = {}
        for name, source_jobs in jobs.items():
            for job in source_jobs:
                sources[asyncio.ensure_future(job)] = name
        left = {name: len(source_jobs) for name, source_jobs in jobs.items()}
        finished = 0
        results = {name: [] for name in jobs}
        errors = {}
        running = set(sources)
        while running:
            done, running = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
            for job in done:
                name = sources[job]
                left[name] -= 1
                error = asyncio.CancelledError("cancelled") if job.cancelled() else job.exception()
                if error is None:
                    results[name].append(job.result())
                    if not left[name] and name not in errors:
                        finished += 1
                        progress.append("%s done (%s/%s)" % (name, finished, len(started)))
                    continue
                if raise_on_error:
                    raise error
                if name not in errors:
                    finished += 1
                    progress.append("%s failed (%s/%s): %s" % (name, finished, len(started), describe_error(error)))
                errors.setdefault(name, error)
                still_running = sorted({sources[other] for other in running})
                logger.error(
                    "%s failed for %s: %s%s",
                    action,
                    name,
                    describe_error(error),
                    " (still running: %s)" % ", ".join(still_running) if still_running else "",
                )
        summary = {}
        for name in jobs:
            outcomes = [result for result in results[name] if result is not None]
            if name in errors:
                summary[name] = "failed: %s" % describe_error(errors[name])
            elif not jobs[name]:
                summary[name] = "skipped"
            else:
                summary[name] = outcomes[0] if len(outcomes) == 1 else (outcomes or "done")
        if errors:
            raise SourcesFailed(action, summary, errors) from next(iter(errors.values()))
        return summary

    task = asyncio.ensure_future(wait())
    task.progress = progress
    return task


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
