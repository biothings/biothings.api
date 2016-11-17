import importlib
import logging
import asyncio, aiocron
import os, pickle, inspect
from functools import wraps, partial
import time

from biothings.utils.mongo import get_src_conn
from biothings import config


def track_process(func):
    @wraps(func)
    def func_wrapper(*args,**kwargs):
        # func is the do_work wrapper, we want the actual partial
        if type(args[0]) == partial:
            fname = args[0].func.__name__
            fargs = args[0].args
            fkwargs = args[0].keywords
        else:
            fname = args[0].__name__
            fargs = args[1:]
            fkwargs = kwargs
        pinfo = {'func_name' : fname,
                 'args': fargs, 'kwargs' : fkwargs,
                 'time': time.time()}
        try:
            pid = os.getpid()
            pidfile = os.path.join(config.RUN_DIR,"%s.pickle" % pid)
            pickle.dump(pinfo,open(pidfile,"wb"))
            func(*args,**kwargs)
        finally:
            if os.path.exists(pidfile):
                os.unlink(pidfile)
    return func_wrapper

@track_process
def do_work(func,*args,**kwargs):
    # need to wrap calls otherwise multiprocessing could have
    # issue pickling directly the passed func because of some import
    # issues ("can't pickle ... object is not the same as ...")
    return func(*args,**kwargs)


class UnknownResource(Exception):
    pass
class ResourceError(Exception):
    pass
class ManagerError(Exception):
    pass
class ResourceNotFound(Exception):
    pass

class BaseManager(object):

    def __init__(self, job_manager):
        self.register = {}
        self.job_manager = job_manager

    def __repr__(self):
        return "<%s [%d registered]: %s>" % (self.__class__.__name__,len(self.register), sorted(list(self.register.keys())))

    def __getitem__(self,src_name):
        try:
            # as a main-source
            return self.register[src_name]
        except KeyError:
            try:
                # as a sub-source
                main,sub = src_name.split(".")
                srcs = self.register[main]
                # there can be many uploader for one resource (when each is dealing
                # with one specific file but upload to the same collection for instance)
                # so we want to make sure user is aware of this and not just return one
                # uploader when many are needed
                # on the other hand, if only one avail, just return it
                res = []
                for src in srcs:
                    if src.name == sub:
                        res.append(src)
                if len(res) == 0:
                    raise KeyError(src_name)
                else:
                    return res
            except (ValueError,KeyError):
                # nope, can't find it...
                raise KeyError(src_name)



class BaseSourceManager(BaseManager):
    """
    Base class to provide source management: discovery, registration
    Actual launch of tasks must be defined in subclasses
    """

    # define the class manager will look for. Set in a subclass
    SOURCE_CLASS = None

    def __init__(self, job_manager, datasource_path="dataload.sources", *args, **kwargs):
        super(BaseSourceManager,self).__init__(job_manager,*args,**kwargs)
        self.conn = get_src_conn()
        self.default_src_path = datasource_path

    def filter_class(self,klass):
        """
        Gives opportunity for subclass to check given class and decide to
        keep it or not in the discovery process. Returning None means "skip it".
        """
        # keep it by default
        return klass

    def register_classes(self,klasses):
        """
        Register each class in self.register dict. Key will be used
        to retrieve the source class, create an instance and run method from it.
        It must be implemented in subclass as each manager may need to access 
        its sources differently,based on different keys.
        """
        raise NotImplementedError("implement me in sub-class")

    def find_classes(self,src_module,fail_on_notfound=True):
        """
        Given a python module, return a list of classes in this module, matching
        SOURCE_CLASS (must inherit from)
        """
        # try to find a uploader class in the module
        found_one = False
        for attr in dir(src_module):
            something = getattr(src_module,attr)
            if type(something) == type and issubclass(something,self.__class__.SOURCE_CLASS):
                klass = something
                if not self.filter_class(klass):
                    continue
                found_one = True
                logging.debug("Found a class based on %s: '%s'" % (self.__class__.SOURCE_CLASS.__name__,klass))
                yield klass
        if not found_one:
            if fail_on_notfound:
                raise UnknownResource("Can't find a class based on %s in module '%s'" % (self.__class__.SOURCE_CLASS.__name__,src_module))
            return []


    def register_source(self,src_data,fail_on_notfound=True):
        """Register a new data source. src_data can be a module where some classes
        are defined. It can also be a module path as a string, or just a source name
        in which case it will try to find information from default path.
        """
        if isinstance(src_data,str):
            try:
                src_m = importlib.import_module(src_data)
            except ImportError:
                try:
                    src_m = importlib.import_module("%s.%s" % (self.default_src_path,src_data))
                except ImportError:
                    msg = "Can't find module '%s', even in '%s'" % (src_data,self.default_src_path)
                    logging.error(msg)
                    raise UnknownResource(msg)

        elif isinstance(src_data,dict):
            # source is comprised of several other sub sources
            assert len(src_data) == 1, "Should have only one element in source dict '%s'" % src_data
            _, sub_srcs = list(src_data.items())[0]
            for src in sub_srcs:
                self.register_source(src,fail_on_notfound)
            return
        else:
            src_m = src_data
        klasses = self.find_classes(src_m,fail_on_notfound)
        self.register_classes(klasses)

    def register_sources(self, sources):
        assert not isinstance(sources,str), "sources argument is a string, should pass a list"
        self.register.clear()
        for src_data in sources:
            try:
# batch registration, we'll silently ignore not-found sources
                self.register_source(src_data,fail_on_notfound=False)
            except UnknownResource as e:
                logging.info("Can't register source '%s', skip it; %s" % (src_data,e))
                import traceback
                logging.error(traceback.format_exc())


class JobManager(object):

    def __init__(self, loop, process_queue=None,thread_queue=None):
        self.loop = loop
        self.process_queue = process_queue
        self.thread_queue = thread_queue

    def defer_to_process(self,func,*args):
        return self.loop.run_in_executor(self.process_queue,partial(do_work,func,*args))

    def defer_to_thread(self,func,*args):
        return self.loop.run_in_executor(self.thread_queue,func,*args)

    def submit(self,pfunc,schedule=None):
        """
        Helper to submit and run tasks. Tasks will run async'ly.
        pfunc is a functools.partial
        schedule is a string representing a cron schedule, task will then be scheduled
        accordingly.
        """
        logging.info("Building task: %s" % pfunc)
        if schedule:
            logging.info("Scheduling task %s: %s" % (pfunc,schedule))
            cron = aiocron.crontab(schedule,func=pfunc, start=True, loop=self.loop)
            return cron
        else:
            ff = asyncio.ensure_future(pfunc())
            return ff

