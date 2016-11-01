import importlib
import logging
import asyncio, aiocron

from biothings.utils.mongo import get_src_conn

class UnknownResource(Exception):
    pass
class ResourceError(Exception):
    pass

class BaseSourceManager(object):
    """
    Base class to provide source management: discovery, registration
    Actual launch of tasks must be defined in subclasses
    """

    # define the class manager will look for. Set in a subclass
    SOURCE_CLASS = None

    def __init__(self, event_loop=None, datasource_path="dataload.sources"):
        self.src_register = {}
        self.conn = get_src_conn()
        self.default_src_path = datasource_path
        self.loop = event_loop

    def create_instances(self,klasses):
        for klass in klasses:
            res = self.create_instance(klass)
            # a true factory may return several instances
            if isinstance(res,list):
                for inst in res:
                    yield inst
            else:
                yield res

    def filter_class(self,klass):
        """
        Gives opportunity for subclass to check given class and decide to
        keep it or not in the discovery process. Returning None means "skip it".
        """
        # keep it by default
        return klass

    def register_instances(self,insts):
        """
        Register each instances in self.src_register dict. Key will be used
        to retrieve the source object and run method from it. It must be implemented
        in subclass as each manager may need to access its sources differently,based
        on different keys.
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
        instances = self.create_instances(klasses)
        self.register_instances(instances)

    def register_sources(self, sources):
        assert not isinstance(sources,str), "sources argument is a string, should pass a list"
        self.src_register.clear()
        for src_data in sources:
            try:
# batch registration, we'll silently ignore not-found sources
                self.register_source(src_data,fail_on_notfound=False)
            except UnknownResource as e:
                logging.info("Can't register source '%s', skip it; %s" % (src_data,e))
                import traceback
                logging.error(traceback.format_exc())

    def submit(self,pfunc,schedule=None):
        """
        Helper to submit and run tasks. If self.loop defined, tasks will run async'ly.
        pfunc is a functools.partial
        schedule is a string representing a cron schedule, task will then be scheduled 
        accordingly.
        """
        if schedule and not self.loop:
            raise ResourceError("Cannot schedule without an event loop")
        if self.loop:
            logging.info("Building task: %s" % pfunc)
            if schedule:
                logging.info("Scheduling task %s: %s" % (pfunc,schedule))
                cron = aiocron.crontab(schedule,func=pfunc, start=True, loop=self.loop)
                return cron
            else:
                ff = asyncio.ensure_future(pfunc())
                return ff
        else:
                return pfunc()

    def __repr__(self):
        return "<%s [%d registered]: %s>" % (self.__class__.__name__,len(self.src_register), sorted(list(self.src_register.keys())))

    def __getitem__(self,src_name):
        try:
            # as a main-source
            return self.src_register[src_name]
        except KeyError:
            try:
                # as a sub-source
                main,sub = src_name.split(".")
                srcs = self.src_register[main]
                # there can be many uploader for one resource (when each is dealing
                # with one specific file but upload to the same collection for instance)
                # so we want to make sure user is aware of this and not just return one
                # uploader when many are needed
                # on the other hand, if only one avail, just return it
                res = []
                for src in srcs:
                    if src.name == sub:
                        res.append(src)
                if len(res) == 1:
                    return res.pop()
                elif len(res) == 0:
                    raise KeyError(src_name)
                else:
                    return res
            except (ValueError,KeyError):
                # nope, can't find it...
                raise KeyError(src_name)


