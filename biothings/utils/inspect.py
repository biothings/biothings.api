"""
This module contains util functions may be shared by both BioThings data-hub and web components.
In general, do not include utils depending on any third-party modules.
"""
import math, statistics, random
import collections
import time, re, os
import logging
from pprint import pprint, pformat
import copy
from datetime import datetime
import bson, json

from .common import timesofar, is_scalar, is_float, is_str, is_int, splitstr
from .web.es import flatten_doc
from biothings.utils.dataload import dict_walk


class BaseMode(object):

    # dict storing the actual specific values the mode deals with
    template = {}
    # key under which values are stored for this mode
    key = None

    def report(self, struct, drep, orig_struct=None):
        """
        Given a data structure "struct" being inspected, report (fill)
        "drep" dictionary with useful values for this mode, under
        drep[self.key] key.
        Sometimes "struct" is already converted to its analytical value at
        this point (inspect may count number of dict and would force to pass
        struct as "1", instead of the whole dict, where number of keys could be
        then be reported), "orig_struct" is that case contains the original
        structure that was to be reported, whatever the pre-conversion step did.
        """
        raise NotImplementedError("Implement in sub-class")

    def merge(self, target, tomerge):
        """
        Merge two different maps together (from tomerge into target)
        """
        raise NotImplementedError("Implement in sub-class")

    def post(self, mapt, mode,clean):
        pass

class StatsMode(BaseMode):

    template = {"_stats" : {"_min":math.inf,"_max":-math.inf,"_count":0}}
    key = "_stats"

    def sumiflist(self, val):
        if type(val) == list:
            return sum(val)
        else:
            return val

    def maxminiflist(self, val, func):
        if type(val) == list:
            return func(val)
        else:
            return val

    def flatten_stats(self, stats):
        # after merge_struct, stats can be merged together as list (merge_struct
        # is only about data structures). Re-adjust here considering there could lists
        # that need to be sum'ed and min/max to be dealt with
        stats["_count"] = self.sumiflist(stats["_count"])
        stats["_max"] = self.maxminiflist(stats["_max"],max)
        stats["_min"] = self.maxminiflist(stats["_min"],min)
        return stats

    def report(self, struct, drep, orig_struct=None):
        if is_str(struct) or type(struct) in [dict,list]:
            val = len(struct)
        else:
            val = struct
        drep[self.key]["_count"] += 1
        if val < drep[self.key]["_min"]:
            drep[self.key]["_min"] = val
        if val > drep[self.key]["_max"]:
            drep[self.key]["_max"] = val

    def merge(self, target_stats, tomerge_stats):
        target_stats = self.flatten_stats(target_stats)
        tomerge_stats = self.flatten_stats(tomerge_stats)
        # sum the counts and the sums
        target_stats["_count"] = target_stats["_count"] + tomerge_stats["_count"]
        # adjust min and max
        if tomerge_stats["_max"] > target_stats["_max"]:
            target_stats["_max"] = tomerge_stats["_max"]
        if tomerge_stats["_min"] < target_stats["_min"]:
            target_stats["_min"] = tomerge_stats["_min"]


class DeepStatsMode(StatsMode):

    template = {"_stats" : {"_min":math.inf,"_max":-math.inf,"_count":0,"__vals":[]}}
    key = "_stats"

    def merge(self, target_stats, tomerge_stats):
        super(DeepStatsMode, self).merge(target_stats,tomerge_stats)
        # extend values
        target_stats.get("__vals",[]).extend(tomerge_stats.get("__vals",[]))

    def report(self, val, drep, orig_struct=None):
        super(DeepStatsMode, self).report(val,drep,orig_struct)
        # keep track of vals for now, stats are computed at the end
        drep[self.key]["__vals"].append(val)

    def post(self, mapt, mode,clean):
        if type(mapt) == dict:
            for k in list(mapt.keys()):
                if is_str(k) and k.startswith("__"):
                    if k == "__vals" and mode == "deepstats":
                        if len(mapt["__vals"]) > 1:
                            mapt["_stdev"] = statistics.stdev(mapt["__vals"])
                            mapt["_median"] = statistics.median(mapt["__vals"])
                            mapt["_mean"] = statistics.mean(mapt["__vals"])
                    if clean:
                        mapt.pop(k)
                else:
                    self.post(mapt[k],mode,clean)
        elif type(mapt) == list:
            for e in mapt:
                self.post(e,mode,clean)


class RegexMode(BaseMode):

    # list of {"re":"...","info":...}, if regex matches, then content
    # in "info" is used in report
    matchers = []

    def __init__(self):
        # pre-compile patterns
        for d in self.matchers:
            d["_pat"] = re.compile(d["re"])
        assert self.__class__.key, "Define class attribute 'key' in sub-class"
        self.__class__.template = {self.__class__.key : []}

    def merge(self, target, tomerge):
        # structure are lists (see template), just extend avoiding duplicated
        for e in tomerge:
            if not e in target:
                target.append(e)

    def report(self, val, drep, orig_struct=None):
        if not orig_struct is None:
            v = orig_struct
        else:
            v = val
        if is_scalar(v):
            sval = str(v)
            for dreg in self.matchers:
                if dreg["_pat"].match(sval):
                    for oneinfo in dreg["info"]:
                        if not oneinfo in drep.get(self.key,[]):
                            drep.setdefault(self.key,[]).append(oneinfo)


class IdentifiersMode(RegexMode):

    key = "_ident"
    # set this to a list of dict coming from http://identifiers.org/rest/collections
    ids = None

    matchers = None

    def __init__(self):
        if self.__class__.matchers is None:
            self.__class__.matchers = []
            res = {}
            # not initialized
            for ident in self.__class__.ids:
                res.setdefault(ident["pattern"],[]).append(ident)
            for pat,info in res.items():
                self.__class__.matchers.append({"re" : pat, "info" : info})
        super().__init__()


############################################################################

MODES_MAP = {
        "stats" : StatsMode,
        "deepstats" : DeepStatsMode,
        "identifiers" : IdentifiersMode,
        }

def get_mode_layer(mode):
    try:
        k = MODES_MAP[mode]
        return k() # instance is what's used
    except KeyError:
        return None


def merge_record(target,tomerge,mode):
    mode_inst = get_mode_layer(mode)
    for k in tomerge:
        if k in target:
            if mode_inst and mode_inst.key == k:
                tgt = target[mode_inst.key]
                tom = tomerge[mode_inst.key]
                mode_inst.merge(tgt,tom)
                continue
            if not isinstance(tomerge[k], collections.Iterable):
                continue
            for typ in tomerge[k]:
                # if not an actual type we need to merge further to reach them
                if mode_inst is None and (type(typ) != type or typ == list):
                    target[k].setdefault(typ,{})
                    target[k][typ] = merge_record(target[k][typ],tomerge[k][typ],mode)
                else:
                    if mode == "type":
                        # we can safely update and possibly overwrite
                        # target with tomerge's values and in mode "type"
                        # there's no actual information for scalar fields
                        # (eg a string field will be like: {"myfield" : {str:{}}}
                        target[k].update(tomerge[k])
                    elif mode == "mapping":
                        # keep track on splitable (precedence: splitable > non-splitable)
                        # so don't merge if target has a "split" and tomerge has not,
                        # as we would loose that information
                        if splitstr is typ:
                            target.pop(k)
                            target[k] = tomerge[k]
                    elif mode_inst:
                        if typ in target[k]:
                            # same key, same type, need to merge
                            if not mode_inst.key in tomerge[k][typ]:
                                # we try to merge record at a too higher level, need to merge deeper
                                target[k] = merge_record(target[k],tomerge[k],mode)
                                continue
                            tgt = target[k][typ][mode_inst.key]
                            tom = tomerge[k][typ][mode_inst.key]
                            mode_inst.merge(tgt,tom)
                        else:
                            target[k].setdefault(typ,{}).update(tomerge[k][typ])
                    else:
                        raise ValueError("Unknown mode '%s'" % mode)
        else:
            # key doesn't exist, create key
            if mode == "type":
                target.setdefault(k,{}).update(tomerge[k])
            else:
                if mode_inst:
                    # running special mode, we just set the keys in target
                    target.setdefault(k,tomerge[k])
                else:
                    target.setdefault(k,{}).update(tomerge[k])
                # if we already have splitstr and we want to merge str, skip it
                # as splitstr > str
                if str in target and splitstr in target:
                    target.pop(str)

    return target


def inspect(struct,key=None,mapt=None,mode="type",level=0,logger=logging):
    """
    Explore struct and report types contained in it.
    - struct: is the data structure to explore
    - mapt: if not None, will complete that type map with passed struct. This is usefull
      when iterating over a dataset of similar data, trying to find a good type summary 
      contained in that dataset.
    - (level: is for internal purposes, mostly debugging)
    - mode: see inspect_docs() documentation
    """

    mode_inst = get_mode_layer(mode)

    # init recording structure if none were passed
    if mapt is None:
        mapt = {}

    if type(struct) == dict:
        # was this struct already explored before ? was it a list for that previous doc ?
        # then we have to pretend here it's also a list even if not, because we want to
        # report the list structure
        for k in struct:
            if mapt and list in mapt:# and key == k:
                already_explored_as_list = True
            else:
                already_explored_as_list = False
            if False:#already_explored_as_list:
                mapt[list].setdefault(k,{})
                typ = inspect(struct[k],key=k,mapt=mapt[list][k],mode=mode,level=level+1)
                mapt[list].update({k:typ})
            else:
                mapt.setdefault(k,{})
                typ = inspect(struct[k],key=k,mapt=mapt[k],mode=mode,level=level+1)

        if mode_inst:
            mapt.setdefault(mode_inst.key,copy.deepcopy(mode_inst.template[mode_inst.key]))
            mode_inst.report(1,mapt,struct)
    elif type(struct) == list:

        mapl = {}
        for e in struct:
            typ = inspect(e,key=key,mapt=mapl,mode=mode,level=level+1)
            mapl.update(typ)
        if mode_inst:
            # here we just report that one document had a list
            mapl.update(copy.deepcopy(mode_inst.template))
            mode_inst.report(struct,mapl)
        # if mapt exist, it means it's been explored previously but not as a list,
        # instead of mixing dict and list types, we want to normalize so we merge the previous
        # struct into that current list
        if mapt and list in mapt:
            mapt[list] = merge_record(mapt[list],mapl,mode)
        else:
            mapt.setdefault(list,{})
            mapt[list].update(mapl)
    elif is_scalar(struct) or type(struct) == datetime:
        typ = type(struct)
        if mode == "type":
            mapt[typ] = {}
        elif mode == "mapping":
            # some type precedence processing...
            # splittable string ?
            if is_str(struct) and len(re.split(" +",struct.strip())) > 1:
                mapt[splitstr] = {}
            elif typ == bson.int64.Int64:
                mapt[int] = {}
            else:
                mapt[typ] = {}
            # splitstr > str
            if str in mapt and splitstr in mapt:
                mapt.pop(str)
            # float > int
            # TODO: could this be moved to es.generate_es_mapping ?
            if int in mapt and float in mapt:
                mapt.pop(int)
        else:
            mapt.setdefault(typ,copy.deepcopy(mode_inst.template))
            mode_inst.report(struct,mapt[typ])
    else:
        raise TypeError("Can't analyze type %s (data was: %s)" % (type(struct),struct))

    return mapt

def merge_scalar_list(mapt,mode):
    # TODO: this looks "strangely" to merge_record... refactoring needed ?
    # if a list is found and other keys at same level are found in that
    # list, then we need to merge. Ex: ...,{"bla":1},["bla":2],...
    mode_inst = get_mode_layer(mode)
    if "stats" in mode:
        raise NotImplementedError("merging with stats is not supported (yet)")
    if is_scalar(mapt):
        return
    if list in mapt.keys():
        other_keys = [k for k in mapt if k != list]
        for e in other_keys:
            if e in mapt[list]:
                tomerge = mapt.pop(e)
                if mode_inst:
                    for typ in tomerge:
                        if not type(typ) == type:
                            continue
                        if not typ in mapt[list][e]:
                            mapt[list][e][typ] = tomerge[typ]
                        # Note: don't update [list]["_stats" (or other modes' key)], we keep the original stats
                        # that is, what's actually been inspected on the list, originally
                        # (and we can't really update those stats as scalar stats aren't relevant
                        # to a list context
                        elif typ == mode_inst.key:
                            mode_inst.merge(mapt[list][e][mode_inst.key],tomerge[mode_inst.key])
                            pass
                        else:
                            mode_inst.merge(mapt[list][e][typ][mode_inst.key],tomerge[typ][mode_inst.key])
                            pass
                elif mode == "mapping":
                    for typ in tomerge:
                        if typ is str and splitstr in mapt[list][e]:
                            # precedence splitstr > str, we keep splitstr and ignore str
                            continue
                        if not typ in mapt[list][e]:
                            # that field exist in the [list] but with a different type
                            # just merge the typ
                            mapt[list][e].update(tomerge)
                        # precedence splitstr > str
                        if splitstr is typ:
                            mapt[list][e].pop(str,None)
                            mapt[list][e].update(tomerge)
                else:
                    # assuming what's in [list] is enough, we just popped the value
                    # from mapt, that's enough
                    pass
        # explore further
        merge_scalar_list(mapt[list],mode)
    elif type(mapt) == dict:
        for k in mapt:
            merge_scalar_list(mapt[k],mode)
    elif type(mapt) == list:
        for e in mapt:
            merge_scalar_list(e,mode)


def get_converters(modes,logger=logging):
    converters = []
    # should we actually run another mode and then convert the results ?
    if "jsonschema" in modes:
        from biothings.utils.jsonschema import generate_json_schema
        # first get schema with mode="type", then convert the results
        # note "type" can't also be specified as jsonschema will replace
        # the results in _map["type"] key
        converters.append(
                {
                    "output_mode" : "jsonschema",
                    "input_mode" : "type",
                     "func" : generate_json_schema,
                     "delete_input_mode" : not "type" in modes
                })
        modes.remove("jsonschema")
        if not "type" in modes:
            modes.append("type")

    return converters, modes


def run_converters(_map,converters,logger=logging):
    # need to convert some results ?
    for converter in converters:
        logger.info("Finalizing result for mode '%s' using converter %s" % (converter["output_mode"],converter))
        converted = converter["func"](_map[converter["input_mode"]])
        _map[converter["output_mode"]] = converted
        if converter["delete_input_mode"]:
            _map.pop(converter["input_mode"])

def inspect_docs(docs, mode="type", clean=True, merge=False, logger=logging,
                 pre_mapping=False, limit=None, sample=None, metadata=True,
                 auto_convert=True):
    """Inspect docs and return a summary of its structure:
    - mode:
        + "type": explore documents and report strict data structure
        + "mapping": same as type but also perform test on data so guess best mapping
          (eg. check if a string is splitable, etc...). Implies merge=True
        + "stats": explore documents and compute basic stats (count,min,max,sum)
        + "deepstats": same as stats but record values and also compute mean,stdev,median
          (memory intensive...)
        + "jsonschema", same as "type" but returned a json-schema formatted result
      (mode can also be a list of modes, eg. ["type","mapping"]. There's little
       overhead computing multiple types as most time is spent on actually getting the data)
    - clean: don't delete recorded vqlues or temporary results
    - merge: merge scalar into list when both exist (eg. {"val":..} and [{"val":...}]
    - limit: can limit the inspection to the x first docs (None = no limit, inspects all)
    - sample: in combination with limit, randomly extract a sample of 'limit' docs
              (so not necessarily the x first ones defined by limit). If random.random()
              is greater than sample, doc is inspected, otherwise it's skipped
    - metadata: compute metadata on the result
    - auto_convert: run converters automatically (converters are used to convert one mode's
                    output to another mode's output, eg. type to jsonschema)
    """

    if type(mode) == str:
        modes = [mode]
    else:
        modes = mode
    if auto_convert:
        converters,modes = get_converters(modes,logger=logger)
    _map = {}
    for m in modes:
        _map[m] = {}
    cnt = 0
    errors = set()
    t0 = time.time()
    innert0 = time.time()

    if not sample is None:
        assert limit, "Parameter 'sample' requires 'limit' to be defined"
        assert sample != 1, "Sample value 1 not allowed (no documents would be inspected)"
    if limit:
        limit = int(limit)
        logger.debug("Limiting inspection to the %s first documents" % limit)
    for doc in docs:
        if not sample is None:
            if random.random() <= sample:
                continue
        for m in modes:
            try:
                inspect(doc,mapt=_map[m],mode=m)
            except Exception as e:
                logging.exception("Can't inspect document (_id: %s) because: %s\ndoc: %s" % (doc.get("_id"),e,pformat("dpc")))
                errors.add(str(e))
        cnt += 1
        if cnt % 10000 == 0:
            logger.info("%d documents processed [%s]" % (cnt,timesofar(innert0)))
            innert0 = time.time()
        if limit and cnt > limit:
            logger.debug("done")
            break
    logger.info("Done [%s]" % timesofar(t0))
    logger.info("Post-processing")

    # post-process, specific for each mode
    for m in modes:
        mode_inst = get_mode_layer(m)
        if mode_inst:
            mode_inst.post(_map[m],m,clean)

    if auto_convert:
        run_converters(_map,converters,logger=logger)

    merge = "mapping" in modes and True or merge
    if merge:
        merge_scalar_list(_map["mapping"],"mapping")
    if "mapping" in modes and pre_mapping is False:
        # directly generate ES mapping
        import biothings.utils.es as es
        try:
            _map["mapping"] = es.generate_es_mapping(_map["mapping"])
            if metadata:
                # compute some extra metadata
                _map = compute_metadata(_map,"mapping")
        except es.MappingError as e:
            prem = {"pre-mapping" : _map["mapping"], "errors" : e.args[1]}
            _map["mapping"] = prem
    elif errors:
        _map["errors"] = errors
    return _map

def compute_metadata(mapt,mode):
    if mode == "mapping":
        flat = flatten_doc(mapt["mapping"])
        # total fields: ES6 requires to overcome the default 1000 limit if needed
        mapt["__metadata__"] = {"total_fields" : len(flat)}

    return mapt


def typify_inspect_doc(dmap):
    """
    dmap is an inspect which was converted to be stored in a database,
    namely actual python types were stringify to be storabled. This function
    does the oposite and restore back python types within the inspect doc
    """
    def typify(val):
        if type(val) != type and val.startswith("__type__:"):
            typ = val.replace("__type__:","")
            # special cases
            if typ == "NoneType":
                return None
            elif typ == "Int64": # bson's Int64
                return bson.int64.Int64
            else:
                return eval(val.replace("__type__:",""))
        else:
            return val
    return dict_walk(dmap,typify)


def stringify_inspect_doc(dmap):
    def stringify(val):
        if type(val) == type:
            return "__type__:%s" % val.__name__ # prevent having dots in the field (not storable in mongo)
        else:
            return str(val)
    return dict_walk(dmap,stringify)


if __name__ == "__main__":
    d1 = {"id" : "124",'lofd': [{"val":34.3},{"ul":"bla"}],"d":{"start":134,"end":5543}}
    d2 = {"id" : "5",'lofd': {"oula":"mak","val":34},"d":{"start":134,"end":5543}}
    d3 = {"id" : "890",'lofd': [{"val":34}],"d":{"start":134,"end":5543}}

    # merge either ways in the same
    m12 = inspect_docs([d1,d2])["type"]
    m21 = inspect_docs([d2,d1])["type"]
    #if undordered list, then:
    assert m21 == m12, "\nm21=%s\n!=\nm12=%s" % (pformat(m21),pformat(m12))
    # val can be an int and a float
    m1 = inspect_docs([{"val":34},{"val":1.2}])["type"]
    # set: types can be in any order
    assert set(m1["val"]) ==  {int,float}
    # even if val is in a list
    m2 = inspect_docs([{"val":34},[{"val":1.2}]])["type"]
    # list and val not merged
    assert set(m2.keys()) == {'val',list}
    # another example with a mix a dict and list (see "p")
    od1 = {"id" : "124","d":[{"p":123},{"p":456}]}
    od2 = {"id" : "124","d":[{"p":123},{"p":[456,789]}]}
    m12 = inspect_docs([od1,od2],mode="type")["type"]
    m21 = inspect_docs([od2,od1],mode="type")["type"]
    assert m12 == m21
    # "p" is a integer or a list of integer
    assert m12["d"][list]["p"].keys() == {list,int}

    # stats
    m = {}
    inspect(d1,mapt=m,mode="stats")
    # some simple check
    assert set(m["id"].keys()) == {str}
    assert m["id"][str]["_stats"]["_count"] == 1
    assert m["id"][str]["_stats"]["_max"] == 3
    assert m["id"][str]["_stats"]["_min"] == 3
    assert m["lofd"].keys() == {list}
    # list's stats
    assert m["lofd"][list]["_stats"]["_count"] == 1
    assert m["lofd"][list]["_stats"]["_max"] == 2
    assert m["lofd"][list]["_stats"]["_min"] == 2
    # one list's elem stats
    assert m["lofd"][list]["val"][float]["_stats"]["_count"] == 1
    assert m["lofd"][list]["val"][float]["_stats"]["_max"] == 34.3
    assert m["lofd"][list]["val"][float]["_stats"]["_min"] == 34.3
    # again
    inspect(d1,mapt=m,mode="stats")
    assert m["id"][str]["_stats"]["_count"] == 2
    assert m["id"][str]["_stats"]["_max"] == 3
    assert m["id"][str]["_stats"]["_min"] == 3
    assert m["lofd"][list]["_stats"]["_count"] == 2
    assert m["lofd"][list]["_stats"]["_max"] == 2
    assert m["lofd"][list]["_stats"]["_min"] == 2
    assert m["lofd"][list]["val"][float]["_stats"]["_count"] == 2
    assert m["lofd"][list]["val"][float]["_stats"]["_max"] == 34.3
    assert m["lofd"][list]["val"][float]["_stats"]["_min"] == 34.3
    # mix with d2
    inspect(d2,mapt=m,mode="stats")
    assert m["id"][str]["_stats"]["_count"] == 3
    assert m["id"][str]["_stats"]["_max"] == 3
    assert m["id"][str]["_stats"]["_min"] == 1 # new min
    assert m["lofd"][list]["_stats"]["_count"] == 2 # not incremented as in d2 it's not a list
    assert m["lofd"][list]["_stats"]["_max"] == 2
    assert m["lofd"][list]["_stats"]["_min"] == 2
    # now float & int
    assert m["lofd"][list]["val"][float]["_stats"]["_count"] == 2
    assert m["lofd"][list]["val"][float]["_stats"]["_max"] == 34.3
    assert m["lofd"][list]["val"][float]["_stats"]["_min"] == 34.3
    # val{int} wasn't merged
    assert m["lofd"]["val"][int]["_stats"]["_count"] == 1
    assert m["lofd"]["val"][int]["_stats"]["_max"] == 34
    assert m["lofd"]["val"][int]["_stats"]["_min"] == 34
    # d2 again
    inspect(d2,mapt=m,mode="stats")
    assert m["id"][str]["_stats"]["_count"] == 4
    assert m["id"][str]["_stats"]["_max"] == 3
    assert m["id"][str]["_stats"]["_min"] == 1
    assert m["lofd"][list]["_stats"]["_count"] == 2
    assert m["lofd"][list]["_stats"]["_max"] == 2
    assert m["lofd"][list]["_stats"]["_min"] == 2
    assert m["lofd"][list]["val"][float]["_stats"]["_count"] == 2
    assert m["lofd"][list]["val"][float]["_stats"]["_max"] == 34.3
    assert m["lofd"][list]["val"][float]["_stats"]["_min"] == 34.3
    assert m["lofd"]["val"][int]["_stats"]["_count"] == 2
    assert m["lofd"]["val"][int]["_stats"]["_max"] == 34
    assert m["lofd"]["val"][int]["_stats"]["_min"] == 34

    # all counts should be 10
    m = inspect_docs([d1] * 10,mode="stats")["stats"]
    assert m["d"]["end"][int]["_stats"]["_count"] == 10
    assert m["d"]["start"][int]["_stats"]["_count"] == 10
    assert m["id"][str]["_stats"]["_count"] == 10
    assert m["lofd"][list]["_stats"]["_count"] == 10
    assert m["lofd"][list]["ul"][str]["_stats"]["_count"] == 10
    assert m["lofd"][list]["val"][float]["_stats"]["_count"] == 10

    #### test merge_stats
    nd1 = {"id" : "124",'lofd': [{"val":34.3},{"ul":"bla"}]}
    nd2 = {"id" : "5678",'lofd': {"val":50.2}}
    m = {}
    inspect(nd1,mapt=m,mode="deepstats")
    inspect(nd2,mapt=m,mode="deepstats")
    assert set(m["lofd"].keys()) == {list,'val','_stats'}, "%s" % m["lofd"].keys()
    assert m["lofd"][list]["val"][float]["_stats"] == {'__vals': [34.3], '_count': 1, '_max': 34.3, '_min': 34.3}, \
            m["lofd"][list]["val"][float]["_stats"]
    # merge stats into the left param
    DeepStatsMode().merge(m["lofd"][list]["val"][float]["_stats"],m["lofd"]["val"][float]["_stats"])
    assert m["lofd"][list]["val"][float]["_stats"] == {'__vals': [34.3, 50.2], '_count': 2, '_max': 50.2, '_min': 34.3}

    # mapping mode (splittable strings)
    # "bla" is splitable in one case, not in the other
    # "oula" is splitable, "arf" is not
    sd1 = {"_id" : "124",'vals': [{"oula":"this is great"},{"bla":"I am splitable","arf":"ENS355432"}]}
    sd2 = {"_id" : "5678",'vals': {"bla":"rs45653","void":654}}
    sd3 = {"_id" : "124",'vals': [{"bla":"thisisanid"}]}
    m = {}
    inspect(sd3,mapt=m,mode="mapping")
    # bla not splitable here
    assert m["vals"][list]["bla"][str] == {}
    inspect(sd1,mapt=m,mode="mapping")
    # now it is
    assert m["vals"][list]["bla"][splitstr] == {}
    inspect(sd2,mapt=m,mode="mapping")
    # not splitable in sd2
    assert m["vals"]["bla"][str] == {}
    # mapping with type of type
    sd1 = {"_id" : "123","homologene" : {"id":"bla","gene" : [[123,456],[789,102]]}}
    m = inspect_docs([sd1],mode="mapping")["mapping"]
    assert m == {'homologene': {'properties': {'gene': {'type': 'integer'},
        'id': {'normalizer': 'keyword_lowercase_normalizer', 'type': 'keyword'}}}}, "mapping %s" % m

    # ok, "bla" is either a scalar or in a list, test merge
    md1 = {"_id" : "124",'vals': [{"oula":"this is great"},{"bla":"rs24543","arf":"ENS355432"}]}
    md2 = {"_id" : "5678",'vals': {"bla":"I am splitable in a scalar","void":654}}
    # bla is a different type here
    md3 = {"_id" : "5678",'vals': {"bla":1234}}
    m = inspect_docs([md1,md2],mode="mapping",pre_mapping=True)["mapping"] # "mapping" implies merge=True
    assert not "bla" in m["vals"]
    assert m["vals"][list]["bla"] == {splitstr: {}}, m["vals"][list]["bla"] # splittable str from md2 merge to list
    m = inspect_docs([md1,md3],mode="mapping",pre_mapping=True)["mapping"]
    assert not "bla" in m["vals"]
    assert m["vals"][list]["bla"] == {int: {}, str: {}} # keep as both types
    m = inspect_docs([md1,md2,md3],mode="mapping",pre_mapping=True)["mapping"]
    assert not "bla" in m["vals"]
    assert m["vals"][list]["bla"] == {int: {}, splitstr: {}}, m["vals"][list]["bla"]# splittable kept + merge int to keep both types

    #### test merge scalar/list with stats
    #### unmerged is a inspect-doc with mode=stats, structure is:
    #### id and name keys are both as root keys and in [list]
    ###insdoc = {list:
    ###                {'_stats': {'_count': 10, '_max': 200, '_sum': 1000, '_min': 2},
    ###                 'id': {str: {'_stats': {'_count': 100, '_max': 10, '_sum': 1000, '_min': 1}}},
    ###                 'name': {str: {'_stats': {'_count': 500, '_max': 5, '_sum': 500, '_min': 0.5}}}},
    ###            'id': {str: {'_stats': {'_count': 300, '_max': 30, '_sum': 300, '_min': 3}},
    ###                   int: {'_stats': {'_count': 1, '_max': 1, '_sum': 1, '_min': 1}}},
    ###            'name': {str: {'_stats': {'_count': 400, '_max': 40, '_sum': 4000, '_min': 4}}}}
    ###merge_scalar_list(insdoc,mode="stats")
    #### root keys have been merged into [llist] (even id as an integer, bc it's merged based on
    #### key name, not key name *and* type
    ###assert list(insdoc) == [list]
    #### check merged stats for "id"
    ###assert insdoc[list]["id"][str]["_stats"]["_count"] == 400    # 300 + 100
    ###assert insdoc[list]["id"][str]["_stats"]["_max"] == 30       # from root key
    ###assert insdoc[list]["id"][str]["_stats"]["_min"] == 1        # from list key
    ###assert insdoc[list]["id"][str]["_stats"]["_sum"] == 1300     # 1000 + 300
    #### "id" as in integer is also merged, stats are kept
    ###assert insdoc[list]["id"][int]["_stats"]["_count"] == 1
    ###assert insdoc[list]["id"][int]["_stats"]["_max"] == 1
    ###assert insdoc[list]["id"][int]["_stats"]["_min"] == 1
    ###assert insdoc[list]["id"][int]["_stats"]["_sum"] == 1
    #### check merged stats for "name"
    ###assert insdoc[list]["name"][str]["_stats"]["_count"] == 900    # 500 + 400
    ###assert insdoc[list]["name"][str]["_stats"]["_max"] == 40       # from root key
    ###assert insdoc[list]["name"][str]["_stats"]["_min"] == 0.5      # from list key
    ###assert insdoc[list]["name"][str]["_stats"]["_sum"] == 4500     # 4000 + 500
    #### [list] stats unchanged
    ###assert insdoc[list]["_stats"]["_count"] == 10
    ###assert insdoc[list]["_stats"]["_max"] == 200
    ###assert insdoc[list]["_stats"]["_min"] == 2
    ###assert insdoc[list]["_stats"]["_sum"] == 1000

    d1 = {'go': {'BP': {'term': 'skeletal muscle fiber development', 'qualifier': 'NOT', 'pubmed': 1234, 'id': \
        'GO:0048741', 'evidence': 'IBA'}}, '_id': '101362076'}
    d2 = {'go': {'BP': [{'term': 'ubiquitin-dependent protein catabolic process', 'pubmed': 5678, 'id': 'GO:0006511', \
        'evidence': 'IEA'}, {'term': 'protein deubiquitination', 'pubmed': [2222, 3333], 'id': 'GO:0016579', 'evidence': \
            'IEA'}]}, '_id': '101241878'}

    m = inspect_docs([d1,d1,d2,d2],mode="stats")["stats"]

    # more merge tests involving real case, deeply nested
    # here, go.BP contains a list and some scalars that should be merge
    # together, but also go.BP.pubmed also contains list and scalars
    # needed to be merged together
    insdocdeep = {'_id': {str: {}},
            'go': {
                'BP': {
                    'evidence': {str: {}},
                    'id': {str: {}},
                    'pubmed': {
                        list: {int: {}},
                        int: {}},
                    'qualifier': {str: {}},
                    'term': {str: {}},
                    list: {
                        'evidence': {str: {}},
                        'id': {str: {}},
                        'pubmed': {
                            list: {int: {}},
                            int: {}},
                        'qualifier': {str: {}},
                        'term': {str: {}}},
                    }
                }
            }
    merge_scalar_list(insdocdeep,mode="type")
    # we merge the first level
    assert list(insdocdeep["go"]["BP"].keys()) == [list]
    # and also the second one
    assert list(insdocdeep["go"]["BP"][list]["pubmed"].keys()) == [list]

    # merge_scalar_list when str split involved (?) in list of list
    doc = {"_id":"1","f":["b",["a 0","b 1"]]}

    # merge list of str and splitstr
    docb = {"_id":"1","f":["a 0"]}
    docg = {"_id":"1","f":["a0"]}
    m = inspect_docs([docb,docg],mode="mapping")
    assert m["mapping"]["f"] == {"type":"text"} # splitstr > str
    # same when strings (not list)
    docb = {"_id":"1","f":"a 0"}
    docg = {"_id":"1","f":"a0"}
    m = inspect_docs([docb,docg],mode="mapping")
    assert m["mapping"]["f"] == {"type":"text"} # splitstr > str
    # same when strings and list of strings
    doc1 = {"_id":"1","f":["a 0"]}
    doc2 = {"_id":"1","f":["a0"]}
    doc3 = {"_id":"1","f":"a 0"}
    doc4 = {"_id":"1","f":"a0"}
    m = inspect_docs([doc1,doc2,doc3,doc4],mode="mapping")
    assert m["mapping"]["f"] == {"type":"text"} # splitstr > str
    doc1 = {"_id":"1","f":["a0"]}
    doc2 = {"_id":"1","f":["a 0"]}
    m = inspect_docs([doc1,doc2],mode="mapping")
    assert m["mapping"]["f"] == {"type":"text"} # splitstr > str

    # splitstr > str whatever the order they appear while inspected (here: splitstr,str,str, in list,list,dict)
    d1 = {'_id': 'a', 'r': {'k': [{'id': 'one', 'rel': 'is'}, {'id': 'two', 'rel': 'simil to'}]}}
    d2 = {'_id': 'b', 'r': {'k': [{'id': 'three', 'rel': 'is'}, {'id': 'four', 'rel': 'is'}]}}
    d3 = {'_id': 'c', 'r': {'k': {'id': 'five', 'rel': 'is'}}}
    m = inspect_docs([d1,d2,d3],mode="mapping")
    assert "errors" not in m["mapping"]

    # merge_record splitstr > str
    d1 = {'_id': {str: {}}, 'k': {'a': {list: {'i': {str: {}}, 'r': {str: {}}}}}}
    d2 = {'_id': {str: {}},'k': {'a': {list: {'i': {str: {}},'r': {splitstr: {}}}}}}
    m = {}
    m = merge_record(m,d1,"mapping")
    m = merge_record(m,d2,"mapping")
    assert m["k"]["a"][list]["r"] == {splitstr:{}}

    # allow int & float in mapping (keep float
    t1 = {"_id":"a","f":[1,2]}
    t2 = {"_id":"a","f":[1.1,2.2]}
    m = inspect_docs([t1,t2],mode="mapping")
    assert m["mapping"]["f"]["type"] == "float"


    print("All tests OK")


