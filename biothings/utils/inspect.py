"""
This module contains util functions may be shared by both BioThings data-hub and web components.
In general, do not include utils depending on any third-party modules.
Note: unittests available in biothings.tests.hub
"""
import math
import statistics
import random
import collections
import time
import re
import logging
import copy
from pprint import pformat
from datetime import datetime

import bson

from biothings.utils.common import timesofar, is_scalar, is_str, splitstr, nan, inf
from biothings.utils.web.es import flatten_doc
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

    def post(self, mapt, mode, clean):
        pass

class StatsMode(BaseMode):

    template = {
        "_stats": {
            "_min": math.inf,
            "_max": -math.inf,
            "_count": 0,
            "_none": 0
        }
    }
    key = "_stats"

    def sumiflist(self, val):
        # if type(val) == list:   # TODO: remove this line
        if isinstance(val, list):
            return sum(val)
        else:
            return val

    def maxminiflist(self, val, func):
        # if type(val) == list:   # TODO: remove this line
        if isinstance(val, list):
            return func(val)
        else:
            return val

    def flatten_stats(self, stats):
        # after merge_struct, stats can be merged together as list (merge_struct
        # is only about data structures). Re-adjust here considering there could lists
        # that need to be sum'ed and min/max to be dealt with
        stats["_count"] = self.sumiflist(stats["_count"])
        stats["_max"] = self.maxminiflist(stats["_max"], max)
        stats["_min"] = self.maxminiflist(stats["_min"], min)
        return stats

    def report(self, struct, drep, orig_struct=None):
        # if is_str(struct) or type(struct) in [dict, list]:    # TODO: remove this line
        if is_str(struct) or isinstance(struct, (dict, list)):
            val = len(struct)
        else:
            val = struct
        drep[self.key]["_count"] += 1
        if val is None:
            drep[self.key]["_none"] += 1
        else:
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

    template = {"_stats": {"_min": math.inf, "_max": -math.inf, "_count": 0, "__vals": []}}
    key = "_stats"

    def merge(self, target_stats, tomerge_stats):
        super(DeepStatsMode, self).merge(target_stats, tomerge_stats)
        # extend values
        target_stats.get("__vals", []).extend(tomerge_stats.get("__vals", []))

    def report(self, val, drep, orig_struct=None):
        super(DeepStatsMode, self).report(val, drep, orig_struct)
        # keep track of vals for now, stats are computed at the end
        drep[self.key]["__vals"].append(val)

    def post(self, mapt, mode, clean):
        if isinstance(mapt, dict):
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
                    self.post(mapt[k], mode, clean)
        elif isinstance(mapt, list):
            for e in mapt:
                self.post(e, mode, clean)


class RegexMode(BaseMode):

    # list of {"re":"...","info":...}, if regex matches, then content
    # in "info" is used in report
    matchers = []

    def __init__(self):
        # pre-compile patterns
        for d in self.matchers:
            d["_pat"] = re.compile(d["re"])
        assert self.__class__.key, "Define class attribute 'key' in sub-class"
        self.__class__.template = {self.__class__.key: []}

    def merge(self, target, tomerge):
        # structure are lists (see template), just extend avoiding duplicated
        for e in tomerge:
            if e not in target:
                target.append(e)

    def report(self, val, drep, orig_struct=None):
        if orig_struct is not None:
            v = orig_struct
        else:
            v = val
        if is_scalar(v):
            sval = str(v)
            for dreg in self.matchers:
                if dreg["_pat"].match(sval):
                    for oneinfo in dreg["info"]:
                        if oneinfo not in drep.get(self.key, []):
                            drep.setdefault(self.key, []).append(oneinfo)


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
                res.setdefault(ident["pattern"], []).append(ident)
            for pat, info in res.items():
                self.__class__.matchers.append({"re": pat, "info": info})
        super().__init__()


############################################################################

MODES_MAP = {
    "stats": StatsMode,
    "deepstats": DeepStatsMode,
    "identifiers": IdentifiersMode
}

def get_mode_layer(mode):
    try:
        k = MODES_MAP[mode]
        return k()  # instance is what's used
    except KeyError:
        return None


def merge_record(target, tomerge, mode):
    mode_inst = get_mode_layer(mode)
    for k in tomerge:
        if k in target:
            if mode_inst and mode_inst.key == k:
                tgt = target[mode_inst.key]
                tom = tomerge[mode_inst.key]
                mode_inst.merge(tgt, tom)
                continue
            if not isinstance(tomerge[k], collections.Iterable):
                continue
            for typ in tomerge[k]:
                # if not an actual type we need to merge further to reach them
                if mode_inst is None and (type(typ) != type or typ == list):
                    target[k].setdefault(typ, {})
                    target[k][typ] = merge_record(target[k][typ], tomerge[k][typ], mode)
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
                            if mode_inst.key not in tomerge[k][typ]:
                                # we try to merge record at a too higher level, need to merge deeper
                                target[k] = merge_record(target[k], tomerge[k], mode)
                                continue
                            tgt = target[k][typ][mode_inst.key]
                            tom = tomerge[k][typ][mode_inst.key]
                            mode_inst.merge(tgt, tom)
                        else:
                            target[k].setdefault(typ, {}).update(tomerge[k][typ])
                    else:
                        raise ValueError("Unknown mode '%s'" % mode)
        else:
            # key doesn't exist, create key
            if mode == "type":
                target.setdefault(k, {}).update(tomerge[k])
            else:
                if mode_inst:
                    # running special mode, we just set the keys in target
                    target.setdefault(k, tomerge[k])
                else:
                    target.setdefault(k, {}).update(tomerge[k])
                # if we already have splitstr and we want to merge str, skip it
                # as splitstr > str
                if str in target and splitstr in target:
                    target.pop(str)

    return target


def inspect(struct, key=None, mapt=None, mode="type", level=0, logger=logging):
    """
    Explore struct and report types contained in it.

    Args:
        struct: is the data structure to explore
        mapt: if not None, will complete that type map with passed struct. This is useful
              when iterating over a dataset of similar data, trying to find a good type summary
              contained in that dataset.
        level: is for internal purposes, mostly debugging
        mode: see inspect_docs() documentation
    """

    mode_inst = get_mode_layer(mode)

    # init recording structure if none were passed
    if mapt is None:
        mapt = {}

    # if type(struct) == dict:    # TODO: remove this line
    if isinstance(struct, dict):
        # was this struct already explored before ? was it a list for that previous doc ?
        # then we have to pretend here it's also a list even if not, because we want to
        # report the list structure
        for k in struct:
            if mapt and list in mapt:  # and key == k:
                already_explored_as_list = True
            else:
                already_explored_as_list = False
            if False:  # already_explored_as_list:      # TODO: check this
                mapt[list].setdefault(k, {})
                typ = inspect(struct[k], key=k, mapt=mapt[list][k], mode=mode, level=level+1)
                mapt[list].update({k: typ})
            else:
                mapt.setdefault(k, {})
                typ = inspect(struct[k], key=k, mapt=mapt[k], mode=mode, level=level+1)

        if mode_inst:
            mapt.setdefault(mode_inst.key, copy.deepcopy(mode_inst.template[mode_inst.key]))
            mode_inst.report(1, mapt, struct)
    elif type(struct) == list:

        mapl = {}
        for e in struct:
            typ = inspect(e, key=key, mapt=mapl, mode=mode, level=level+1)
            mapl.update(typ)
        if mode_inst:
            # here we just report that one document had a list
            mapl.update(copy.deepcopy(mode_inst.template))
            mode_inst.report(struct, mapl)
        # if mapt exist, it means it's been explored previously but not as a list,
        # instead of mixing dict and list types, we want to normalize so we merge the previous
        # struct into that current list
        if mapt and list in mapt:
            mapt[list] = merge_record(mapt[list], mapl, mode)
        else:
            mapt.setdefault(list, {})
            mapt[list].update(mapl)
    # elif is_scalar(struct) or type(struct) == datetime:   # TODO: remove this line
    elif is_scalar(struct) or isinstance(struct, datetime):
        typ = type(struct)
        if mode == "type":
            mapt[typ] = {}
        elif mode == "mapping":
            # some type precedence processing...
            # splittable string ?
            if is_str(struct) and len(re.split(" +", struct.strip())) > 1:
                mapt[splitstr] = {}
            elif typ == bson.int64.Int64:
                mapt[int] = {}
            # we know struct is a scalar. NaN and Inf can't be indexed on ES,
            # need to catch those
            elif isinstance(struct, float) and math.isnan(struct):
                mapt[nan] = {}
            elif isinstance(struct, float) and math.isinf(struct):
                mapt[inf] = {}
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
            mapt.setdefault(typ, copy.deepcopy(mode_inst.template))
            mode_inst.report(struct, mapt[typ])
    else:
        raise TypeError("Can't analyze type %s (data was: %s)" % (type(struct), struct))

    return mapt

def merge_scalar_list(mapt, mode):
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
                        if typ not in mapt[list][e]:
                            mapt[list][e][typ] = tomerge[typ]
                        # Note: don't update [list]["_stats" (or other modes' key)], we keep the original stats
                        # that is, what's actually been inspected on the list, originally
                        # (and we can't really update those stats as scalar stats aren't relevant
                        # to a list context
                        elif typ == mode_inst.key:
                            mode_inst.merge(mapt[list][e][mode_inst.key], tomerge[mode_inst.key])
                        else:
                            mode_inst.merge(mapt[list][e][typ][mode_inst.key], tomerge[typ][mode_inst.key])
                elif mode == "mapping":
                    for typ in tomerge:
                        if typ is str and splitstr in mapt[list][e]:
                            # precedence splitstr > str, we keep splitstr and ignore str
                            continue
                        if typ not in mapt[list][e]:
                            # that field exist in the [list] but with a different type
                            # just merge the typ
                            mapt[list][e].update(tomerge)
                        # precedence splitstr > str
                        if splitstr is typ:
                            mapt[list][e].pop(str, None)
                            mapt[list][e].update(tomerge)
                else:
                    # assuming what's in [list] is enough, we just popped the value
                    # from mapt, that's enough
                    pass
        # explore further
        merge_scalar_list(mapt[list], mode)
    # elif type(mapt) == dict:   # TODO: remove this line
    elif isinstance(mapt, dict):
        for k in mapt:
            merge_scalar_list(mapt[k], mode)
    # elif type(mapt) == list:   # TODO: remove this line
    elif isinstance(mapt, list):
        for e in mapt:
            merge_scalar_list(e, mode)


def get_converters(modes, logger=logging):
    converters = []
    # should we actually run another mode and then convert the results ?
    if "jsonschema" in modes:
        from biothings.utils.jsonschema import generate_json_schema
        # first get schema with mode="type", then convert the results
        # note "type" can't also be specified as jsonschema will replace
        # the results in _map["type"] key
        converters.append({
            "output_mode": "jsonschema",
            "input_mode": "type",
            "func": generate_json_schema,
            "delete_input_mode": "type" not in modes
        })
        modes.remove("jsonschema")
        if "type" not in modes:
            modes.append("type")

    return converters, modes


def run_converters(_map, converters, logger=logging):
    # need to convert some results ?
    for converter in converters:
        logger.info("Finalizing result for mode '%s' using converter %s", converter["output_mode"], converter)
        converted = converter["func"](_map[converter["input_mode"]])
        _map[converter["output_mode"]] = converted
        if converter["delete_input_mode"]:
            _map.pop(converter["input_mode"])

def inspect_docs(docs, mode="type", clean=True, merge=False, logger=logging,
                 pre_mapping=False, limit=None, sample=None, metadata=True,
                 auto_convert=True):
    """Inspect docs and return a summary of its structure:

    Args:
        mode: possible values are:

            - "type": (default) explore documents and report strict data structure
            - "mapping": same as type but also perform test on data so guess best mapping
                       (eg. check if a string is splitable, etc...). Implies merge=True
            - "stats": explore documents and compute basic stats (count,min,max,sum)
            - "deepstats": same as stats but record values and also compute mean,stdev,median
                         (memory intensive...)
            - "jsonschema", same as "type" but returned a json-schema formatted result

            `mode` can also be a list of modes, eg. ["type","mapping"]. There's little
            overhead computing multiple types as most time is spent on actually getting the data.
        clean: don't delete recorded vqlues or temporary results
        merge: merge scalar into list when both exist (eg. {"val":..} and [{"val":...}]
        limit: can limit the inspection to the x first docs (None = no limit, inspects all)
        sample: in combination with limit, randomly extract a sample of 'limit' docs
                (so not necessarily the x first ones defined by limit). If random.random()
                is greater than sample, doc is inspected, otherwise it's skipped
        metadata: compute metadata on the result
        auto_convert: run converters automatically (converters are used to convert one mode's
                      output to another mode's output, eg. type to jsonschema)
    """

    # if type(mode) == str:    # TODO: remove this line
    if isinstance(mode, str):
        modes = [mode]
    else:
        modes = mode
    if auto_convert:
        converters, modes = get_converters(modes, logger=logger)
    _map = {}
    for m in modes:
        _map[m] = {}
    cnt = 0
    errors = set()
    t0 = time.time()
    innert0 = time.time()

    if sample is not None:
        assert limit, "Parameter 'sample' requires 'limit' to be defined"
        assert sample != 1, "Sample value 1 not allowed (no documents would be inspected)"
    if limit:
        limit = int(limit)
        logger.debug("Limiting inspection to the %s first documents", limit)
    for doc in docs:
        if sample is not None:
            if random.random() <= sample:
                continue
        for m in modes:
            try:
                inspect(doc, mapt=_map[m], mode=m)
            except Exception as e:
                logging.exception("Can't inspect document (_id: %s) because: %s\ndoc: %s", doc.get("_id"), e, pformat("dpc"))
                errors.add(str(e))
        cnt += 1
        if cnt % 10000 == 0:
            logger.info("%d documents processed [%s]", cnt, timesofar(innert0))
            innert0 = time.time()
        if limit and cnt > limit:
            logger.debug("done")
            break
    logger.info("Done [%s]", timesofar(t0))
    logger.info("Post-processing")

    # post-process, specific for each mode
    for m in modes:
        mode_inst = get_mode_layer(m)
        if mode_inst:
            mode_inst.post(_map[m], m, clean)

    if auto_convert:
        run_converters(_map, converters, logger=logger)

    merge = "mapping" in modes and True or merge
    if merge:
        merge_scalar_list(_map["mapping"], "mapping")
    if "mapping" in modes and pre_mapping is False:
        # directly generate ES mapping
        import biothings.utils.es as es
        try:
            _map["mapping"] = es.generate_es_mapping(_map["mapping"])
            if metadata:
                # compute some extra metadata
                _map = compute_metadata(_map, "mapping")
        except es.MappingError as e:
            prem = {"pre-mapping": _map["mapping"], "errors": e.args[1]}
            _map["mapping"] = prem
    elif errors:
        _map["errors"] = errors
    return _map

def compute_metadata(mapt, mode):
    if mode == "mapping":
        flat = flatten_doc(mapt["mapping"])
        # total fields: ES6 requires to overcome the default 1000 limit if needed
        mapt["__metadata__"] = {"total_fields": len(flat)}

    return mapt


def typify_inspect_doc(dmap):
    """
    dmap is an inspect which was converted to be stored in a database,
    namely actual python types were stringify to be storabled. This function
    does the oposite and restore back python types within the inspect doc
    """
    def typify(val):
        if type(val) != type and val.startswith("__type__:"):
            typ = val.replace("__type__:", "")
            # special cases
            if typ == "NoneType":
                return None
            elif typ == "Int64":  # bson's Int64
                return bson.int64.Int64
            else:
                return eval(val.replace("__type__:", ""))
        else:
            return val
    return dict_walk(dmap, typify)


def stringify_inspect_doc(dmap):
    def stringify(val):
        if type(val) == type:
            return "__type__:%s" % val.__name__  # prevent having dots in the field (not storable in mongo)
        else:
            return str(val)
    return dict_walk(dmap, stringify)
