"""
This module contains util functions may be shared by both BioThings data-hub and web components.
In general, do not include utils depending on any third-party modules.
"""
import math, statistics
import time
import logging
from pprint import pprint, pformat

from .common import timesofar, is_scalar, is_float, is_str, is_int


def inspect(struct,key=None,mapt=None,mode="type",level=0,logger=logging):
    """
    Explore struct and report types contained in it.
    - struct: is the data structure to explore
    - mapt: if not None, will complete that type map with passed struct. This is usefull
      when iterating over a dataset of similar data, trying to find a good type summary 
      contained in that dataset.
    - (level: is for internal purposes, mostly debugging)
    - mode: "type", "stats", "deepstats"
    """

    def report(val,drep):
        drep["_stats"] = flatten_stats(drep["_stats"])
        drep["_stats"]["_count"] += 1
        drep["_stats"]["_sum"] += val
        if val < drep["_stats"]["_min"]:
            drep["_stats"]["_min"] = val
        if val > drep["_stats"]["_max"]:
            drep["_stats"]["_max"] = val
        if mode== "deepstats":
            # just keep track of vals for now, stats are computed at the end
            drep["_stats"]["__vals"].append(val)

    def sumiflist(val):
        if type(val) == list:
            return sum(val)
        else:
            return val
    def maxminiflist(val,func):
        if type(val) == list:
            return func(val)
        else:
            return val

    def flatten_stats(stats):
        # after merge_struct, stats can be merged together as list (merge_struct
        # is only about data structures). Re-adjust here considering there could lists
        # that need to be sum'ed and min/max to be dealt with
        stats["_count"] = sumiflist(stats["_count"])
        stats["_sum"] = sumiflist(stats["_sum"])
        stats["_max"] = maxminiflist(stats["_max"],max)
        stats["_min"] = maxminiflist(stats["_min"],min)
        return stats

    def merge_stats(target_stats, tomerge_stats):
        target_stats = flatten_stats(target_stats)
        tomerge_stats = flatten_stats(tomerge_stats)
        # sum the counts and the sums
        target_stats["_count"] = target_stats["_count"] + tomerge_stats["_count"]
        target_stats["_sum"] = target_stats["_sum"] + tomerge_stats["_sum"]
        # adjust min and max
        if tomerge_stats["_max"] > target_stats["_max"]:
            target_stats["_max"] = tomerge_stats["_max"]
        if tomerge_stats["_min"] < target_stats["_min"]:
            target_stats["_min"] = tomerge_stats["_min"]
        # extend values
        target_stats["__vals"].extend(tomerge_stats["__vals"])

    def merge_record(target,tomerge,mode):
        for k in tomerge:
            if k in target:
                if k == "_stats":
                    tgt_stats = target["_stats"]
                    tom_stats = tomerge["_stats"]
                    merge_stats(tgt_stats,tom_stats)
                    continue
                for typ in tomerge[k]:
                    if mode == "type":
                        # in mode=type, we just keep track on types,
                        # we already did it so nothing to do
                        continue
                    else:
                        if typ in target[k]:
                            # same key, same type, need to merge stats
                            if not "_stats" in tomerge[k][typ]:
                                # we try to merge record at a too higher level, need to merge deeper
                                target[k] = merge_record(target[k],tomerge[k],mode)
                                continue
                            tgt_stats = target[k][typ]["_stats"]
                            tom_stats = tomerge[k][typ]["_stats"]
                            merge_stats(tgt_stats,tom_stats)
                        else:
                            # key exists but with a different type, create new type
                            if mode == "type":
                                target[k].update(tomerge[k])
                            else:
                                target[k].setdefault(typ,{}).update(tomerge[k][typ])
            else:
                # key doesn't exist, create key
                if mode == "type":
                    target.setdefault(k,{}).update(tomerge)
                else:
                    target.setdefault(k,{}).update(tomerge[k])

        return target

    def merge_dict(dtarget,d):
        for k in d:
            if k in dtarget:
                dtarget[k] = merge_dict(dtarget[k],d[k])
            else:
                dtarget[k] = d[k]
        return dtarget

    stats_tpl = {"_stats" : {"_min":math.inf,"_max":-math.inf,"_count":0,"_sum":0,"__vals":[]}}

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
            if already_explored_as_list:
                mapt[list].setdefault(k,{})
                typ = inspect(struct[k],key=k,mapt=mapt[list][k],mode=mode,level=level+1)
                mapt[list].update({k:typ})
            else:
                mapt.setdefault(k,{})
                typ = inspect(struct[k],key=k,mapt=mapt[k],mode=mode,level=level+1)
    elif type(struct) == list:
        mapl = {}
        for e in struct:
            typ = inspect(e,key=key,mapt=mapl,mode=mode,level=level+1)
            mapl.update(typ)
        if mode != "type":
            mapl.update(stats_tpl)
            report(len(struct),mapl)
        # if mapt exist, it means it's been explored previously but not a list,
        # instead of mixing dict and list types, we want to normalize so we merge the previous
        # struct into that current list
        if mapt and list in mapt:
            mapt[list] = merge_record(mapt[list],mapl,mode)
        else:
            topop = []
            if mapt:
                topop = [k for k in list(mapt.keys()) if k != list]
                merge_dict(mapl,mapt)
            mapt.setdefault(list,{})
            mapt[list].update(mapl)
            for k in topop:
                mapt.pop(k,None)
    elif is_scalar(struct):
        typ = type(struct)
        if mode == "type":
            mapt[typ] = {}
        else:
            mapt.setdefault(typ,stats_tpl)
            if is_str(struct):
                report(len(struct),mapt[typ])
            elif is_int(struct) or is_float(struct):
                report(struct,mapt[typ])
            elif type(struct) == bool:
                report(struct,mapt[typ])
    else:
        raise TypeError("Can't analyze type %s" % type(struct))

    return mapt

def inspect_docs(docs,mode="type",clean=True,logger=logging):

    def post(mapt, mode,clean):
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
                    post(mapt[k],mode,clean)
        elif type(mapt) == list:
            for e in mapt:
                post(e,mode,clean)

    mapt = {}
    cnt = 0
    t0 = time.time()
    innert0 = time.time()
    for doc in docs:
        inspect(doc,mapt=mapt,mode=mode)
        cnt += 1
        if cnt % 10000 == 0:
            logger.info("%d documents processed [%s]" % (cnt,timesofar(innert0)))
            innert0 = time.time()
    logger.info("Done [%s]" % timesofar(t0))
    logger.info("Post-processing (stats)")
    post(mapt,mode,clean)
    return mapt


if __name__ == "__main__":
    d1 = {"id" : "124",'lofd': [{"val":34.3},{"ul":"bla"}],"d":{"start":134,"end":5543}}
    d2 = {"id" : "5",'lofd': {"oula":"mak","val":34},"d":{"start":134,"end":5543}}
    d3 = {"id" : "890",'lofd': [{"val":34}],"d":{"start":134,"end":5543}}

    # merge either ways in the same
    m12 = inspect_docs([d1,d2])
    m21 = inspect_docs([d2,d1])
    #if undordered list, then:
    assert m21 == m12, "\nm21=%s\n!=\nm12=%s" % (pformat(m21),pformat(m12))
    # val can be an int and a float
    m1 = inspect_docs([{"val":34},{"val":1.2}]) 
    # set: types can be in any order
    assert set(m1["val"]) ==  {int,float}
    # even if val is in a list
    m2 = inspect_docs([{"val":34},[{"val":1.2}]])
    # val merged as list in the end
    assert set(m2[list]["val"]) ==  {int,float}
    assert set(m1["val"]) == set(m2[list]["val"])

    # stats
    m = {}
    inspect(d1,mapt=m,mode="stats")
    # some simple check
    assert set(m["id"].keys()) == {str}
    assert m["id"][str]["_stats"]["_count"] == 1
    assert m["id"][str]["_stats"]["_max"] == 3
    assert m["id"][str]["_stats"]["_min"] == 3
    assert m["id"][str]["_stats"]["_sum"] == 3
    assert m["lofd"].keys() == {list}
    # list's stats
    assert m["lofd"][list]["_stats"]["_count"] == 1
    assert m["lofd"][list]["_stats"]["_max"] == 2
    assert m["lofd"][list]["_stats"]["_min"] == 2
    assert m["lofd"][list]["_stats"]["_sum"] == 2
    # one list's elem stats
    assert m["lofd"][list]["val"][float]["_stats"]["_count"] == 1
    assert m["lofd"][list]["val"][float]["_stats"]["_max"] == 34.3
    assert m["lofd"][list]["val"][float]["_stats"]["_min"] == 34.3
    assert m["lofd"][list]["val"][float]["_stats"]["_sum"] == 34.3
    # again
    inspect(d1,mapt=m,mode="stats")
    assert m["id"][str]["_stats"]["_count"] == 2
    assert m["id"][str]["_stats"]["_max"] == 3
    assert m["id"][str]["_stats"]["_min"] == 3
    assert m["id"][str]["_stats"]["_sum"] == 6
    assert m["lofd"][list]["_stats"]["_count"] == 2
    assert m["lofd"][list]["_stats"]["_max"] == 2
    assert m["lofd"][list]["_stats"]["_min"] == 2
    assert m["lofd"][list]["_stats"]["_sum"] == 4
    assert m["lofd"][list]["val"][float]["_stats"]["_count"] == 2
    assert m["lofd"][list]["val"][float]["_stats"]["_max"] == 34.3
    assert m["lofd"][list]["val"][float]["_stats"]["_min"] == 34.3
    assert m["lofd"][list]["val"][float]["_stats"]["_sum"] == 68.6
    # mix with d2
    inspect(d2,mapt=m,mode="stats")
    assert m["id"][str]["_stats"]["_count"] == 3
    assert m["id"][str]["_stats"]["_max"] == 3
    assert m["id"][str]["_stats"]["_min"] == 1 # new min
    assert m["id"][str]["_stats"]["_sum"] == 7
    assert m["lofd"][list]["_stats"]["_count"] == 2 # not incremented as in d2 it's not a list
    assert m["lofd"][list]["_stats"]["_max"] == 2
    assert m["lofd"][list]["_stats"]["_min"] == 2
    assert m["lofd"][list]["_stats"]["_sum"] == 4
    # now float & int
    assert m["lofd"][list]["val"][float]["_stats"]["_count"] == 2
    assert m["lofd"][list]["val"][float]["_stats"]["_max"] == 34.3
    assert m["lofd"][list]["val"][float]["_stats"]["_min"] == 34.3
    assert m["lofd"][list]["val"][float]["_stats"]["_sum"] == 68.6
    assert m["lofd"][list]["val"][int]["_stats"]["_count"] == 1
    assert m["lofd"][list]["val"][int]["_stats"]["_max"] == 34
    assert m["lofd"][list]["val"][int]["_stats"]["_min"] == 34
    assert m["lofd"][list]["val"][int]["_stats"]["_sum"] == 34
    # d2 again
    inspect(d2,mapt=m,mode="stats")
    assert m["id"][str]["_stats"]["_count"] == 4
    assert m["id"][str]["_stats"]["_max"] == 3
    assert m["id"][str]["_stats"]["_min"] == 1
    assert m["id"][str]["_stats"]["_sum"] == 8
    assert m["lofd"][list]["_stats"]["_count"] == 2
    assert m["lofd"][list]["_stats"]["_max"] == 2
    assert m["lofd"][list]["_stats"]["_min"] == 2
    assert m["lofd"][list]["_stats"]["_sum"] == 4
    assert m["lofd"][list]["val"][float]["_stats"]["_count"] == 2
    assert m["lofd"][list]["val"][float]["_stats"]["_max"] == 34.3
    assert m["lofd"][list]["val"][float]["_stats"]["_min"] == 34.3
    assert m["lofd"][list]["val"][float]["_stats"]["_sum"] == 68.6
    assert m["lofd"][list]["val"][int]["_stats"]["_count"] == 2
    assert m["lofd"][list]["val"][int]["_stats"]["_max"] == 34
    assert m["lofd"][list]["val"][int]["_stats"]["_min"] == 34
    assert m["lofd"][list]["val"][int]["_stats"]["_sum"] == 68


    # all counts should be 10
    m = inspect_docs([d1] * 10,mode="stats") 
    assert m["d"]["end"][int]["_stats"]["_count"] == 10
    assert m["d"]["start"][int]["_stats"]["_count"] == 10
    assert m["id"][str]["_stats"]["_count"] == 10
    assert m["lofd"][list]["_stats"]["_count"] == 10
    assert m["lofd"][list]["ul"][str]["_stats"]["_count"] == 10
    assert m["lofd"][list]["val"][float]["_stats"]["_count"] == 10

    ## test merge
    #m = {}
    #inspect({"A":[{"a":34}]},mode="stats",mapt=m)
    ## same key but different type
    #inspect({"A":[{"a":"va1"}]},mode="stats",mapt=m)
    ## same key same type (stats merged)
    #inspect({"A":[{"a":"val2"}]},mode="stats",mapt=m)
    ## new key
    #inspect({"A":[{"a":"val2","b":"new"}]},mode="stats",mapt=m)

    ## deeply nested struct
    #m = {}
    #inspect({"A":[{"B":{"C":1}},{"b":{"c":1}}]},mode="stats",mapt=m)
    #inspect({"A":[{"B":{"C":4}},{"b":{"c":"oula"}}]},mode="stats",mapt=m)
