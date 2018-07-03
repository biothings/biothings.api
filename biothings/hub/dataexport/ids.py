import os, subprocess, shutil

from biothings.utils.hub_db import get_src_dump, get_src_build
from biothings.utils.mongo import get_src_db, id_feeder, get_target_db, \
                                  get_cache_filename
from biothings.utils.common import anyfile

from biothings import config as btconfig
logging = btconfig.logger


def export_ids(col_name):
    """
    Export all _ids from collection named col_name.
    If col_name refers to a build where a cold_collection is defined,
    will also extract _ids and sort/uniq them to have the full list of _ids
    of the actual merged (cold+hot) collection
    Output file is stored in DATA_EXPORT_FOLDER/ids,
    defaulting to <DATA_ARCHIVE_ROOT>/export/ids. Output filename is
    returned as the end, if successful.
    """
    # prepare output directory
    DATA_EXPORT_FOLDER = getattr(btconfig,"DATA_EXPORT_FOLDER",None)
    if not DATA_EXPORT_FOLDER:
        DATA_EXPORT_FOLDER = os.path.join(btconfig.DATA_ARCHIVE_ROOT,"export")
    ids_export_folder = os.path.join(DATA_EXPORT_FOLDER,"ids")
    if not os.path.exists(ids_export_folder):
        logging.debug("Creating export/ids folder: %s" % ids_export_folder)
        os.makedirs(ids_export_folder)
    build = get_src_build().find_one({"_id":col_name})
    cold = None
    if build:
        col = get_target_db()[col_name]
        if build.get("build_config",{}).get("cold_collection"):
            cold_name = build["build_config"]["cold_collection"]
            cold = get_target_db()[cold_name]
            logging.info("Found a cold collection '%s' associated to '%s'" % (cold_name,col_name))
    else:
        # it's a src
        col = get_src_db()[col_name]
    
    # first iterate over all _ids. This will potentially update underlying _id cache it's not valid anymore,
    # so we're sure to work with latest data. If cache is valid, this will be pretty fast
    logging.info("Screening _ids in collection '%s'" % col.name)
    for _id in id_feeder(col,validate_only=True):
        pass
    # now accessing cache
    col_ids_cache = get_cache_filename(col.name)
    assert os.path.exists(col_ids_cache)
    logging.info("Now using cache file %s" % col_ids_cache)
    if cold:
        logging.info("Screening _ids in cold collection '%s'" % cold.name)
        for _id in id_feeder(cold,validate_only=True):
            pass
        # now accessing cache
        cold_ids_cache = get_cache_filename(cold.name)
        assert os.path.exists(cold_ids_cache)
        logging.info("Now using cache file %s" % cold_ids_cache)
    outfn = os.path.join(ids_export_folder,"%s_ids.xz" % col_name)
    # NOTE: can't use anyfile to open cache files and send _id through pipes
    # because it would load _id in memory (unless using hacks) so use cat (and
    # existing uncompressing ones, like gzcat/xzcat/...) to fully run the pipe
    # on the shell
    if cold:
        fout = anyfile(outfn,"wb")
        colext = os.path.splitext(col_ids_cache)[1]
        coldext = os.path.splitext(cold_ids_cache)[1]
        assert colext == coldext, "Hot and Cold _id cache are compressed differently (%s and %s), it should be the same" % (coldext,coldext)
        comp = colext.replace(".","")
        supportedcomps = ["xz","gz",""] # no compression allowed as well
        assert comp in supportedcomps, "Compression '%s' isn't supported (%s)" % (comp,supportedcomps)
        # IDs sent to pipe's input (sort) then compress it (xz)
        pcat = subprocess.Popen(["%scat" % comp, col_ids_cache, cold_ids_cache],stdout=subprocess.PIPE)
        psort = subprocess.Popen(["sort","-u"],stdin=pcat.stdout,stdout=subprocess.PIPE,universal_newlines=True)
        pcat.stdout.close() # will raise end of pipe error when finished
        if comp:
            pcomp = subprocess.Popen(["xz","-c"],stdin=psort.stdout,stdout=fout)
        else:
            # just print stdin to stdout
            pcomp = subprocess.Popen(["tee"],stdin=psort.stdout,stdout=fout)
        psort.stdout.close()
        try:
            (out,err) = pcomp.communicate() # run the pipe! (blocking)
            if err:
                raise Exception(err)
        except Exception as e:
            logging.error("Error while running pipe to export _ids: %s" % e)
            # make sure to clean empty or half processed files
            try:
                os.unlink(outfn)
            finally:
                pass
            raise
    else:
        logging.info("Copying cache _id file")
        try:
            shutil.copyfile(col_ids_cache,outfn)
        except Exception as e:
            logging.error("Error while exporting _ids: %s" % e)
            # make sure to clean empty or half processed files
            try:
                os.unlink(outfn)
            finally:
                pass
            raise

    logging.info("Done exporting _ids to '%s'" % outfn)
    return outfn


        



