from biothings.utils.hub_db import get_src_build


def set_pending_to_publish(col_name):
    src_build = get_src_build()
    src_build.update({"_id": col_name}, {"$addToSet": {"pending": "publish"}})


def set_pending_to_release_note(col_name):
    src_build = get_src_build()
    src_build.update({"_id": col_name}, {"$addToSet": {"pending": "release_note"}})
