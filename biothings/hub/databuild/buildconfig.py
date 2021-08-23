"""
    A build config contains autobuild configs and other information.

    TODO: not all features already supported in the code
    
    For exmaple:
    {
        "_id": "mynews",
        "name": "mynews",
        "doc_type": "news",
        "sources": ["mynews"],
        "root": ["mynews"],
        "builder_class": "biothings.hub.databuild.builder.DataBuilder",
        "autobuild": { ... },
        "autopublish": { ... },
        "build_version": "%Y%m%d%H%M"
    }
    
    Autobuild:
    - build
    - diff/snapshot

    Autopublish:
    - release note
    - publish

    Autorelease:
    - release

"""

try:
    from functools import singledispatchmethod
except ImportError:
    from singledispatchmethod import singledispatchmethod

import logging

logger = logging.getLogger(__name__)


class AutoBuildConfigError(Exception):
    pass

class AutoBuildConfig:
    """
    Parse automation configurations after each steps following 'build'.

    Example:
    {
        "autobuild": {
            "schedule": "0 8 * * 7", // Make a build every 08:00 on Sunday.
            "type": "diff",          // Auto create a "diff" w/previous version.
                                     // The other option is "snapshot".
            "env": "local",          // ES env to create an index and snapshot, 
                                     // not required when type above is diff.
                                     // Setting the env also implies auto snapshot.
                                     // It could be in addition to auto diff.
                                     // Also accept (indexer_env, snapshot_env).
        },
        "autopublish": {
            "type": "snapshot",      // Auto publish new snapshots for new builds.
                                     // The type field can also be 'diff'.
            "env": "prod",           // The release environment to publish snapshot.
                                     // Or the release environment to publish diff.
                                     // This field is required for either type.
            "note": True             // If we should publish with a release note
                                     // TODO not implemented yet
        },
        "autorelease": {
            "schedule": "0 0 * * 1", // Make a release every Monday at midnight
                                     // (if there's a new published version.)
            "type": "full",          // Only auto install full releases.
                                     // The release type can also be 'incremental'.
        }
    }
    The terms below are used interchangeably.
    """
    BUILD_TYPES = ('diff', 'snapshot')
    RELEASE_TYPES = ('incremental', 'full')
    RELEASE_TO_BUILD = dict(zip(RELEASE_TYPES, BUILD_TYPES))

    def __init__(self, confdict):

        try:
            self.auto_build = confdict.get('autobuild', {})
            self.auto_build['type'] = self._types_as_set(
                self.auto_build.get('type'))

            self.auto_publish = confdict.get('autopublish', {})
            self.auto_publish['type'] = self._types_as_set(
                self.auto_publish.get('type'))

            self.auto_release = confdict.get('autorelease', {})
            self.auto_release['type'] = self._types_as_set(
                self.auto_release.get('type'))

        except AttributeError as exc:
            raise AutoBuildConfigError(
                "Autobuild config entries should be dicts."
            ) from exc

    @classmethod
    def _standardize_type(cls, type_str):
        """
        Ensure using build type 'diff' and 'snapshot'.
        Translate release type names accordingly.
        """
        if type_str in cls.BUILD_TYPES:
            return type_str
        elif type_str in cls.RELEASE_TYPES:
            return cls.RELEASE_TO_BUILD[type_str]
        raise AutoBuildConfigError(
            f"Unsupported type: {type_str}")

    @singledispatchmethod
    @classmethod
    def _types_as_set(cls, type_val):
        """
        The field "type", if specified, can be:
        "diff" or "incremental", "snapshot" or "full",
        or a list of the values mentioned above.
        """
        raise AutoBuildConfigError(
            "Auto build type must be a list or string."
        )

    @_types_as_set.register(type(None))
    @classmethod
    def _(cls, _):
        return set()

    @_types_as_set.register(str)
    @classmethod
    def _(cls, type_str):
        return set((cls._standardize_type(type_str), ))

    @_types_as_set.register(list)
    @classmethod
    def _(cls, type_list):
        return set(cls._standardize_type(_type) for _type in type_list)

    def export(self):
        autoconf = {
            "autobuild": dict(self.auto_build),
            "autopublish": dict(self.auto_publish),
            "autorelease": dict(self.auto_release)
        }
        autoconf["autobuild"]["type"] = list(
            autoconf["autobuild"]["type"])
        autoconf["autopublish"]["type"] = list(
            autoconf["autopublish"]["type"])
        autoconf["autorelease"]["type"] = list(
            autoconf["autorelease"]["type"])
        return autoconf

    # after build

    def should_diff_new_build(self):

        if 'diff' in self.auto_build['type']:
            return True

        if not self.auto_build['type'] and \
                self.auto_build.get('schedule'):
            return True  # implicit

        # other implied relationships can be specified
        # --------------------------------------------
        # if 'diff' in self.auto_publish['type']:
        #     return True
        # if 'diff' in self.auto_release['type']:
        #     return True
        # --------------------------------------------
        # currently not sure if this is a good idea

        return False

    def should_snapshot_new_build(self):

        if 'snapshot' in self.auto_build['type'] \
                or self.auto_build.get('env'):
            # env indicates snapshot, diff doesn't need it
            if not self.auto_build.get('env'):
                logger.error("Auto snapshot env not set.")
            return True

        return False

    # after diff/snapshot

    def should_publish_new_diff(self):
        return 'diff' in self.auto_publish['type']

    def should_publish_new_snapshot(self):
        return 'snapshot' in self.auto_publish['type']

    # after publish

    def should_install_new_diff(self):
        return 'diff' in self.auto_release['type']

    def should_install_new_snapshot(self):
        return 'snapshot' in self.auto_release['type']

    def should_install_new_release(self):
        """
        Install the latest version regardless of update type/path.
        """
        if len(self.auto_release['type']) == 2:
            return True

        if not self.auto_release['type'] and \
                self.auto_release.get('schedule'):
            return True

        return False

def test():

    mygene = AutoBuildConfig({
        "autobuild": {
            "schedule": "0 8 * * 7"
        }
    })
    print(mygene.export())
    assert mygene.should_diff_new_build()
    assert not mygene.should_snapshot_new_build()
    assert not mygene.should_publish_new_diff()
    assert not mygene.should_publish_new_snapshot()
    assert not mygene.should_install_new_diff()
    assert not mygene.should_install_new_snapshot()
    assert not mygene.should_install_new_release()

    outbreak = AutoBuildConfig({
        "autobuild": {
            "schedule": "0 8 * * *",
            "type": "snapshot",
            "env": "auto"  # snapshot location, indexing ES.
        },
        "autopublish": {
            "type": "snapshot",
            "env": "auto"
        },
        "autorelease": {
            "schedule": "0 10 * * *"
        }
    })
    print(outbreak.export())
    assert not outbreak.should_diff_new_build()
    assert outbreak.should_snapshot_new_build()
    assert not outbreak.should_publish_new_diff()
    assert outbreak.should_publish_new_snapshot()
    assert not outbreak.should_install_new_diff()
    assert not outbreak.should_install_new_snapshot()
    assert outbreak.should_install_new_release()


if __name__ == '__main__':
    test()
