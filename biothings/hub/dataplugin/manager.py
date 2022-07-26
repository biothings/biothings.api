import asyncio
import os
import subprocess

from biothings.utils.hub_db import get_data_plugin
import biothings.hub.dataload.dumper as dumper


class GitDataPlugin(dumper.GitDumper):

    # override to point to "data_plugin" collection instead of src_dump
    # so we don't mix data sources and plugins
    def prepare_src_dump(self):
        self.src_dump = get_data_plugin()
        self.src_doc = self.src_dump.find_one({'_id': self.src_name}) or {}

    # override to update code base to the newest commit instead if just fetch but not merge on a specific branch
    # so we don't mix data sources and plugins
    def _pull(self, localdir, commit):
        # fetch+merge
        self.logger.info("git pull data (commit %s) into '%s'" % (commit, localdir))
        old = os.path.abspath(os.curdir)
        try:
            os.chdir(localdir)
            # discard changes, we don't want to activate a conflit resolution session...
            cmd = ["git", "reset", "--hard", "HEAD"]
            subprocess.check_call(cmd)
            # then fetch latest code (local repo, not applied to code base yet)
            cmd = ["git", "fetch", "--all"]
            subprocess.check_call(cmd)
            if commit != "HEAD":
                # first get the latest code from repo
                # (if a newly created branch is avail in remote, we can't check it out)
                self.logger.info("git checkout to commit %s" % commit)
                cmd = ["git", "checkout", commit]
                subprocess.check_call(cmd)
            else:
                # if we were on a detached branch (due to specific commit checkout)
                # we need to make sure to go back to master (re-attach)
                # TODO: figure out why it was originally using the class
                #  variable exclusively. Changed to prefer instance varaibles.
                branch = self._get_default_branch()
                cmd = ["git", "checkout", branch]
                subprocess.check_call(cmd)
            # then merge
            cmd = ["git", "merge"]
            subprocess.check_call(cmd)
            # and then get the commit hash
            out = subprocess.check_output(["git", "rev-parse", "HEAD"])
            self.release = commit + " (%s)" % out.decode().strip()[:7]
        finally:
            os.chdir(old)
        pass


class ManualDataPlugin(dumper.ManualDumper):

    # override to point to "data_plugin" collection instead of src_dump
    # so we don't mix data sources and plugins
    def prepare_src_dump(self):
        self.src_dump = get_data_plugin()
        self.src_doc = self.src_dump.find_one({'_id': self.src_name}) or {}

    async def dump(self, *args, **kwargs):
        await super(ManualDataPlugin, self).dump(
            path="",  # it's the version is original method implemention
            # but no version here available
            release="", *args, **kwargs)


class DataPluginManager(dumper.DumperManager):

    def load(self, plugin_name, *args, **kwargs):
        return super(DataPluginManager, self).dump_src(plugin_name, *args, **kwargs)
