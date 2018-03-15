<template>
    <div>
        <button class="ui newrelease  button" @click="newRelease">
            New release
        </button>
        <div class="ui feed"  v-if="releases">
            <div class="event" v-for="rel in releases">
                <index-release-event :release="rel" v-if="rel.index_name"></index-release-event>
                <diff-release-event :release="rel" :index_envs="index_envs" :build_config="build.build_config" v-if="rel.diff"></diff-release-event>

            </div>
        </div>
        <div v-else>
            No release found
        </div>

        <!-- create new release -->
        <div class="ui basic newrelease modal">
            <h3 class="ui icon">
                <i class="tag icon"></i>
                Create new release
            </h3>
            <div class="content">
                <div class="ui form">
                    <div class="ui centered grid">
                        <div class="eight wide column">

                            <label>Select the type of release</label>
                            <div>
                                <select class="ui dropdown" name="release_type" v-model="release_type">
                                    <option>incremental</option>
                                    <option>full</option>
                                </select>
                                <br>
                                <br>
                            </div>

                            <span v-if="release_type == 'incremental'">
                                <label>Select a build to compute incremental release (compared to this one)</label>
                                <div>
                                    <select class="ui fluid availbuilds dropdown" name="old_build" v-if="avail_builds.length">
                                        <option v-for="b in avail_builds">{{b}}</option>
                                    </select>
                                    <div class="ui red message" v-else>
                                        No build available to compute incremental data release
                                    </div>
                                    <br>
                                </div>
                                <div>
                                    <label>Select the type of diff files to generate</label>
                                    <select class="ui fluid difftypes dropdown" name="diff_type">
                                        <option v-for="dtyp in diff_types" :selected="dtyp == 'jsondiff-selfcontained'">{{dtyp}}</option>
                                    </select>
                                    <br>
                                </div>
                            </span>
                            <span v-if="release_type == 'full'">
                                <div>
                                    <label>Enter a name for the index (or leave it empty to have the same name as the build)</label>
                                    <input type="text" name="index_name" placeholder="Index name" autofocus>
                                    <br>
                                    <br>
                                </div>
                                <div>
                                    <label>Select an indexer environment to create the index on</label>
                                    <select class="ui fluid indexenvs dropdown" name="index_env">
                                        <option v-for="(info,env) in index_envs.env" :data-env="env">{{env}} <i>({{info.host}})</i></option>
                                    </select>
                                    <br>
                                </div>
                            </span>

                        </div>

                        <div class="eight wide column">

                            <div class="ui teal message">
                                <b>Note</b>: sources providing root documents, or <i>root sources</i>, are sources allowed to
                                create a new document in a build (merged data). If a root source is declared, data from other sources will only be merged <b>if</b>
                                documents previously exist with same IDs (documents coming from root sources). If not, data is discarded. Finally, if no root source
                                is declared, any data sources can generate a new document in the merged data.
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <div class="ui error message" v-if="errors.length">
                <ul class="ui list">
                    <li v-for="err in errors">{{err}}</li>
                </ul>
            </div>

            <div class="actions">
                <div class="ui red basic cancel inverted button">
                    <i class="remove icon"></i>
                    Cancel
                </div>
                <div class="ui green ok inverted button">
                    <i class="checkmark icon"></i>
                    OK
                </div>
            </div>
        </div>
    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import Vue from 'vue';
import IndexReleaseEvent from './IndexReleaseEvent.vue';
import DiffReleaseEvent from './DiffReleaseEvent.vue';

export default {
    name: 'build-releases',
    props: ['build'],
    mounted() {
        $(".ui.dropdown").dropdown();
    },
    created() {
        this.loadData();
    },
    beforeDestroy() {
        $('.ui.basic.newrelease.modal').remove();
    },
    components: { IndexReleaseEvent, DiffReleaseEvent},
    data () {
        return {
            errors : [],
            release_type : null,
            diff_types : [],
            avail_builds : [],
            index_envs : [],
        }
    },
    computed: {
        releases: function () {
            // sort index and diff releases by dates
            var _releases = [];
            if(this.build.index) {
                _releases = _releases.concat(Object.values(this.build.index));
            }
            if(this.build.diff)
                _releases = _releases.concat(Object.values(this.build.diff));
            _releases.sort(function(a,b) {
                var da = a.created_at && Date.parse(a.created_at)
                var db = b.created_at && Date.parse(b.created_at)
                console.log(`da ${da} db ${db}`);
                return db - da;
            });
            console.log(_releases);
            return _releases;
        }
    },
    methods: {
        displayError : function() {
        },
        newFullRelease : function() {
            var index_name = $(".ui.form input[name=index_name]").val();
            if(index_name == "")
                index_name = null;
            var index_env = $(".ui.form select[name=index_env] :selected").attr("data-env");
            axios.put(axios.defaults.baseURL + `/index`,{"indexer_env" : index_env, "target_name" : this.build._id, "index_name" : index_name})
            .then(response => {
                console.log(response.data.result)
                bus.$emit("reload_build_detailed");
                return response.data.result;
            })
            .catch(err => {
                console.log("Error creating index: ");
                console.log(err);
            })
        },
        newIncrementalRelease : function() {
            var old_build = $(".ui.form select[name=old_build]").val();
            var diff_type =  $(".ui.form select[name=diff_type]").val();
            if(!old_build)
                this.errors.push("Select a build to compute incremental data");
            if(!diff_type)
                this.errors.push("Select a diff type");
            axios.put(axios.defaults.baseURL + `/diff`,{"diff_type" : diff_type, "old" : old_build, "new" : this.build._id})
            .then(response => {
                console.log(response.data.result)
                bus.$emit("reload_build_detailed");
                return response.data.result;
            })
            .catch(err => {
                console.log("Error creating diff: ");
                console.log(err);
            })
        },
        newRelease : function() {
            var self = this;
            $('.ui.basic.newrelease.modal')
            .modal("setting", {
                detachable : false,
                onApprove: function () {
                    self.errors = [];
                    var release_type = $(".ui.form select[name=release_type]").val();
                    if(release_type == 'full') {
                        return self.newFullRelease();
                    } else if(release_type == 'incremental') {
                        return self.newIncrementalRelease();
                    } else {
                        console.log(`Unknown release type ${release_type}`);
                        return false;
                    }
                }
            })
            .modal("show");
        },
        loadData: function() {
            // diff types
            axios.get(axios.defaults.baseURL + '/diff_manager')
            .then(response => {
                console.log(response.data.result)
                this.diff_types = Object.keys(response.data.result).sort();
            })
            .catch(err => {
                console.log(err);
                console.log("Error loading differ information: " + err.data.error);
            });
            // index env
            axios.get(axios.defaults.baseURL + '/index_manager')
            .then(response => {
                this.index_envs = response.data.result;
            })
            .catch(err => {
                console.log(err);
                console.log("Error loading differ information: " + err.data.error);
            });
            // avail builds
            axios.get(axios.defaults.baseURL + `/builds?conf_name=${this.build.build_config._id}`)
            .then(response => {
                console.log(response.data.result)
                $(response.data.result).each((i,e) => {
                    console.log(`${e._id} ${this.build._id}`);
                    if(e._id != this.build._id)
                        this.avail_builds.push(e._id);
                });
            })
            .catch(err => {
                console.log(err);
                console.log("Error loading differ information: " + err.data.error);
            });
        }
    }
}
</script>

<style>
</style>
