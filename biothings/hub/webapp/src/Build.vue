<template>
    <div class="ui fluid card">
        <div class="content">
            <div :class="['ui',color ? color : 'grey', 'tiny', '', 'label','conftag']">{{build.build_config.name}}</div>

            <i class="archived right floated archive icon"
                v-if="build.archived"></i>
            <span v-else>
            <!-- in progress -->
            <i class="right floated cube icon pulsing"
                v-if="build.status == 'building'"></i>
            <i class="right floated unhide icon pulsing"
                v-if="build.status == 'inspecting'"></i>
            <i class="right floated exchange icon pulsing"
                v-if="build.status == 'diffing'"></i>
            <i class="right floated bookmark icon pulsing"
                v-if="build.status == 'indexing'"></i>
            </span>

            <!-- error -->
            <div class="ui"
                v-bind:data-tooltip="displayError()">
                <i class="right floated red alarm icon pulsing"
                    v-if="build.status == 'failed'"></i>
            </div>

            <div class="left aligned header" v-model="build">
                <router-link :to="{name:'build', params: {_id: build._id, color:color}}"><a>{{ build.target_name }}</a></router-link>
            </div>
            <div class="left aligned description">
                <div>
                    <i class="file outline icon"></i>
                    {{ build.count | currency('',0) }} document{{ build.count &gt; 1 ? "s" : "" }}
                    <span class="right floated category" v-model="build" v-if="build._meta">{{ build._meta.build_version }}</span>
                </div>
            </div>
            <div class="meta">
                <span class="left floated category" v-model="build" v-if="build.jobs">Build time: {{ build.jobs | build_time | timesofar }}</span>
                <span class="right floated time" v-model="build">Built {{ build.started_at | moment("from","now") }}</span>
            </div>

            <div class="ui clearing divider"></div>

            <div class="left aligned description">
                <p>
                    <div class="ui top attached pointing secondary menu">
                        <a class="item active" data-tab="sources">Sources</a>
                        <a class="item" data-tab="stats">Stats</a>
                        <a class="item" data-tab="logs">Logs</a>
                    </div>
                    <div class="ui bottom attached tab segment active" data-tab="sources">
                        <build-sources v-bind:build="build"></build-sources>
                    </div>

                    <div class="ui bottom attached tab segment" data-tab="stats">
                        <build-stats v-bind:build="build"></build-stats>
                    </div>

                    <div class="ui bottom attached tab segment" data-tab="logs">
                        <build-logs v-bind:build="build"></build-logs>
                    </div>
                </p>
            </div>
        </div>

        <div class="extra content">
            <div class="ui icon buttons left floated mini">
                <button class="ui button" :data-build_id="build._id" v-on:click="inspect">
                    <i class="unhide icon" :data-build_id="build._id"></i>
                </button>
            </div>
            <div class="ui icon buttons right floated mini">
                <button class="ui button">
                    <i class="trash icon" @click="deleteBuild()"></i>
                </button>
            </div>
            <div class="ui icon buttons right floated mini" v-if="!build.archived">
                <button class="ui button">
                    <i :class="[build.archived ? 'archived' : '', 'archive icon']" @click="archiveBuild()"></i>
                </button>
            </div>
        </div>

        <div class="ui basic deletebuild modal" :id="build._id">
            <div class="ui icon header">
                <i class="remove icon"></i>
                Delete build
            </div>
            <div class="content">
                <p>Are you sure you want to delete build <b>{{build.target_name}}</b> ?</p>
                <p>All merged data and associated metadata will be deleted.</p>
                <div class="ui warning message">
                    Note: this operation is <b>*not* reversible</b> and should be used to completely delete all data and information
                    related to this build (eg. the build was an error...)
                </div>
            </div>
            <div class="actions">
                <div class="ui red basic cancel inverted button">
                    <i class="remove icon"></i>
                    No
                </div>
                <div class="ui green ok inverted button">
                    <i class="checkmark icon"></i>
                    Yes
                </div>
            </div>
        </div>

        <div class="ui basic archivebuild modal" :id="build._id">
            <div class="ui icon header">
                <i class="archive icon"></i>
                Archive build
            </div>
            <div class="content">
                <p>Are you sure you want to archive build <b>{{build.target_name}}</b> ?</p>
                <p>Merged data will be deleted but metadata will be kept (merged source informations, logs, releases, etc...)</p>
                <div class="ui warning message">
                    Note: this operation is <b>*not* reversible</b> and should be used only for outdated, past builds no longer relevant nor used.
                </div>
            </div>
            <div class="actions">
                <div class="ui red basic cancel inverted button">
                    <i class="remove icon"></i>
                    No
                </div>
                <div class="ui green ok inverted button">
                    <i class="checkmark icon"></i>
                    Yes
                </div>
            </div>
        </div>

        <inspect-form v-bind:_id="build._id">
        </inspect-form>

    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import Vue from 'vue';
import BaseBuild from './BaseBuild.vue'
import InspectForm from './InspectForm.vue'
import BuildLogs from './BuildLogs.vue'
import BuildStats from './BuildStats.vue'
import BuildSources from './BuildSources.vue'
import Loader from './Loader.vue'

function build_time(jobs) {
    return jobs.map((q)=>q.time_in_s).reduce(
            function(total, q) {
                return total + q
            }, 0);
};
Vue.filter('build_time',build_time);

export default {
    name: 'build',
    props: ['pbuild','color'],
    mounted() {
        $('.menu .item')
        .tab()
        ;
    },
    created() {
        bus.$on('build_updated',this.onBuildChanged);
    },
    beforeDestroy() {
        bus.$off('build_updated',this.onBuildChanged);
        $(`#${this.build._id}.ui.basic.deletebuild.modal`).remove();
        $(`#${this.build._id}.ui.basic.archivebuild.modal`).remove();
    },
    data() {
        return {
            // this object is set by API call, whereas 'pbuild' prop
            // is set by the parent
            build_from_api: null,
        }
    },
    mixins: [ BaseBuild, Loader, ],
    components: { InspectForm, BuildLogs, BuildStats, BuildSources, },
    computed: {
        build: function () {
            // select build from API call preferably
            return this.build_from_api || this.pbuild;
        },
    },
    methods: {
        displayError : function() {
            var errs = [];
            if (this.build.jobs) {
                var last = this.build.jobs[this.build.jobs.length-1];
                if(last && last.err)
                    errs.push(`Step '${last.step}' failed: ${last.err}`);
            }
            if(errs.length == 0) {
                // couldn't find an error, weird...
                errs.push("Unknown error...");
            }
            return errs.join("<br>");
        },
        deleteBuild : function() {
            var self = this;
            $(`#${self.build._id}.ui.basic.deletebuild.modal`)
            .modal("setting", {
                onApprove: function () {
                    axios.delete(axios.defaults.baseURL + `/build/${self.build._id}`)
                    .then(response => {
                        console.log(response.data.result)
                        return true;
                    })
                    .catch(err => {
                        console.log(err);
                        console.log("Error deleting build: " + err.data.error);
                    })
                }
            })
            .modal("show");
        },
        archiveBuild : function() {
            var self = this;
            $(`#${self.build._id}.ui.basic.archivebuild.modal`)
            .modal("setting", {
                onApprove: function () {
                    self.loading();
                    axios.post(axios.defaults.baseURL + `/build/${self.build._id}/archive`)
                    .then(response => {
                        console.log(response.data.result)
                        self.loaded();
                        return true;
                    })
                    .catch(err => {
                        console.log(err);
                        self.loaderror(err);
                    })
                }
            })
            .modal("show");
        },
        onBuildChanged: function(_id=null, op=null) {
            // this method acts as a dispatcher, reacting to change_build events, filtering
            // them for the proper build
            // _id null: event containing change about a build but we don't know which one
            // (it should be captured by build-grid component
            if(_id == null || this.build._id != _id) {
                //console.log(`I'm ${this.build._id} but they want ${_id}`);
                return;
            } else {
                //console.log("_id was " + _id);
                // deleted or archived builds (though replace_one could catch more than this command)
                if(op == "remove" || op == "replace_one") {
                    // can't getBuild() when not there anymore,
                    // propagate a general change_build event
                    bus.$emit("change_build");
                } else {
                    return this.getBuild();
                }
            };
        },
        getBuild: function() {
            axios.get(axios.defaults.baseURL + '/build/' + this.build._id)
            .then(response => {
                this.build_from_api = response.data.result;
            })
            .catch(err => {
                console.log("Error getting build information: " + err);
            })
        },
    },
}
</script>

<style scoped>
  @keyframes pulse {
    0% {transform: scale(1, 1);}
    50% {transform: scale(1.2, 1.2);}
    100% {transform: scale(1, 1);}
  }

  .pulsing {
    animation: pulse 1s linear infinite;
  }

  .conftag {
      margin-bottom: 1em !important;
  }

  a {
        color: #256e08;
    }

  .archived {color:#208CBC;}


</style>
