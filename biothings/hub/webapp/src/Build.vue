<template>
    <div class="ui fluid card">
        <div class="content">
            <div :class="['ui',color ? color : 'grey', 'tiny', '', 'label','conftag']">{{build.build_config.name}}</div>

            <!-- in progress -->
            <i class="right floated cube icon pulsing"
                v-if="build.status == 'building'"></i>
            <i class="right floated unhide icon pulsing"
                v-if="build.status == 'inspecting'"></i>

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
                <button class="ui button" v-on:click="inspect">
                    <i class="unhide icon"></i>
                </button>
            </div>
            <div class="ui icon buttons right floated mini">
                <button class="ui button">
                    <i class="trash icon" @click="deleteBuild()"></i>
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

        <inspect-form v-bind:toinspect="build" v-bind:select_data_provider="false">
        </inspect-form>

    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import Vue from 'vue';
import InspectForm from './InspectForm.vue'
import BuildLogs from './BuildLogs.vue'
import BuildStats from './BuildStats.vue'
import BuildSources from './BuildSources.vue'

function build_time(jobs) {
    return jobs.map((q)=>q.time_in_s).reduce(
            function(total, q) {
                return total + q
            }, 0);
};
Vue.filter('build_time',build_time);

export default {
    name: 'build',
    props: ['build','color'],
    mounted() {
        $('.menu .item')
        .tab()
        ;
    },
    beforeDestroy() {
        $(`#${this.build._id}.ui.basic.deletebuild.modal`).remove();
    },
    components: { InspectForm, BuildLogs, BuildStats, BuildSources, },
    methods: {
        displayError : function() {
            var errs = [];
            if (this.build.jobs) {
                var last = this.build.jobs[this.build.jobs.length-1];
                if(last && last.err)
                    errs.push(`Step '${last.step}' failed: ${last.err}`);
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
                        bus.$emit("refresh_builds");
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
        inspect: function() {
            var self = this;
            $(`#inspect-${this.build._id}.ui.basic.inspect.modal`)
            .modal("setting", {
                onApprove: function () {
                    var modes = $(`#inspect-${self.build._id}`).find("#select-mode").val();
                    axios.put(axios.defaults.baseURL + '/inspect',
                              {"data_provider" : self.build._id,"mode":modes})
                    .then(response => {
                        console.log(response.data.result)
                        bus.$emit("refresh_builds");
                    })
                    .catch(err => {
                        console.log("Error getting job manager information: " + err);
                    })
                }
            })
            .modal("show");
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

</style>
