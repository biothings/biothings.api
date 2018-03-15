<template>
    <div class="ui fluid container">
    <div id="build" class="ui centered fluid card" v-if="build">
        <div class="content">

            <div class="left aligned header" v-if="build.target_name">{{ build.target_name }}</div>
            <div class="meta">
                <span class="right floated time" v-if="build.started_at">Built {{ build.started_at | moment("from", "now") }}</span>
                <div :class="['ui',color ? color : 'grey tiny', 'left floated', 'label conftag']">{{build.build_config.name}}</div>
            </div>
            <div class="left aligned description">
                <p>
                    <div class="ui clearing divider"></div>
                    <div>
                        <i class="file outline icon"></i>
                        {{ build.count | currency('',0) }} document{{ build.count &gt; 1 ? "s" : "" }}
                    </div>
                    <br>
                </p>

                <p>
                    <div class="ui top attached pointing menu">
                        <a class="red item active" data-tab="info">Information</a>
                        <a class="red item" data-tab="mapping">Mapping</a>
                        <a class="red item" data-tab="releases">Releases</a>
                    </div>
                    <div class="ui bottom attached tab segment active" data-tab="info">
                        <div class="ui grid">
                            <div class="six wide column">
                                <h5>Configuration</h5>
                                <build-config v-bind:build="build"></build-config>
                            </div>
                            <div class="five wide column">
                                <h5>Sources</h5>
                                <build-sources v-bind:build="build"></build-sources>
                                <h5>Statistics</h5>
                                <build-stats v-bind:build="build"></build-stats>
                            </div>
                            <div class="five wide column">
                                <h5>Logs</h5>
                                <build-logs v-bind:build="build"></build-logs>
                            </div>
                        </div>
                    </div>
                    <div class="ui bottom attached tab segment" data-tab="mapping">
                        TODO
                        <data-source-mapping v-bind:maps="maps" v-bind:_id="_id"></data-source-mapping>
                    </div>
                    <div class="ui bottom attached tab segment" data-tab="releases">
                        <build-releases v-bind:build="build"></build-releases>
                    </div>
                </p>

            </div>
        </div>

        <inspect-form v-bind:toinspect="build" v-bind:select_data_provider="true">
        </inspect-form>

        <!-- Diff-->
        <div class="ui diff modal">
            <div class="ui header">
                <i class="exchange icon"></i>
                JSON diff results
            </div>
            <div class="content">
                <p>Operations describe what is required to get from the data on the left, to the data on the right</p>
                <json-diff-results></json-diff-results>
            </div>
            <div class="actions">
                <div class="ui green ok button">
                    <i class="checkmark icon"></i>
                    OK
                </div>
            </div>
        </div>

    </div>
    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import InspectForm from './InspectForm.vue'
import DataSourceMapping from './DataSourceMapping.vue'
import JsonDiffResults from './JsonDiffResults.vue'
import BuildReleases from './BuildReleases.vue'
import BuildSources from './BuildSources.vue'
import BuildStats from './BuildStats.vue'
import BuildLogs from './BuildLogs.vue'
import BuildConfig from './BuildConfig.vue'

export default {
    name: 'build-detailed',
    props: ['_id','color'],
    components: { InspectForm, DataSourceMapping, JsonDiffResults, BuildReleases,
                  BuildSources, BuildStats, BuildLogs, BuildConfig, },
    mounted () {
        console.log("BuildDetailed mounted");
        this.loadData();
        $('select.dropdown').dropdown();
        $('.menu .item').tab();
    },
    created() {
        bus.$on("save_mapping",this.saveMapping);
        bus.$on("reload_build_detailed",this.loadData);
        bus.$on("show_diffed",this.showDiffed);
    },
    beforeDestroy() {
        bus.$off("save_mapping",this.saveMapping);
        bus.$off("reload_build_detailed",this.loadData);
        bus.$off("show_diffed",this.showDiffed);
    },
    data () {
        return {
            build : null,
        }
    },
    computed: {
        // a computed getter
        maps: function () {
            // organize mappings in a simple object, if mappings exist
            var _maps = {};
            if(this.build.mapping) {
                // registered is the registered/active mapping found in src_master
                _maps["registered_mapping"] = this.build.mapping;
            }
            if(Object.keys(_maps).length)
                return _maps;
            return null;
        }
    },
    methods: {
        displayError : function() {
            var errs = [];
            return errs.join("<br>");
        },
        loadData () {
            var self = this;
            axios.get(axios.defaults.baseURL + `/build/${this._id}`)
            .then(response => {
                console.log(response.data.result)
                self.build = response.data.result;
            })
            .catch(err => {
                console.log("Error getting build information: " + err);
            })
        },
        saveMapping: function(subsrc,map,dest, map_id) {
            console.log(`Saving mapping for ${subsrc} dest:${dest}`);
            axios.put(axios.defaults.baseURL + `/build/${subsrc}/mapping`,
                        {"mapping" : map, "dest" : dest})
            .then(response => {
                console.log(response.data.result)
                this.loadData();
                bus.$emit(`${subsrc}-${map_id}-mapping_saved`);
            })
            .catch(err => {
                console.log("Error : " + err);
            })
        },
        showDiffed : function() {
            $('.ui.diff.modal').modal({
                observeChanges: true,
                detachable: false,
            })
            .modal("show")
        },
    },
}
</script>

<style>
.conftag {
    margin-top: 1em !important;
    margin-bottom: 1em !important;
}
</style>
