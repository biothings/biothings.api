<template>
    <div class="ui container">
    <div id="data-source" class="ui centered fluid card" v-if="source">
        <div class="content">

            <div class="left aligned header" v-if="source.name">{{ source.name | splitjoin | capitalize }}</div>
            <div class="meta">
                <span class="right floated time" v-if="source.download && source.download.started_at">Updated {{ source.download.started_at | moment("from", "now") }}</span>
                <span class="right floated time" v-else>Never updated</span>
                <span class="left floated category">{{ source.release }}</span>
            </div>
            <div class="left aligned description">
                <p>
                    <div class="ui clearing divider"></div>
                    <div>
                        <i class="file outline icon"></i>
                        {{ source.count | currency('',0) }} document{{ source.count &gt; 1 ? "s" : "" }}
                    </div>
                    <br>
                    <div>
                        <i class="folder icon"></i>
                        Data folder: {{source.data_folder}}
                    </div>
                </p>

                <p>
                    <div class="ui top attached pointing menu">
                        <a class="red item active" data-tab="dump" v-if="source.download">Dumper</a>
                        <a class="red item" data-tab="upload">Uploader</a>
                        <a class="red item" data-tab="mapping">Mapping</a>
                        <a class="red item" data-tab="inspect">Statistics</a>
                    </div>
                    <div class="ui bottom attached tab segment active" data-tab="dump" v-if="source.download">
                        <data-source-dump v-bind:source="source"></data-source-dump>
                    </div>
                    <div class="ui bottom attached tab segment" data-tab="upload">
                        upload TODO
                    </div>
                    <div class="ui bottom attached tab segment" data-tab="mapping">
                        <data-source-mapping v-bind:maps="maps" v-bind:_id="_id"></data-source-mapping>
                    </div>
                    <div class="ui bottom attached tab segment" data-tab="inspect">
                        <data-source-inspect v-bind:maps="maps" v-bind:_id="_id"></data-source-inspect>
                    </div>
                </p>

            </div>
        </div>
        <div class="extra content">
            <div class="ui icon buttons left floated mini">
                <button class="ui button" v-on:click="dump" v-if="source.download">
                    <i class="download cloud icon"></i>
                </button>
                <button class="ui button" v-on:click="upload" v-if="source.upload">
                    <i class="database icon"></i>
                </button>
            </div>
            <div class="ui icon buttons left floated mini">
                <button class="ui button" v-on:click="inspect">
                    <i class="unhide icon"></i>
                </button>
            </div>
            <div class="ui icon buttons right floated mini">
                <button class="ui button"
                    v-on:click="unregister" v-if="source.data_plugin">
                    <i class="remove icon"></i>
                </button>
            </div>
        </div>

        <inspect-form v-bind:toinspect="source" v-bind:select_data_provider="true">
        </inspect-form>

        <!-- Register new data plugin -->
        <div class="ui basic unregister modal" v-if="source.data_plugin">
            <input class="plugin_url" type="hidden" :value="source.data_plugin.plugin.url">
            <div class="ui icon header">
                <i class="remove icon"></i>
                Unregister data plugin
            </div>
            <div class="content">
                <p>Are you sure you want to unregister and delete data plugin <b>{{source.name}}</b> ?</p>
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

        <!-- Diff-->
        <div class="ui basic diff modal">
            <div class="ui icon header">
                <i class="exchange icon"></i>
                JSON diff results
            </div>
            <div class="content">
                <p>Operations describe what is required to get from the data on the left, to the data on the right</p>
                <json-diff-results></json-diff-results>
            </div>
            <div class="actions">
                <div class="ui green ok inverted button">
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
import DataSourceDump from './DataSourceDump.vue'
import DataSourceInspect from './DataSourceInspect.vue'
import DataSourceMapping from './DataSourceMapping.vue'
import JsonDiffResults from './JsonDiffResults.vue'

export default {
    name: 'data-source-detailed',
    props: ['_id'],
    mounted () {
        console.log("DataSourceDetailed mounted");
        this.loadData();
        $('select.dropdown').dropdown();
        $('.menu .item').tab();
    },
    created() {
        bus.$on("save_mapping",this.saveMapping);
        bus.$on("reload_datasource_detailed",this.loadData);
        bus.$on("show_diffed",this.showDiffed);
    },
    beforeDestroy() {
        bus.$off("save_mapping",this.saveMapping);
        bus.$off("reload_datasource_detailed",this.loadData);
        bus.$off("show_diffed",this.showDiffed);
    },
    data () {
        return {
            source : null,
        }
    },
    computed: {
        // a computed getter
        maps: function () {
            // organize mappings in a simple object, if mappings exist
            if(this.source.inspect && this.source.inspect.sources) {
                var _maps = {};
                for(var subsrc in this.source.inspect.sources) {
                    if(this.source.inspect.sources[subsrc]["inspect"]) {
                        _maps[subsrc] = {};
                        for(var mode in this.source.inspect.sources[subsrc]["inspect"].results) {
                            _maps[subsrc][`inspect_${mode}`] = this.source.inspect.sources[subsrc]["inspect"].results[mode];
                        }
                    }
                }
                for(var subsrc in this.source.mapping) {
                    if(!subsrc in _maps)
                        _maps[subsrc] = {};
                    if(!_maps[subsrc]) {
                        _maps[subsrc] = {};
                    }
                    // registered is the registered/active mapping found in src_master
                    _maps[subsrc]["registered_mapping"] = this.source.mapping[subsrc];
                }
                if(Object.keys(_maps).length)
                    return _maps;
            }
            return null;
        }
    },
    components: { InspectForm, DataSourceDump, DataSourceInspect, DataSourceMapping, JsonDiffResults },
    methods: {
        displayError : function() {
            var errs = [];
            if (this.source.download && this.source.download.status == "failed")
                errs.push("Download failed: " + this.source.download.error);
            if (this.source.upload && this.source.upload.status == "failed")
                errs.push("Upload failed: " + this.source.upload.error);
            return errs.join("<br>");
        },
        dump: function() {
            axios.put(axios.defaults.baseURL + `/source/${this.source.name}/dump`)
            .then(response => {
                console.log(response.data.result)
                this.$parent.getSourcesStatus();
            })
            .catch(err => {
                console.log("Error getting job manager information: " + err);
            })
        },
        upload: function() {
            axios.put(axios.defaults.baseURL + `/source/${this.source.name}/upload`)
            .then(response => {
                console.log(response.data.result)
                this.$parent.getSourcesStatus();
            })
            .catch(err => {
                console.log("Error getting job manager information: " + err);
            })
        },
        unregister: function() {
            $('.ui.basic.unregister.modal')
            .modal("setting", {
                onApprove: function () {
                    var url = $(this).find("input.plugin_url").val();
                    axios.delete(axios.defaults.baseURL + '/dataplugin/unregister_url',{"url":url})
                    .then(response => {
                        console.log(response.data.result)
                        bus.$emit("refresh_sources");
                        return true;
                    })
                    .catch(err => {
                        console.log(err);
                        console.log("Error registering repository URL: " + err.data.error);
                    })
                }
            })
            .modal("show");
        },
        inspect: function() {
            var self = this;
            $(`#inspect-${this._id}.ui.basic.inspect.modal`)
            .modal("setting", {
                onApprove: function () {
                    var modes = $(`#inspect-${self._id}`).find("#select-mode").val();
                    var dp = $(`#inspect-${self._id}`).find("#select-data_provider").val();
                    console.log(modes);
                    console.log(dp);
                    axios.put(axios.defaults.baseURL + '/inspect',
                              {"data_provider" : [dp,self._id],"mode":modes})
                    .then(response => {
                        console.log(response.data.result)
                        bus.$emit("refresh_sources");
                    })
                    .catch(err => {
                        console.log("Error getting job manager information: " + err);
                    })
                }
            })
            .modal("show");
        },
        loadData () {
            var self = this;
            axios.get(axios.defaults.baseURL + `/source/${this._id}`)
            .then(response => {
                console.log(response.data.result)
                self.source = response.data.result;
            })
            .catch(err => {
                console.log("Error getting source information: " + err);
            })
        },
        saveMapping: function(subsrc,map,dest, map_id) {
            console.log(`Saving mapping for ${subsrc} dest:${dest}`);
            axios.put(axios.defaults.baseURL + `/source/${subsrc}/mapping`,
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
            $('.ui.basic.diff.modal').modal("show");
        },
    },
}
</script>

<style>
</style>
