<template>
    <div id="data-source" class="ui fluid card" style="height: 100%;overflow: scroll;">
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
                    <div class="ui top attached pointing  menu">
                        <a class="item active" data-tab="dump" v-if="source.download">Dumper</a>
                        <a class="item" data-tab="upload">Uploader(s)</a>
                        <a class="item" data-tab="inspect" @click="loadInspect()">Data inspection</a>
                    </div>
                    <div class="ui bottom attached tab segment active" data-tab="dump" v-if="source.download">
                        <data-source-dump v-bind:source="source"></data-source-dump>
                    </div>
                    <div class="ui bottom attached tab segment" data-tab="upload">
                        upload TODO
                    </div>
                    <div class="ui bottom attached tab segment" data-tab="inspect">
                        <data-source-inspect v-bind:maps="maps" v-bind:source="source" v-if="source"></data-source-inspect>
                        <span v-else>No data available</span>

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
            <div class="ui icon buttons right floated mini">
                <button class="ui button"><i class="angle double right icon"></i></button>
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


    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import InspectForm from './InspectForm.vue'
import DataSourceDump from './DataSourceDump.vue'
import DataSourceInspect from './DataSourceInspect.vue'

export default {
    name: 'data-source-detailed',
    props: ['source'],
    mounted () {
        $('select.dropdown').dropdown();
        $('.menu .item')
        .tab()
        ;
    },
    data () {
        return {
            maps : {},
        }
    },
    components: { InspectForm, DataSourceDump, DataSourceInspect },
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
            $(`#inspect-${this.source._id}.ui.basic.inspect.modal`)
            .modal("setting", {
                onApprove: function () {
                    var modes = $(`#inspect-${self.source._id}`).find("#select-mode").val();
                    var dp = $(`#inspect-${self.source._id}`).find("#select-data_provider").val();
                    console.log(modes);
                    console.log(dp);
                    axios.put(axios.defaults.baseURL + '/inspect',
                              {"data_provider" : [dp,self.source._id],"mode":modes})
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
        loadInspect () {
            var self = this;
            axios.get(axios.defaults.baseURL + `/source/${self.source._id}`)
            .then(response => {
                if(response.data.result.upload && response.data.result.upload.sources) {
                    /*&& response.data.result.upload.sources[subsrc]
                      && response.data.result.upload.sources[subsrc].inspect
                      && response.data.result.upload.sources[subsrc].inspect.results[mode]) {*/
                    for(var subsrc in response.data.result.upload.sources) {
                        if(response.data.result.upload.sources[subsrc].inspect) {
                            self.maps[subsrc] = {};
                            for(var mode in response.data.result.upload.sources[subsrc].inspect.results) {
                                console.log(`mode ${mode}`);
                                self.maps[subsrc][mode] = response.data.result.upload.sources[subsrc].inspect.results[mode];
                                //var map = response.data.result.upload.sources[subsrc].inspect.results[mode];
                                //console.log(`on emit ${mode}_map`);
                                //bus.$emit(`${mode}_map`, self.source._id, subsrc , self.maps[subsrc][mode]);
                            }
                        }
                    }
                } else {
                    throw 'No inspection data';
                }
            })
            .catch(err => {
                console.log("Error getting inspection data: " + err);
            })
        },
    },
}
</script>

<style>
</style>
