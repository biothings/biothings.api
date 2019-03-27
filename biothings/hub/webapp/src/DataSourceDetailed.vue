<template>
    <div class="ui fluid container">
        <div id="data-source" class="ui centered fluid card" v-if="source">
            <div class="content">

                <div class="left aligned header" v-if="source.name">{{ source.name }}</div>
                <div class="meta">
                    <span class="right floated time" v-if="source.download && source.download.started_at">Updated {{ source.download.started_at | moment("from", "now") }}</span>
                    <span class="right floated time" v-else>Never updated</span>
                    <span class="left floated category">{{ release }}</span>
                </div>
                <div class="left aligned description">
                    <p>
                        <div class="ui clearing divider"></div>
                        <div class="left floated">
                            <i class="file outline icon"></i>
                            {{ source.count | currency('',0) }} document{{ source.count &gt; 1 ? "s" : "" }}
                        </div>
                        <div class="right floated">
                            <span v-if="license !== null && typeof license === 'object'">
                                <table class="meta ui single line compact small table">
                                    <thead>
                                        <tr>
                                            <th v-for="(_,src) in license">{{src}}</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <tr>
                                            <td v-for="url in license">
                                                <a v-if="url.startsWith('http')" :href="url">license</a>
                                                <span v-else>{{license}}</span>
                                            </td>
                                        </tr>
                                        <tr>
                                            <td v-for="url in website">
                                                <a :href="url">website</a>
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </span>
                            <span v-else>
                                <table class="meta ui single line compact small table">
                                    <tbody>
                                        <tr>
                                            <td>
                                                <a v-if="license_url" :href="license_url">
                                                    <span v-if="license">{{license}}</span*>
                                                    <span v-else>license</span>
                                                </a>
                                                <span v-else>{{license}}</span>
                                            </td>
                                            <td>
                                                <a v-if="website" :href="website">website</a>
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </span>
                        </div>
                        <br>
                    </p>

                    <p>
                        <div class="ui top attached pointing menu">
                            <a class="red item active" data-tab="dump" v-if="source.download">Dumper</a>
                            <!-- in case no dumper, uploader should be active tab -->
                            <a :class="['red item', source.download == undefined ? 'active' : '']" data-tab="upload">Uploader</a>
                            <a class="red item" data-tab="plugin" v-if="source.data_plugin">Plugin</a>
                            <a class="red item" data-tab="mapping">Mapping</a>
                            <!--a class="red item" data-tab="inspect">Statistics</a-->
                        </div>
                        <div class="ui bottom attached tab segment " data-tab="dump" v-if="source.download">
                            <data-source-dump v-bind:source="source"></data-source-dump>
                        </div>
                        <div :class="['ui bottom attached tab segment', source.download == undefined ? 'active' : '']" data-tab="upload">
                            <data-source-upload v-bind:source="source"></data-source-upload>
                        </div>
                        <div class="ui bottom attached tab segment active" data-tab="plugin" v-if="source.data_plugin">
                            <data-source-plugin v-bind:source="source"></data-source-plugin>
                        </div>
                        <div class="ui bottom attached tab segment" data-tab="mapping">
                            <data-source-mapping v-bind:maps="maps" v-bind:_id="_id"></data-source-mapping>
                        </div>
                        <!--div class="ui bottom attached tab segment" data-tab="inspect">
                        <data-source-inspect v-bind:maps="maps" v-bind:_id="_id"></data-source-inspect>
                        </div-->
                    </p>

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
            <diff-modal></diff-modal>

        </div>
    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import InspectForm from './InspectForm.vue'
import BaseDataSource from './BaseDataSource.vue'
import DataSourceDump from './DataSourceDump.vue'
import DataSourceUpload from './DataSourceUpload.vue'
import DataSourceInspect from './DataSourceInspect.vue'
import DataSourcePlugin from './DataSourcePlugin.vue'
import DataSourceMapping from './DataSourceMapping.vue'
import DiffModal from './DiffModal.vue'
import Loader from './Loader.vue'

export default {
    name: 'data-source-detailed',
    props: ['_id'],
    components: { InspectForm, DataSourceDump, DataSourceUpload, DataSourceInspect,
                  DataSourcePlugin, DataSourceMapping, DiffModal, Loader},
    mixins : [ BaseDataSource, Loader],
    mounted () {
        console.log("DataSourceDetailed mounted");
        this.loadData();
        $('select.dropdown').dropdown();
        $('.menu .item').tab();
    },
    created() {
        bus.$on('change_source',this.loadData);
        bus.$on('change_master',this.loadData);
        bus.$on('change_data_plugin',this.loadData);
    },
    beforeDestroy() {
        bus.$on('change_source',this.loadData);
        bus.$off('change_master',this.loadData);
        bus.$on('change_data_plugin',this.loadData);
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
            var _maps = {};
            if(this.source.inspect && this.source.inspect.sources) {
                for(var subsrc in this.source.inspect.sources) {
                    if(this.source.inspect.sources[subsrc]["inspect"]) {
                        _maps[subsrc] = {};
                        for(var mode in this.source.inspect.sources[subsrc]["inspect"].results) {
                            _maps[subsrc][`inspect_${mode}`] = this.source.inspect.sources[subsrc]["inspect"].results[mode];
                        }
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

            return null;
        },
        license: function() {
            return this.pick_metadata(["license"]);
        },
        license_url: function() {
            return this.pick_metadata(["license_url","license_url_short"]);
        },
        website: function() {
            return this.pick_metadata(["url"]);
        },

    },
    methods: {
        loadData () {
            var self = this;
            this.loading();
            axios.get(axios.defaults.baseURL + `/source/${this._id}`)
            .then(response => {
                //console.log(response.data.result)
                self.source = response.data.result;
                this.loaded();
            })
            .catch(err => {
                console.log("Error getting source information: " + err);
                this.loaderror(err);
            })
        },
        pick_metadata: function(fields_priority) {
            var meta = null;
            function pick(values) {
                var picked = null;
                for(var field in fields_priority) {
                    if(values[fields_priority[field]]) {
                        picked = values[fields_priority[field]];
                    }
                }
                return picked;
            };

            if(this.source.__metadata__) {
                // one or more licenses ?
                // One : __metadata__ is a dictionary containing license keys
                // More: __metadata__ is a dictionary indexed by sub-src names
                if(Array.isArray(this.source.__metadata__)) {
                    meta = {}
                    for(var idx in this.source.__metadata__) {
                        var m = this.source.__metadata__[idx];
                        for(var subsrc in m) {
                            var picked = pick(m[subsrc]);
                            if(picked)
                                meta[subsrc] = picked;
                        }
                    }
                } else {
                    for(var src in this.source.__metadata__) {
                        meta= pick(this.source.__metadata__)
                    }
                }
            }
            return meta;
        },
    },
}
</script>

<style>
.meta.ui.table {
    margin-bottom: 1em;
    border: 0px;
}
.meta.ui.table thead th {
    padding: 0.4em 0.4em 0.4em 0.4em !important;
    text-align: center;
}
.meta.ui.table tbody td {
    padding: 0.4em 0.4em 0.4em 0.4em !important;
    text-align: center;
}
</style>
