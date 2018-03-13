<template>
    <span>
    <span v-if="maps">
        <!-- multiple sub-source -->
        <span v-if="Object.keys(maps).length > 1">
            <p>Found sub-sources linked to main source <b>{{_id}}</b>, select one to see mapping</p>
            <div id="maps" class="ui top attached tabular menu">
                <a :class="['green item', i === 0 ? 'active' : '']" v-for="(_,subsrc,i) in maps" :data-tab="subsrc">{{subsrc}}</a>
            </div>
        </span>
        <div :class="['ui bottom attached tab segment', i === 0 ? 'active' : '']" v-for="(data,subsrc,i) in maps" :data-tab="subsrc" v-if="maps">
            <p>
                These are the mappings for source <b>{{subsrc}}</b>.
            </p>
            <p>
                <i>Mapping from inspection</i> has been generated during data inspection, while <i>Registered mapping</i> is the actual active mapping, used during indexation.
            </p>
            <p>
                Mappings can be manually edited and mapping from inspection can be saved as the new registered, active mapping.
            </p>
            <div class="ui warning message">
                <ul class="ui list">
                    <li>If a mapping is hard-coded in source code, it can't be edited, saved or replaced.</li>
                    <li>When testing a mapping, an temporary index is created on the selection ElasticSearch environment. That index is then deleted.</li>
                </ul>
            </div>
            <table class="ui celled table">
                <thead>
                    <tr class="ui center aligned">
                        <th class="eight wide top aligned">
                            <div>Mapping from inspection</div>
                            <div>
                                <button class="ui labeled mini icon button" 
                                    v-if="maps[subsrc]['inspect_mapping']
                                          && !maps[subsrc]['inspect_mapping']['errors']
                                          && !maps[subsrc]['inspect_mapping']['pre-mapping']"
                                        v-on:click="saveMapping('tab_mapping_inspected',subsrc,'inspect')">
                                    <i class="save icon"></i>
                                    Save
                                </button>
                                <!-- TODO: duplication !!! -->
                                <div class="ui mini buttons"
                                    v-if="maps[subsrc]['registered_mapping']">
                                    <div class="ui labeled icon button"
                                        v-on:click="testMapping('tab_mapping_inspected',subsrc)">
                                        <i class="check square outline icon"></i>Validate on <span :class='[subsrc,"tab_mapping_inspected test-on"]'>...</span>
                                    </div>
                                    <div class="ui floating tab_mapping_inspected dropdown icon button">
                                        <i class="dropdown icon"></i>
                                        <div class="menu">
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </th>
                        <th class="eight wide top aligned">
                            <div>Registered mapping</div>
                            <div> <i>{{ maps[subsrc]['registered_mapping'] ? maps[subsrc]['registered_mapping']['origin'] == 'uploader' ? '(from python uploader code)':'' : ''}}</i></div>
                            <div>
                                <button class="ui labeled mini icon button"
                                        v-if="maps[subsrc]['registered_mapping'] && maps[subsrc]['registered_mapping']['origin'] != 'uploader'"
                                        v-on:click="saveMapping('tab_mapping_registered',subsrc,'master')">
                                        <i class="save icon"></i>
                                        Save
                                </button>
                                <div class="ui mini buttons"
                                    v-if="maps[subsrc]['registered_mapping']">
                                    <div class="ui labeled icon button"
                                        v-on:click="testMapping('tab_mapping_registered',subsrc)">
                                        <i class="check square outline icon"></i>Validate on <span :class='[subsrc,"tab_mapping_registered test-on"]'>...</span>
                                    </div>
                                    <div class="ui floating tab_mapping_registered dropdown icon button">
                                        <i class="dropdown icon"></i>
                                        <div class="menu">
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </th>
                    </tr>
                    <tr v-if="maps[subsrc]['inspect_mapping'] && !maps[subsrc]['inspect_mapping']['errors'] && !maps[subsrc]['inspect_mapping']['pre-mapping']">
                        <th colspan="2" class="sixteen wide top center aligned">
                                <button class="ui labeled mini icon button"
                                    v-on:click="diffMapping('tab_mapping_inspected','tab_mapping_registered',subsrc)">
                                        <i class="exchange icon"></i>
                                        Diff
                                </button>
                                <button class="ui labeled mini icon button"
                                    v-if="maps[subsrc]['registered_mapping'] ? maps[subsrc]['registered_mapping']['origin'] != 'uploader' : true"
                                        v-on:click="commitMapping('tab_mapping_inspected',subsrc)">
                                        <i class="angle double right icon"></i>
                                        Commit
                                </button>
                        </th>
                    </tr>
                </thead>
                <tbody>
                    <tr class="top aligned">
                        <td>
                            <mapping-map v-bind:map="maps[subsrc]['inspect_mapping']"
                                v-bind:name="subsrc"
                                v-bind:map_id="'tab_mapping_inspected'"
                                v-if="maps[subsrc]"
                                v-bind:read_only="maps[subsrc]['inspect_mapping'] && maps[subsrc]['inspect_mapping']['pre-mapping']"
                                ></mapping-map>
                        </td>
                        <td>
                            <mapping-map v-bind:map="maps[subsrc]['registered_mapping']['mapping']" 
                                v-bind:name="subsrc"
                                v-bind:map_id="'tab_mapping_registered'"
                                v-bind:read_only="maps[subsrc]['registered_mapping'] && maps[subsrc]['registered_mapping']['origin'] == 'uploader'"
                                v-if="maps[subsrc]['registered_mapping']">
                            </mapping-map>
                        </td>
                    </tr>
                </tbody>
            </table>
            <div v-bind:id="'modal_commit_' + subsrc" class="ui modal">
                <div class="ui icon header">
                    <i class="angle double right icon"></i>
                    Commit new mapping
                </div>
                <div class="content">
                    <p>Are you sure you want to commit mapping from inspection?</p>
                    <p>This will be replace current registered one.</p>
                </div>
                <div class="actions">
                    <div class="ui red basic cancel button">
                        <i class="remove icon"></i>
                        No
                    </div>
                    <div class="ui green ok button">
                        <i class="checkmark icon"></i>
                        Yes
                    </div>
                </div>

            </div>
        </div>
    </span>
    <div v-else>
        No mapping data found for this source
    </div>
    </span>

</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import MappingMap from './MappingMap.vue'
import Utils from './Utils.vue'

export default {
    name: 'data-source-mapping',
    props: ['_id','maps'],
    mixins: [Utils],
    mounted () {
        $('.menu .item').tab();
        $('#maps .item:first').addClass('active');
        $('.tab:first').addClass('active');
        this.buildIndexDropdown("tab_mapping_registered");
        this.buildIndexDropdown("tab_mapping_inspected");
    },
    components: { MappingMap },
    data () {
        return {
            environments : {},
        }
    },
    methods: {
        saveMapping: function(map_elem_id,subsrc, dest) {
            var html = $(`#${subsrc}-${map_elem_id}`).html();
            var json = this.html2json(html);
            bus.$emit("save_mapping",subsrc,json,dest,map_elem_id);
        },
        commitMapping: function(map_elem_id,subsrc) {
            var self = this;
            $(`#modal_commit_${subsrc}`)
            .modal("setting", {
                onApprove: function () {
                    self.saveMapping(map_elem_id,subsrc,'master');
                    bus.$emit("reload_datasource_detailed");
                }
            })
            .modal(`show`);
        },
        diffMapping: function(map_elem_id_left, map_elem_id_right,subsrc) {
            var lefthtml = $(`#${subsrc}-${map_elem_id_left}`).html();
            var leftjson = this.html2json(lefthtml);
            var righthtml = $(`#${subsrc}-${map_elem_id_right}`).html();
            var rightjson = this.html2json(righthtml);
            axios.post(axios.defaults.baseURL + `/jsondiff`,{"src" : leftjson, "dst" : rightjson})
            .then(response => {
                console.log(response.data.result)
                bus.$emit("diffed",response.data.result);
                bus.$emit("show_diffed");
            })
            .catch(err => {
                console.log("Error diffing mappings: " + err);
            })
        },
        buildIndexDropdown : function(map_id) {
            axios.get(axios.defaults.baseURL + `/index_manager`)
            .then(response => {
                this.environments = Object.keys(response.data.result.config.env);
                var envs = [];
                var cnt = 0;
                for(var e in this.environments) {
                    var d = {"name" : this.environments[e], "value" : this.environments[e]}
                    if(cnt == 0)
                        d["selected"] = true;
                    envs.push(d);
                    cnt++;
                }
                $(`.ui.${map_id}.dropdown`).dropdown({
                    values: envs,
                    onChange: function(value, text, $selectedItem) {
                        console.log(`value ${value} text ${text}`);
                        $(`.${map_id}.test-on`).text(`${value}`);
                    }
                });
            })
            .catch(err => {
                console.log("Error getting index environments: " + err);
            })
        },
        testMapping: function(map_elem_id,subsrc, dest) {
            var html = $(`#${subsrc}-${map_elem_id}`).html();
            var json = this.html2json(html);
            var env = $(`.${subsrc}.${map_elem_id}.test-on`).text();
            axios.post(axios.defaults.baseURL + `/mapping/validate`,{"mapping" : json, "env" : env})
            .then(response => {
                console.log(response.data.result)
                bus.$emit(`mapping_test_${map_elem_id}-${subsrc}`,"","info");
            })
            .catch(err => {
                console.log("Error validating mapping: ");
                console.log(err);
                bus.$emit(`mapping_test_${map_elem_id}-${subsrc}`,err.data.error,"error");
            })
        },
    },
}
</script>

<style>
</style>
