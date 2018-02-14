<template>
    <span>
    <span v-if="maps">
        <!-- multiple sub-source -->
        <span v-if="Object.keys(maps).length > 1">
            <p>Found sub-sources linked to main source <b>{{_id}}</b>, select one to see inspection data</p>
            <div id="maps" class="ui top attached tabular menu">
                <a :class="['green item', i === 0 ? 'active' : '']" v-for="(_,subsrc,i) in maps" :data-tab="subsrc">{{subsrc}}</a>
            </div>
        </span>
        <!--div class="active content" v-if="data">
        <div class="ui top pointing secondary menu" :id="_id + '-tabinspect'">
            <a class="item active" data-tab="type">type</a>
            <a class="item" data-tab="mapping">mapping</a>
        </div>
        <div class="ui tab segment active" data-tab="type">
            <type-map v-bind:map="maps[subsrc]['type']" v-if="maps[subsrc]"></type-map>
        </div>
        <div class="ui tab segment" data-tab="mapping">
            <mapping-map v-bind:map="maps[subsrc]['mapping']" v-if="maps[subsrc]"></mapping-map>
        </div>
    </div>
    <div class="active content" v-else>
        No inspected data
        </div-->
        <div :class="['ui bottom attached tab segment', i === 0 ? 'active' : '']" v-for="(data,subsrc,i) in maps" :data-tab="subsrc" v-if="maps">
            <p>These are the results for the different inspection mode found for source <b>{{subsrc}}</b></p>
            <table class="ui celled table">
                <thead>
                    <tr class="ui center aligned">
                        <th class="eight wide top aligned">Mode <i>type</i></th>
                        <th class="eight wide top aligned">
                            Mode <i>mapping</i>
                            <div>
                            <button class="ui labeled mini icon button" v-if="maps[subsrc]['mapping']" v-on:click="saveMapping(subsrc)">
                                <i class="save icon"></i>
                                Save
                            </button>
                            </div>
                        </th>
                    </tr>
                </thead>
                <tbody>
                    <tr class="top aligned">
                        <td>
                            <type-map v-bind:map="maps[subsrc]['type']" v-if="maps[subsrc]"></type-map>
                        </td>
                        <td>
                            <mapping-map v-bind:map="maps[subsrc]['mapping']" v-bind:name="subsrc" v-if="maps[subsrc]"></mapping-map>
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
    </span>
    <div v-else>
        No inspection data found for this source
    </div>
    </span>

</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import TypeMap from './TypeMap.vue'
import MappingMap from './MappingMap.vue'

export default {
    name: 'data-source-inspect',
    props: ['_id','maps'],
    mounted () {
        $('.menu .item').tab();
        $('#maps .item:first').addClass('active');
        $('.tab:first').addClass('active');
    },
    components: {TypeMap, MappingMap},
    methods: {
        inspect: function() {
            var self = this;
            $(`#inspect-${this._id}.ui.basic.inspect.modal`)
            .modal("setting", {
                onApprove: function () {
                    var modes = $(`#inspect-${self._id}`).find("#select-mode").val();
                    var dp = $(`#inspect-${self._id}`).find("#select-data_provider").val();
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
        saveMapping: function(subsrc) {
            bus.$emit(`save_mapping_${subsrc}`);
        }
    },
}
</script>

<style>
</style>
