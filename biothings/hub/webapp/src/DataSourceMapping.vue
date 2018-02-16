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
            <p>These are the mappings for source <b>{{subsrc}}</b>.</p>
            <p><i>Mapping from inspection</i> has been generated during data inspection, while <i>Registered mapping</i> is the actual active mapping, used during indexation.</p>
            <p>Mappings can be manually edited and mapping from inspection can be saved as the new registered, active mapping.</p>
            <div class="ui warning message">
                If a mapping is hard-coded in source code, there won't be any "Save" or "Commit" actions on the registered mapping.
            </div>
            <table class="ui celled table">
                <thead>
                    <tr class="ui center aligned">
                        <th class="eight wide top aligned">
                            <div>Mapping from inspection</div>
                            <div>
                            <button class="ui labeled mini icon button" v-if="maps[subsrc]['mapping']" v-on:click="saveMapping(subsrc,'inspect')">
                                <i class="save icon"></i>
                                Save
                            </button>
                            </div>
                        </th>
                        <th class="eight wide top aligned">
                            Registered mapping
                            <div>
                            <button class="ui labeled mini icon button" v-if="maps[subsrc]['mapping']" v-on:click="saveMapping(subsrc,'master')">
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
                            <mapping-map v-bind:map="maps[subsrc]['mapping']" v-bind:name="subsrc" v-if="maps[subsrc]"></mapping-map>
                        </td>
                        <td>
                            TODO
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
    },
    components: { MappingMap },
    methods: {
        saveMapping: function(subsrc, dest) {
            var html = $("#htmlmap").html();
            var json = this.html2json(html);
            bus.$emit("save_mapping",subsrc,json,dest);
        }
    },
}
</script>

<style>
</style>
