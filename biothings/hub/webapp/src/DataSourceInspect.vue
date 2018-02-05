<template>
    <span>
        data source inpest
        <!-- multiple sub-source -->
        <div class="ui styled accordion" v-for="(data,subsrc) in maps">
            <div class="active title" >
                <i class="dropdown icon"></i>
                {{subsrc}}
            </div>
            <div class="active content" v-if="data">
                <div class="ui top pointing secondary menu" :id="source._id + '-tabinspect'">
                    <a class="active item" data-tab="type">type</a>
                    <a class="item" data-tab="mapping">mapping</a>
                </div>
                <div class="ui bottom attached tab segment" data-tab="type">
                    <type-map v-bind:map="maps[subsrc]['type']" v-if="maps[subsrc]"></type-map>
                </div>
                <div class="ui bottom attached tab segment" data-tab="mapping">
                    <mapping-map v-bind:map="maps[subsrc]['mapping']" v-if="maps[subsrc]"></mapping-map>
                </div>
            </div>
            <div class="active content" v-else>
                No inspected data
            </div>
        </div>
        <!--span v-else>
            No datasource found
        </span-->

    </span>

</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import TypeMap from './TypeMap.vue'
import MappingMap from './MappingMap.vue'

export default {
    name: 'data-source-inspect',
    props: ['source','maps'],
    mounted () {
        //$(`${this.source._id}-tabinspect`).tab();
        $('.menu .item').tab();
        $('.ui.accordion').accordion();
    },
    components: {TypeMap, MappingMap},
    methods: {
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
    },
}
</script>

<style>
</style>
