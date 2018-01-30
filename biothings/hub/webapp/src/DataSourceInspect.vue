<template>
    <span>
        <!-- multiple sub-source -->
        <div class="ui styled accordion" v-if="Object.keys(source.upload.sources).length" v-for="(data,subsrc) in source.upload.sources">
            <div class="active title" >
                <i class="dropdown icon"></i>
                {{subsrc}}
            </div>
            <div class="active content" v-if="data.inspect">
                <div class="ui top attached pointing secondary menu">
                    <a class="item" data-tab="type" @click="loadInspect(subsrc,'type')">type</a>
                    <a class="item" data-tab="mapping" @click="loadInspect(subsrc,'mapping')">mapping</a>
                </div>
                <div class="ui bottom attached tab segment" data-tab="type">
                    <type-map v-bind:mainsrc="source._id" v-bind:subsrc="subsrc"></type-map>
                </div>
                <div class="ui bottom attached tab segment" data-tab="mapping">
                    TODO
                </div>
            </div>
            <div class="active content" v-else>
                No inspected data
            </div>
        </div>
        <span v-else>
            No datasource found
        </span>

    </span>

</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import TypeMap from './TypeMap.vue'

export default {
    name: 'data-source-inspect',
    props: ['source'],
    mounted () {
        $('.menu .item').tab();
        $('.ui.accordion').accordion();
    },
    components: { TypeMap},
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
        loadInspect (subsrc,mode) {
            var self = this;
            axios.get(axios.defaults.baseURL + `/source/${self.source._id}`)
            .then(response => {
                if(response.data.result.upload 
                   && response.data.result.upload.sources[subsrc]
                   && response.data.result.upload.sources[subsrc].inspect
                   && response.data.result.upload.sources[subsrc].inspect.results[mode]) {
                       var map = response.data.result.upload.sources[subsrc].inspect.results[mode];
                       console.log("on emit");
                       bus.$emit('type_map', self.source._id, subsrc ,map);
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
