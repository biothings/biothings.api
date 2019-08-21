<template>
    <div class="ui fluid container">
        <div id="" class="ui centered fluid card">
            <div class="ui icon message">
                <i class="exclamation triangle icon"></i>
                <div class="content">
                    <div class="header">
                        Warning
                    </div>
                    <p>
                        From this page, you can restart or stop the Hub, access and modify its internal configuraton in order to customize the behavior and appearance.
                        Be careful though, as incorrect values could lead to a non-working system.
                    </p>
                </div>
            </div>
            <div class="content">
                <p>
                    <div class="ui top attached pointing menu">
                        <a class="red item " :data-tab="section" v-if="config" v-for="section in Object.keys(config)">{{ section }} </a>
                    </div>
                    <div class="ui bottom attached tab segment" :data-tab="section" v-if="config" v-for="section in Object.keys(config)">
                        <hub-config-tab v-bind:section="section" v-bind:params="config[section]"></hub-config-tab>
                    </div>
                </p>
            </div>

        </div>
    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import Loader from './Loader.vue'
import HubConfigTab from './HubConfigTab.vue'

export default {
    name: 'hub-config',
    props: [],
    components: { Loader, HubConfigTab},
    mixins : [ Loader],
    mounted () {
        console.log("HubConfig mounted");
        this.loadData();
    },
    created() {
    },
    updated() {
        $('.menu .item').tab();
        $('.menu .item').tab('change tab', 'General')
    },
    beforeDestroy() {
    },
    data () {
        return {
            config: {},
            dirty: false,
            error: null,
        }
    },
    computed: {
        // a computed getter
    },
    methods: {
        loadData () {
            var self = this;
            this.loading();
            axios.get(axios.defaults.baseURL + `/config`)
            .then(response => {
                try {
                    var conf = response.data.result.scope.config; // shorten...
                    // re-organize by section
                    var bysections = {};
                    for(var param in conf) {
                        var info = conf[param];
                        // rename default section
                        var section = info.section || "General";
                        delete info.section; // no need to store for each param now
                        info.name = param; // but we need to store the parameter name
                        if(bysections.hasOwnProperty(section)) {
                            bysections[section].push(info);
                        } else {
                            bysections[section] = [info];
                        }
                    }
                    this.config = bysections;
                    this.loaded();
                } catch(err) {
                    self.error = `Can't parse configuration: ${err}`;
                }
            })
            .catch(err => {
                console.log("Error getting source config information: " + err);
                this.loaderror(err);
            })
        },
    },
}
</script>

<style scoped>
.ui.config.segment {
    color: black;
}
</style>
