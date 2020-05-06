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

                <div class="configmenu">
                    <div class="ui secondary small menu">
                        <div class="ui mini yellow compact message right floated" v-if="dirty">Hub needs to restart to reflect changes</div>
                        <a class="right aligned item" :class="actionable">
                            <a class="item">
                                <button class="ui red labeled icon button" @click="restartHub">
                                    <i class="sync alternate icon"></i>
                                    Restart
                                </button>
                            </a>
                        </a>
                    </div>
                </div>

                <p>
                    <div class="ui mini top attached pointing menu">
                        <!-- array.keys() gives index number -->
                        <a :class="['red item',idx == 0 ? 'active' : '']" :data-tab="config_tabs[idx]" v-if="config" v-for="idx in config_tabs.keys()" >{{ config_tabs[idx] }} </a>
                    </div>
                    <div :class="['ui bottom attached tab segment',idx == 0 ? 'active':'']" :data-tab="config_tabs[idx]" v-if="config" v-for="idx in config_tabs.keys()">
                        <hub-config-tab v-bind:section="config_tabs[idx]" v-bind:params="config[config_tabs[idx]]" v-bind:allow_edits="allow_edits"></hub-config-tab>
                    </div>
                </p>
            </div>

        </div>

        <div class="ui basic restart modal">
            <div class="ui icon header">
                <i class="sync alternate icon"></i>
                Restart Hub ?
            </div>
            <div class="content">
                <p>Are you sure you want to restart the Hub ?</p>
                <div class="ui warning message">
                    Note: Hub will wait until all jobs are finished before restarting.
                    While restarting you will loose connection. Please allow some time before
                    being able to connect again.
                </div>
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
import Vue from 'vue';
import axios from 'axios'
import bus from './bus.js'
import Loader from './Loader.vue'
import Actionable from './Actionable.vue'
import HubConfigTab from './HubConfigTab.vue'

export default {
    name: 'hub-config',
    props: [],
    components: { Loader, HubConfigTab},
    mixins : [ Loader, Actionable, ],
    mounted () {
        console.log("HubConfig mounted");
        this.loadData();
    },
    created() {
        bus.$on('change_config',this.onConfigChanged);
        bus.$on('restart_hub',this.restartHub)
        bus.$on('save_config_param',this.onSaveParameter);
    },
    updated() {
        $('.menu .item').tab();
    },
    beforeDestroy() {
        bus.$off('change_source',this.onConfigChanged);
        bus.$off('restart_hub',this.restartHub)
        bus.$off('save_config_param',this.onSaveParameter);
    },
    data () {
        return {
            config: {},
            dirty: false,
            allow_edits: false,
            error: null,
        }
    },
    computed: {
        config_tabs: function() {
            return Object.keys(this.config).sort();
        },
    },
    methods: {
        loadData () {
            var self = this;
            this.loading();
            axios.get(axios.defaults.baseURL + `/config`)
            .then(response => {
                try {
                    var conf = response.data.result.scope.config; // shorten...
                    self.dirty = response.data.result._dirty;
                    self.allow_edits = response.data.result.allow_edits;
                    // re-organize by section
                    var bysections = {};
                    for(var param in conf) {
                        var info = conf[param];
                        // rename default section
                        var section = info.section || "Misc";
                        delete info.section; // no need to store for each param now
                        info.name = param; // but we need to store the parameter name
                        if(bysections.hasOwnProperty(section)) {
                            bysections[section].push(info);
                        } else {
                            bysections[section] = [info];
                        }
                    }
                    this.config = bysections;
                    // make this config avail globally
                    Vue.config.hub_config = response.data.result.scope.config;
                    bus.$emit("current_config",response.data.result);
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
        restartHub: function() {
            var self = this;
            this.loading();
            $(`.ui.basic.restart.modal`)
            .modal("setting", {
                onApprove: function () {
                    axios.put(axios.defaults.baseURL + "/restart")
                    .then(response => {
                        console.log(response.data.result)
                        self.loaded();
                        return true;
                    })
                    .catch(err => {
                        console.log(err);
                        self.loaderror(err);
                    })
                },
                onDeny: function () {
                    self.loaded();
                },
            })
            .modal("show");
        },
        onConfigChanged: function() {
            this.loadData();
        },
        onSaveParameter: function(data) {
            var self = this;
            axios.put(axios.defaults.baseURL + `/config`, {"name" : data["name"], "value" : data["value"]})
            .then(response => {
                self.loadData();
            })
            .catch(err => {
                console.log("error saving parameter: " + err);
            })
        },
    },
}
</script>

<style scoped>
.ui.config.segment {
    color: black;
}
.configmenu {
    padding: 0;
}

.configmenu .right.item {
    padding: 0;
}

.configmenu .right.item .item {
    padding: 0;
}
</style>
