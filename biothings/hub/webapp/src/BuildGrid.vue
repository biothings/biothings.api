<template>
    <div id="builds">

        <div class="ui left vertical labeled icon small inverted sidebar menu">
            <div class="item"><i>Existing configurations</i></div>
            <a class="ui buildconfigs dropdown item" v-for="(conf,conf_name) in build_configs">
                <i class="dropdown icon"></i>
                <div :class="['ui', build_colors[conf_name], 'empty circular label']"></div>
                {{conf_name}}
                <div class="ui inverted menu">
                    <div class="item":conf-name="conf_name" @click="newBuild($event)"><i class="cube icon"></i> Create new build</div>
                    <div class="item":conf-name="conf_name"><i class="edit outline icon"></i> Edit configuration</div>
                    <div class="item" :conf-name="conf_name" @click="deleteConfiguration($event)"><i class="trash alternate outline icon"></i> Delete configuration</div>
                </div>
            </a>
            <div class="item"><i>Other actions</i></div>
            <a class="item"  v-on:click="createNewConfiguration">
                <i class="big icons">
                    <i class="configure icon"></i>
                    <i class="huge corner add icon"></i>
                </i>
                <br>
                <br>
                <div>New configuration</div>
            </a>
        </div>
        <div class="pusher">
            <div class="ui main container">
                <div class="ui segment">
                    <div class="ui secondary small menu">
                        <a class="item" id="side_menu">
                            <i class="sidebar icon"></i>
                            Menu
                        </a>
                        <a class="right aligned item">
                            <button class="ui clearconffilter button" v-if="conf_filter" style="margin-right:1em;" @click="clearFilter">
                                Clear 
                            </button>
                            <select class="ui filterbuilds dropdown" v-model="conf_filter">
                                <option value="">Filter</option>
                                <option :value="name" v-for="(conf,name) in build_configs">
                                    <div :class="['ui', build_colors[name], 'empty circular label']"></div>
                                    {{name}}
                                </option>
                            </select>
                          </a>
                      </div>
                </div>
                <div class="ui centered grid">
                    <div class="ui five wide column" v-for="build in builds">
                        <build v-bind:build="build" v-bind:color="build_colors[build.build_config.name]"></build>
                    </div>
                </div>
            </div>
        </div>

        <!-- create new build configuration -->
        <div class="ui basic newconfiguration modal">
            <h3 class="ui icon">
                <i class="configure icon"></i>
                Create/edit build configuration
            </h3>
            <div class="content">
                <div class="ui form">
                    <div class="ui centered grid">
                        <div class="eight wide column">

                            <label>Enter a build configuration name</label>
                            <input type="text" id="conf_name" placeholder="Configuration name" autofocus>
                            <br>
                            <br>

                            <label>Select the sources used to create merged data</label>
                            <select class="ui fluid sources dropdown" id="selected_sources" multiple="">
                                <option value="">Available sources</option>
                                <option v-for="_id in sources">{{_id}}</option>
                            </select>
                            <br>

                            <label>Once sources are selected, choose sources providing root documents</label>
                            <select class="ui fluid rootsources dropdown" multiple="">
                                <option value="">Root sources</option>
                            </select>
                            <br>


                        </div>

                        <div class="eight wide column">

                            <p>Optional key=value pairs can be added to the configuration (usefull to customize builder behavior). Enter one per line.</p>
                            <textarea id="optionals"></textarea>

                            <div class="ui teal message">
                                <b>Note</b>: sources providing root documents, or <i>root sources</i>, are sources allowed to
                                create a new document in a build (merged data). If a root source is declared, data from other sources will only be merged <b>if</b>
                                documents previously exist with same IDs (documents coming from root sources). If not, data is discarded. Finally, if no root source
                                is declared, any data sources can generate a new document in the merged data.
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <div class="ui error message" v-if="errors.length">
                <ul class="ui list">
                    <li v-for="err in errors">{{err}}</li>
                </ul>
            </div>

            <div class="actions">
                <div class="ui red basic cancel inverted button">
                    <i class="remove icon"></i>
                    Cancel
                </div>
                <div class="ui green ok inverted button">
                    <i class="checkmark icon"></i>
                    OK
                </div>
            </div>
        </div>

        <div class="ui basic deleteconf modal">
            <div class="ui icon header">
                <i class="trash alternate icon"></i>
                Delete configuration
            </div>
            <div class="content">
                <p>Are you sure you want to delete this build configuration ?</p>
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

        <div class="ui basic newbuild modal">
            <div class="ui icon header">
                <i class="cube icon"></i>
                Create new build
            </div>
            <div class="content">
                <p>Enter a name for the merged data collection or leave it empty to generate a random one</p>
            </div>
            <div class="actions">
                <div class="ui red basic cancel inverted button">
                    <i class="remove icon"></i>
                    Cancel
                </div>
                <div class="ui green ok inverted button">
                    <i class="checkmark icon"></i>
                    OK
                </div>
            </div>
        </div>

    </div>


</template>

<script>
import axios from 'axios'
import Build from './Build.vue'
import bus from './bus.js'


export default {
    name: 'build-grid',
    mounted () {
        console.log("BuildGrid mounted");
        $('.ui.dropdown').dropdown();
        $('.ui.sources.dropdown').dropdown({
            onChange: function(addedValue, addedText, $addedChoice) {
                console.log(addedValue);console.log(addedText);
                var selected_sources = $('.ui.sources.dropdown').dropdown('get value');
                var fmt = []
                for(var i in selected_sources) {
                    var x = selected_sources[i];
                    var d = {"name":x,"text":x,"value":x};
                    fmt.push(d);
                }
                $('.ui.rootsources.dropdown').dropdown("setup menu",{"values" : fmt}).dropdown("refresh");
            },
        });
        $('#builds .ui.sidebar')
        .sidebar({context:$('#builds')})
        .sidebar('setting', 'transition', 'overlay')
        .sidebar('attach events', '#side_menu');
        $('.ui.form').form();
    },
    updated() {
        // there's some kind of race-condition regarding dropdown init, if
        // in mounted() they won't get init, prob. because data changed and needs to
        // be re-rendered
        $('.ui.buildconfigs.dropdown').dropdown();
    },
    created() {
        this.getBuilds();
        this.getSourceList();
        this.getBuildConfigs();
        this.interval = setInterval(this.getBuilds,15000);
        bus.$on('refresh_builds',this.refreshBuilds);
    },
    beforeDestroy() {
        bus.$off('refresh_builds',this.refreshBuilds);
        clearInterval(this.interval);
        // hacky to remove modal from div outside of app, preventing having more than one
        // modal displayed when getting back to that page. https://github.com/Semantic-Org/Semantic-UI/issues/4049
        $('.ui.basic.deleteconf.modal').remove();
        $('.ui.basic.newbuild.modal').remove();
        $('.ui.basic.newconfiguration.modal').remove();
    },
    watch: {
        conf_filter: function(newv,oldv) {
            if(newv != oldv) {
                this.getBuilds();
            }
        },
    },
    data () {
        return  {
            builds: [],
            sources : [],
            build_configs: {},
            errors: [],
            build_colors : {},
            // build colors for easy visual id
            colors: ["orange","green","yellow","olive","teal","violet","blue","pink","purple"],
            color_idx : 0,
            conf_filter : "",
        }
    },
    components: { Build, },
    methods: {
        getBuilds: function() {
            var filter = this.conf_filter == "" ? '' : `?build_config=${this.conf_filter}`;
            axios.get(axios.defaults.baseURL + '/builds' + filter)
            .then(response => {
                this.builds = response.data.result;
            })
            .catch(err => {
                console.log("Error getting builds information: " + err);
            })
        },
        getBuildConfigs: function() {
            var self = this;
            self.color_idx = 0;
            axios.get(axios.defaults.baseURL + '/build_manager')
            .then(response => {
                self.build_configs = response.data.result;
                // make sure we always give the same color for a given build config
                var keys = Object.keys(self.build_configs);
                console.log(keys);
                keys.sort();
                for(var k in keys) {
                    console.log(keys[k]);
                    self.build_colors[keys[k]] = self.colors[self.color_idx++];
                    if(self.color_idx + 1 >= Object.keys(self.colors).length) {
                        self.color_idx = 0;
                    }
                }
            })
            .catch(err => {
                console.log("Error getting builds information: " + err);
            })
        },
        getSourceList: function() {
            var self = this;
            axios.get(axios.defaults.baseURL + '/sources')
            .then(response => {
                $(response.data.result).each(function(i,e) {
                    for(var k in e["upload"]["sources"]) {
                        self.sources.push(k);
                    }
                });
                self.sources.sort();
            })
            .catch(err => {
                console.log("Error listing sources: " + err);
            })
        },
        refreshBuilds: function() {
            console.log("Refreshing builds");
            this.getBuilds();
        },
        showBuildConfig: function(event) {
            var confname = $(event.currentTarget).html().trim();
            console.log(this.build_configs[confname]);
        },
        createNewConfiguration: function() {
            var self = this;
            $('.ui.basic.newconfiguration.modal')
            .modal("setting", {
                detachable : false,
                onApprove: function () {
                    self.errors = [];
                    var conf_name = $(".ui.form").form('get field', "conf_name").val();
                    var selected_sources = $(".ui.form").form('get field', "selected_sources").val();
                    var optionals = $(".ui.form").form('get field','optionals').val();
                    var root_sources = [];
                    // form valid
                    if(!conf_name)
                        self.errors.push("Provide a configuration name");
                    if(selected_sources.length == 0)
                        self.errors.push("Select at least one source to build merged data");
                    // semantic won't populate select.option when dynamically set values, but rather add "q" elements, 
                    // despite the use of refresh. How ugly...
                    $(".ui.rootsources.dropdown a").each(function(i,e) {root_sources.push($(e).text())});
                    var params = {};
                    $(optionals.split("\n")).each(function(i,line) {
                        if(line) {
                            var kv = line.split("=").map(x => x.trim());
                            if(kv.length == 2) {
                                params[kv[0]] = kv[1];
                            } else {
                                self.errors.push("Invalid parameter: " + line);
                            }
                        }
                    });
                    if(self.errors.length)
                        return false;
                    axios.post(axios.defaults.baseURL + '/buildconf',
                               {"name":conf_name, "sources":selected_sources, "roots":root_sources, "params":params})
                    .then(response => {
                        console.log(response.data.result)
                        self.getBuildConfigs();
                        self.refreshBuilds();
                        return true;
                    })
                    .catch(err => {
                        console.log(err);
                        console.log("Error creating build configuration: " + err.data.error);
                    })
                }
            })
            .modal("show");
        },
        deleteConfiguration : function(event) {
            var conf_name = $(event.currentTarget).attr("conf-name");
            var self = this;
            $('.ui.basic.deleteconf.modal')
            .modal("setting", {
                detachable : false,
                onApprove: function () {
                    axios.delete(axios.defaults.baseURL + '/buildconf',{"data":{"name":conf_name}})
                    .then(response => {
                        console.log(response.data.result)
                        self.getBuildConfigs();
                        self.refreshBuilds();
                        return true;
                    })
                    .catch(err => {
                        console.log(err);
                        console.log("Error deleting configuration: " + err ? err.data.error : 'unknown error');
                    })
                },
            })
            .modal("show");
        },
        newBuild : function(event) {
            var conf_name = $(event.currentTarget).attr("conf-name");
            var target_name = null; // todo: take it from form
            var self = this;
            $('.ui.basic.newbuild.modal')
            .modal("setting", {
                detachable : false,
                onApprove: function () {
                    axios.put(axios.defaults.baseURL + `/build/${conf_name}/new`,{"target_name":target_name})
                    .then(response => {
                        console.log(response.data.result)
                        self.getBuilds();
                        return true;
                    })
                    .catch(err => {
                        console.log(err);
                        console.log("Error lauching new build: " + err ? err.data.error : 'unknown error');
                    })
                },
            })
            .modal("show");
        },
        clearFilter : function() {
            console.log("onela");
            $('.ui.filterbuilds.dropdown')
            .dropdown('clear');
            this.conf_filter = "";
            this.getBuilds();
        },
    }
}
</script>

<style>
.ui.sidebar {
    overflow: visible !important;
}
</style>
