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
                    <!--div class="item":conf-name="conf_name"><i class="edit outline icon"></i> Edit configuration</div-->
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
                            <button class="ui clearconffilter button" v-if="conf_filter" @click="clearFilter">
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
                        <build v-bind:pbuild="build" v-bind:color="build_colors[build.build_config.name]"></build>
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
                <div class="ui newconfiguration form">
                    <div class="ui centered grid">
                        <div class="eight wide column">

                            <label>Enter a build configuration name</label>
                            <input type="text" id="conf_name" placeholder="Configuration name" autofocus>
                            <br>
                            <br>

                            <label>Enter a name for the type of stored documents ("gene", "variant", ...)</label>
                            <input type="text" id="doc_type" placeholder="Document type" autofocus>
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

                            <p>Optional parameters can be added to the configuration (usefull to customize builder behavior). Enter a JSON object structure</p>
                            <textarea id="optionals">{}</textarea>

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
            <div class="ui newbuild form">
                <div class="ui centered grid">
                    <div class="ten wide column">
                        <p>Enter a name for the merged data collection or leave it empty to generate a random one</p>
                        <input type="text" id="target_name" placeholder="Collection name" autofocus>
                    </div>
                    <div class="six wide column">
                    </div>
                </div>
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
import Loader from './Loader.vue'
import bus from './bus.js'


export default {
    name: 'build-grid',
    mixins: [ Loader, ],
    mounted () {
        console.log("BuildGrid mounted");
        $('.ui.filterbuilds.dropdown').dropdown();
        $('.ui.buildconfigs.dropdown').dropdown();
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
        // load sources to build dropdown list when creating a new config
        this.loading();
        this.getSourceList();
        // builds & configs
        this.getBuildConfigs();
        this.getBuilds();
        bus.$on('change_source',this.onSourceChanged);
        bus.$on('change_build',this.onBuildChanged);
        bus.$on('change_build_config',this.onBuildConfigChanged);
    },
    beforeDestroy() {
        clearInterval(this.interval);
        // hacky to remove modal from div outside of app, preventing having more than one
        // modal displayed when getting back to that page. https://github.com/Semantic-Org/Semantic-UI/issues/4049
        $('.ui.basic.deleteconf.modal').remove();
        $('.ui.basic.newbuild.modal').remove();
        $('.ui.basic.newconfiguration.modal').remove();
        bus.$off('change_source',this.onSourceChanged);
        bus.$off('change_build',this.onBuildChanged);
        bus.$off('change_build_config',this.onBuildConfigChanged);
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
            var filter = this.conf_filter == "" ? '' : `?conf_name=${this.conf_filter}`;
            // https://vuejs.org/v2/guide/list.html#Caveats:
            // "Vue implements some smart heuristics to maximize DOM element reuse, so replacing an 
            //  array with another array containing overlapping objects is a very efficient operation."
            // Well, if only one element changes, like a deleted build, it seems that smart heuristic
            // is somehow dumb enough to partially render the list, so we need to empty that list here
            // (and if emptied in "response", I guess there's a race condition because builds aren't
            // rendered properly again...). Anyway, I don't know if it's related but that's the only
            // explanation I have...
            this.loading();
            this.builds = [];
            axios.get(axios.defaults.baseURL + '/builds' + filter)
            .then(response => {
                this.builds = response.data.result;
                this.loaded();
            })
            .catch(err => {
                console.log("Error getting builds information: " + err);
                this.loaderror(err);
            })
        },
        getBuildConfigs: function() {
            var self = this;
            // reset colors (if configs haven't changed we should get the same colors
            // as we sort the build config by name)
            self.color_idx = 0;
            self.build_colors = {};
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
        buildExists: function(_id) {
            var gotit = false;
            var self = this;
            $(this.builds).each(i => {
                if(self.builds[i]["_id"] == _id) {
                    gotit = true;
                    return false;
                }
            });
            return gotit;
        },
        onSourceChanged: function(_id=null, op=null) {
            // reload all of them
            this.getSourceList();
        },
        onBuildChanged: function(_id=null, op=null) {
            //console.log(`_id ${_id} op ${op}`);
            if(_id == null) {
                console.log("Refreshing builds");
                this.getBuilds();
            } else {
                if(this.buildExists(_id)) {
                    // there's an ID for an existing build, propagate
                    //console.log(`emit build_updated for ${_id}`);
                    bus.$emit("build_updated",_id,op);
                } else {
                    //console.log(`_id ${_id} doesn't exist, get all`);
                    this.getBuilds();
                }
            }
        },
        onBuildConfigChanged: function(_id=null, op=null) {
            // reload all of them
            this.getBuildConfigs();
            if(op == "remove") {
                // refresh builds to assign new colors
                this.getBuilds();
            }
        },
        showBuildConfig: function(event) {
            var confname = $(event.currentTarget).html().trim();
            console.log(this.build_configs[confname]);
        },
        createNewConfiguration: function() {
            // force close sidebar
            $('#builds .ui.sidebar').sidebar("hide");
            var self = this;
            $('.ui.basic.newconfiguration.modal')
            .modal("setting", {
                detachable : false,
                onApprove: function () {
                    self.errors = [];
                    var conf_name = $(".ui.newconfiguration.form").form('get field', "conf_name").val();
                    var doc_type = $(".ui.newconfiguration.form").form('get field', "doc_type").val();
                    var selected_sources = $(".ui.newconfiguration.form").form('get field', "selected_sources").val();
                    console.log(optionals);
                    var root_sources = [];
                    // form valid
                    if(!conf_name)
                        self.errors.push("Provide a configuration name");
                    if(!doc_type)
                        self.errors.push("Provide a document type");
                    if(selected_sources.length == 0)
                        self.errors.push("Select at least one source to build merged data");
                    // semantic won't populate select.option when dynamically set values, but rather add "q" elements, 
                    // despite the use of refresh. How ugly...
                    $(".ui.rootsources.dropdown a").each(function(i,e) {root_sources.push($(e).text())});
                    //var params = {};
                    //$(optionals.split("\n")).each(function(i,line) {
                    //    if(line) {
                    //        var kv = line.split("=").map(x => x.trim());
                    //        if(kv.length == 2) {
                    //            params[kv[0]] = kv[1];
                    //        } else {
                    //            self.errors.push("Invalid parameter: " + line);
                    //        }
                    //    }
                    //});
                    var optionals = {};
                    try {
                        optionals = JSON.parse($(".ui.newconfiguration.form").form('get field','optionals').val());
                    } catch(e) {
                        self.errors.push("Invalid optional parameter: " + e);
                    }
                    if(self.errors.length)
                        return false;
                    axios.post(axios.defaults.baseURL + '/buildconf',
                               {"name":conf_name,
                                "doc_type": doc_type,
                                "sources":selected_sources,
                                "roots":root_sources,
                                "params":optionals})
                    .then(response => {
                        console.log(response.data.result)
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
            // force close sidebar
            $('#builds .ui.sidebar').sidebar("hide");
            var conf_name = $(event.currentTarget).attr("conf-name");
            var self = this;
            $('.ui.basic.deleteconf.modal')
            .modal("setting", {
                detachable : false,
                onApprove: function () {
                    axios.delete(axios.defaults.baseURL + '/buildconf',{"data":{"name":conf_name}})
                    .then(response => {
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
            // force close sidebar
            $('#builds .ui.sidebar').sidebar("hide");
            var conf_name = $(event.currentTarget).attr("conf-name");
            var self = this;
            $('.ui.basic.newbuild.modal')
            .modal("setting", {
                detachable : false,
                onApprove: function () {
                    var target_name = $(".ui.newbuild.modal #target_name").val();
                    if(target_name == "")
                        target_name = null;
                    axios.put(axios.defaults.baseURL + `/build/${conf_name}/new`,{"target_name":target_name})
                    .then(response => {
                        console.log(response.data.result)
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
.clearconffilter {
    margin-right:1em !important;
}
</style>
