<template>
    <div id="builds">

        <div class="ui left vertical labeled icon small inverted sidebar menu">
            <a class="ui dropdown item" v-model="build_configs">
                <i class="big icons">
                    <i class="filter icon"></i>
                </i>
                Filter
                <i class="dropdown icon"></i>
                <div class="ui inverted menu">
                    <div class="item"><i>No filter</i></div>
                    <div class="item" v-for="(conf,name) in build_configs">{{name}}</div>
                    <!--
                    <div class="divider"></div>
                    <div class="ui icon search input">
                        <i class="search icon"></i>
                        <input type="text" name="search" placeholder="Search builds...">
                    </div>
                    <div class="scrolling menu" v-if="builds">
                        <div class="item" v-for="build in builds">
                            {{build.target_name}}
                        </div>
                    </div>
                    -->
                </div>
            </a>
            <a class="item">
                <i class="big icons">
                    <i class="cube icon"></i>
                    <i class="huge corner add icon"></i>
                </i>
                <br>
                <br>
                <div>New build</div>
            </a>
            <a class="item"  v-on:click="createNewConfiguration">
                <i class="big icons">
                    <i class="configure icon"></i>
                    <i class="huge corner add icon"></i>
                </i>
                <br>
                <br>
                <div>New configuration</div>
            </a>
            <a class="item">
                <i class="unhide icon"></i>
                Inspect
            </a>
        </div>
        <div class="pusher">
            <div class="ui main container" id="list_builds">
                <div class="ui segment">
                    <div class="ui secondary small menu">
                        <a class="item" id="side_menu">
                            <i class="sidebar icon"></i>
                            Menu
                        </a>
                    <!--
                        <select name="filter_config" multiple="" class="ui dropdown right aligned item">
                              <option value="">Filter</option>
                              <option :value="name" v-for="(conf,name) in build_configs">{{name}}</option>
                          </select>
                    -->
                    </div>
                </div>
                <div class="ui centered grid">
                    <div class="ui five wide column" v-for="build in builds">
                        <build v-bind:build="build"></build>
                    </div>
                </div>
            </div>
        </div>

        <!-- create new build configuration -->
        <div class="ui basic newconfiguration modal">
            <h3 class="ui icon">
                <i class="configure icon"></i>
                Create a new build configuration
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
        this.getBuilds();
        this.getSourceList();
        this.getBuildConfigs();
        this.interval = setInterval(this.getBuilds,15000);
        $('.ui .dropdown').dropdown();
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
        .sidebar({context:$('#list_builds')})
        .sidebar('setting', 'transition', 'overlay')
        .sidebar('attach events', '#side_menu');
        $('.ui.form').form({
            fields : {
                conf_name : {
                    identifier: 'conf_name',
                    rules: [
                        {
                            type   : 'empty',
                            prompt : 'Please enter your name'
                        }
                    ]
                }
            }
        });
    },
    created() {
        bus.$on('refresh_builds',this.refreshBuilds);
    },
    beforeDestroy() {
        bus.$off('refresh_builds',this.refreshBuilds);
        clearInterval(this.interval);
    },
    data () {
        return  {
            builds: [],
            sources : [],
            build_configs: {},
            errors: [],
        }
    },
    components: { Build, },
    methods: {
        getBuilds: function() {
            axios.get(axios.defaults.baseURL + '/builds')
            .then(response => {
                this.builds = response.data.result;
            })
            .catch(err => {
                console.log("Error getting builds information: " + err);
            })
        },
        getBuildConfigs: function() {
            axios.get(axios.defaults.baseURL + '/build_manager')
            .then(response => {
                this.build_configs = response.data.result;
            })
            .catch(err => {
                console.log("Error getting builds information: " + err);
            })
        },
        getSourceList: function() {
            axios.get(axios.defaults.baseURL + '/sources')
            .then(response => {
                this.sources = response.data.result.map(x => x["_id"]).sort();
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
                    //console.log(`conf_name ${conf_name} selected_sources ${selected_sources} root_sources ${root_sources}`);
                    var params = {};
                    $(optionals.split("\n")).each(function(i,line) {
                        var kv = line.split("=").map(x => x.trim());
                        if(kv.length == 2) {
                            params[kv[0]] = kv[1];
                        } else {
                            this.errors.push("Invalid parameter: " + line);
                        }
                    });
                    if(self.errors.length)
                        return false;
                    axios.post(axios.defaults.baseURL + '/build/configuration',
                               {"name":conf_name, "sources":selected_sources, "roots":root_sources, "params":params})
                    .then(response => {
                        console.log(response.data.result)
                        bus.$emit("refresh_sources");
                        return true;
                    })
                    .catch(err => {
                        console.log(err);
                        console.log("Error registering repository URL: " + err.data.error);
                    })
                }
            })
            .modal("show");
        },
    }
}
</script>

<style>
.ui.sidebar {
    overflow: visible !important;
}
</style>
