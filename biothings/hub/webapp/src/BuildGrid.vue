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
            <a class="item">
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
        this.getBuildConfigs();
        this.interval = setInterval(this.getBuilds,15000);
        $('.ui .dropdown')
        .dropdown()
        ;
        $('#builds .ui.sidebar')
        .sidebar({context:$('#list_builds')})
        .sidebar('setting', 'transition', 'overlay')
        .sidebar('attach events', '#side_menu');
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
        refreshBuilds: function() {
            console.log("Refreshing builds");
            this.getBuilds();
        },
        showBuildConfig: function(event) {
            var confname = $(event.currentTarget).html().trim();
            console.log(this.build_configs[confname]);
        },
    }
}
</script>

<style>
.ui.sidebar {
    overflow: visible !important;
}
</style>
