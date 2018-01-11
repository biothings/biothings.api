<template>
    <div class="ui main container" id="list_build">
        <div class="ui segment right aligned">
            <label v-if="Object.keys(build_configs).length">Build configurations:</label>
            <div class="ui icon buttons left">
                <button class="ui button" v-for="(conf,name) in build_configs" @click="showBuildConfig($event)">
                    {{name}}
                </button>
            </div>
            <div class="ui icon buttons right ">
                <button class="ui button">
                    <i class="add square icon"></i>
                </button>
            </div>
        </div>
        <div class="ui grid">
            <div class="four wide column" v-for="build in builds">
                <build v-bind:build="build"></build>
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
    setInterval(this.getBuilds,15000);
  },
  created() {
    bus.$on('refresh_build',this.refreshBuilds);
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
      this.getBuildsStatus();
    },
    showBuildConfig: function(event) {
        var confname = $(event.currentTarget).html().trim();
        console.log(this.build_configs[confname]);
    },
  }
}
</script>

<style>
</style>
