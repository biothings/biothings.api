<template>
    <span>
        <div v-if="versions.length || error">
			<div class="ui tiny negative message" v-if="error">
				<div class="header">Unable to load versions</div>
                <p>{{error}}</p>
            </div>
			<table class="ui compact versions table">
			  <thead>
			    <tr>
			      <th>Version</th>
			      <th>Release date</th>
			      <th>Type</th>
			      <th>Requires</th>
			      <th>Action</th>
			    </tr>
			  </thead>
			  <tbody>
			    <tr v-for="version in versions">
			      <td>{{version.build_version}}</td>
                  <td>{{ moment(version.release_date).format('MMMM Do YYYY, h:mm:ss a')}}</td>
			      <td><i :class="['ui ', version.type == 'full' ? 'blue bookmark' : 'orange exchange alternate','icon']"></i>{{version.type}}</td>
			      <td>{{version.require_version || "-"}}</td>
                  <td>
                      <div class="ui tiny compact menu">
                          <a class="item" @click="info()">
                              <i class="info circle icon"></i>
                              Info
                          </a>
                          <a class="item" @click="info()">
                              <i class="download icon"></i>
                              Update
                          </a>
                      </div>
                  </td>
			    </tr>
			  </tbody>
			</table>

        </div>
        <div v-else>
				<br><br>
            <div class="ui active inverted dimmer">
                <div class="ui text loader"></div>
            </div>
        </div>
    </span>
</template>

<script>
import Vue from 'vue'
import bus from './bus.js'
import axios from 'axios'
import Loader from './Loader.vue'
import AsyncCommandLauncher from './AsyncCommandLauncher.vue'


export default {
    name: 'standalone-release-versions',
    props: ['name'],
    mixins: [ AsyncCommandLauncher, Loader, ],
    mounted () {
        this.refreshVersions();
    },
    updated() {
    },
    created() {
        bus.$on("refresh_standalone",this.onRefresh);
    },
    beforeDestroy() {
        bus.$off("refresh_standalone",this.onRefresh);
    },
    watch: {
    },
    data () {
        return  {
			error : null,
			versions: [],
        }
    },
    computed: {
    },
    components: { },
    methods: {
        onRefresh: function(name) {
            if(name == this.name) {
                this.versions = [];
                this.refreshVersions();
            }
        },
        refreshVersions: function() {
            var self = this;
            self.error = null;
            var cmd = function() {
                self.loading();
                return axios.get(axios.defaults.baseURL + `/standalone/${self.name}/versions`)
            }
			// results[0]: async command can produce multiple results (cmd1() && cmd2), but here we know we'll have only one
            var onSuccess = function(response) {
                self.versions = response.data.result.results[0].reverse();
            }
            var onError = function(err) {
                console.log("error getting versions");
				console.log(err);
                self.loaderror(err);
                self.error = self.extractAsyncError(err);
            }
            this.launchAsyncCommand(cmd,onSuccess,onError)
		},
    }
}
</script>

<style scoped>
.tinytiny {
    padding: .5em 1em .5em;
    font-size: .6em;
}
</style>
