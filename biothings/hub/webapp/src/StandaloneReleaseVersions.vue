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
                          <a class="item" @click="info(version)">
                              <div :class="['hide loader', cssName(version.build_version)]">
                                  <div class="ui active inverted dimmer">
                                      <div class="ui small loader"></div>
                                  </div>
                              </div>
                              <div>
                                  <i class="info circle icon"></i>
                                  Info
                              </div>
                          </a>
                          <a class="item" @click="update()">
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

        <div :class="['ui basic version modal', cssName(name)]">
            <standalone-release-info v-bind:selected_version="selected_version"></standalone-release-info>
        </div>

    </span>
</template>

<script>
import Vue from 'vue'
import bus from './bus.js'
import axios from 'axios'
import Loader from './Loader.vue'
import AsyncCommandLauncher from './AsyncCommandLauncher.vue'
import StandaloneReleaseInfo from './StandaloneReleaseInfo.vue'


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
        $('.ui.basic.modal').remove();
    },
    watch: {
    },
    data () {
        return  {
            error : null,
            versions: [],
            // fetching info
            selected_version: null, // selected version from the table
            info_error: null,
        }
    },
    computed: {
    },
    components: { StandaloneReleaseInfo, },
    methods: {
        cssName: function(what) {
            return what.replace(".","_");
        },
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
        infoLoader: function(version,loading) {
            var build_version = this.cssName(version.build_version);
            if(loading) {
                $(`.loader.${build_version}`).show();
            } else {
                $(`.loader.${build_version}`).hide();
            }
        },
        info: function(version) {
            console.log(version);
            var self = this;
            self.selected_version = null; // reset
            self.error = null;
            var cmd = function() {
                self.infoLoader(version,true);
                self.loading();
                return axios.get(axios.defaults.baseURL + `/standalone/${self.name}/info?version=${version.build_version}`)
            }
            // results[0]: async command can produce multiple results (cmd1() && cmd2), but here we know we'll have only one
            var onSuccess = function(response) {
                self.infoLoader(version,false);
                self.selected_version = response.data.result.results[0];
                $(`.ui.basic.version.modal.${self.cssName(self.name)}`)
                .modal("setting", {
                    detachable : false,
                    closable: false,
                })
                .modal("show");
            }
            var onError = function(err) {
                self.infoLoader(version,false);
                self.loaderror(err);
                self.error = self.extractAsyncError(err);
            }
            this.launchAsyncCommand(cmd,onSuccess,onError)
        },
    }
}
</script>

<style scoped>
.hide {
    display: none;
}
</style>
