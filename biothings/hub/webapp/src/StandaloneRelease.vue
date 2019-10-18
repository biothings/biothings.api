<template>
	<span>
		<h1 class="ui header">{{name}}</h1>
		<div class="ui secondary small compact menu">
			<a class="item" @click="refresh()">
				<i class="sync icon"></i>
				Refresh
			</a>
			<a class="item">
				<a :href="url">versions.json</a>
			</a>
		</div>
        <br>
        <br>
		<div class="ui grid">
			<div class="twelve wide column">
				<standalone-release-versions v-bind:name="name"></standalone-release-versions>
			</div>
			<div class="four wide column">
                <div class="ui tiny negative message" v-if="backend_error">
                    <div class="header">Unable to load backend information</div>
                    <p>{{backend_error}}</p>
                </div>
                <div class="item" v-else>
                    <div class="ui list">
                        <div class="item">
                            <i class="database icon"></i>
                            <div class="content">
                                <div class="header">ElasticSearch host</div>
                                    <a :href="backend.host"> {{backend.host}}</a>
                            </div>
                        </div>
                        <div class="item">
                            <i class="bookmark icon"></i>
                            <div class="content">
                                <div class="header">Index</div>
                                {{backend.index}}
                            </div>
                        </div>
                        <div class="item">
                            <i class="thumbtack icon"></i>
                            <div class="content">
                                <div class="header">Version</div>
                                {{backend.version || "no version found"}}</a>
                            </div>
                        </div>
                        <div class="item">
                            <i class="file alternate icon"></i>
                            <div class="content">
                                <div class="header">Documents</div>
                                {{ backend.count | formatInteger }}
                            </div>
                        </div>
                    </div>
                </div>
            </div>
		</div>
	</span>
</template>

<script>
import Vue from 'vue'
import axios from 'axios'
import Loader from './Loader.vue'
import AsyncCommandLauncher from './AsyncCommandLauncher.vue'
import StandaloneReleaseVersions from './StandaloneReleaseVersions.vue'
import bus from './bus.js'


export default {
    name: 'standalone-release',
    props: ['name', 'url'],
    mixins: [ AsyncCommandLauncher, Loader, ],
    mounted () {
        this.refresh();
    },
    updated() {
    },
    created() {
    },
    beforeDestroy() {
    },
    watch: {
    },
    data () {
        return  {
			backend : {},
            backend_error : null,
        }
    },
    computed: {
    },
    components: { StandaloneReleaseVersions, },
    methods: {
        refresh: function() {
			this.refreshBackend();
            bus.$emit("refresh_standalone",this.name);
        },
        refreshBackend: function() {
            var self = this;
            self.backend_error = null;
            var cmd = function() { self.loading(); return axios.get(axios.defaults.baseURL + `/standalone/${self.name}/backend`) }
			// results[0]: async command can produce multiple results (cmd1() && cmd2), but here we know we'll have only one
            var onSuccess = function(response) { self.backend = response.data.result.results[0]; }
            var onError = function(err) { console.log(err); self.loaderror(err); self.backend_error = self.extractAsyncError(err);}
            this.launchAsyncCommand(cmd,onSuccess,onError)
		},

    }
}
</script>

<style scoped>
.ui.sidebar {
    overflow: visible !important;
}
.srctab {
	border-color:rgb(212, 212, 213) !important;
	border-style:solid !important;
	border-width:1px !important;
	border-radius: 0px !important;
}
</style>
