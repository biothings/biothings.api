<template>
    <span>
        <div class="ui grid">
            <div class="fourteen wide column">
            </div>
            <div class="two wide column">
                <div class="ui secondary small compact menu right floated">
                    <a class="item" @click="wizard()">
                        <i class="magic icon"></i>
                        Setup
                    </a>
                </div>
            </div>
        </div>
        <div class="ui grid" v-if="version_urls.length">
            <div class="two wide column">
                <div class="ui grey inverted vertical fluid tabular standalone menu">
                    <a :class="['item', i === 0 ? 'active' : '']" :data-tab="src.name" v-for="(src,i) in version_urls" @click="changeTab(src.name)">
                        {{src.name}}
                    </a>
                </div>
            </div>
            <div class="fourteen wide stretched column">
                <div :class="['ui bottom attached tab srctab segment', i === 0 ? 'active' : '']" :data-tab="src.name" v-for="(src,i) in version_urls">
                    <standalone-release v-bind:name="src.name" v-bind:url="src.url"></standalone-release>
                </div>
            </div>
        </div>
    </span>
</template>

<script>
import Vue from 'vue'
import axios from 'axios'
import Loader from './Loader.vue'
import StandaloneRelease from './StandaloneRelease.vue'
import StandaloneWizard from './StandaloneWizard.vue'
import bus from './bus.js'


export default {
	name: 'standalone-releases',
	mixins: [ Loader, ],
	mounted () {
		$('select.dropdown').dropdown();
		this.refresh();
        $('.menu .standalone .item').tab();
	},
	updated() {
	},
	created() {
	},
	beforeDestroy() {
        this.version_urls = [];
	},
	watch: {
	},
	data () {
		return  {
			version_urls: [],
		}
	},
	computed: {
	},
	components: { StandaloneRelease, StandaloneWizard, },
	methods: {
		refresh: function() {
			var self = this;
			this.version_urls = []; // reinit to force components to be rebuilt
			this.loading();
			axios.get(axios.defaults.baseURL + '/standalone/list')
			.then(response => {
				self.version_urls = response.data.result;
				self.loaded();
                if(!self.version_urls.length) {
                    bus.$emit("redirect","wizard");
                }
			})
			.catch(err => {
				console.log("Error getting list of biothings version_urls: " + err);
				self.loaderror(err);
            })
		},
        changeTab: function(tabname) {
            // semantic w/ jquery sometimes is confused with tab init and doesn't react
            // we'll do that ourself...
            $('.ui.standalone.menu').find('.item').tab('change tab', tabname);
        },
        wizard: function() {
            bus.$emit("redirect","wizard");
        }
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
