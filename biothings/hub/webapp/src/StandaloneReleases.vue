<template>
    <span>
        <div class="ui grid">
            <div class="fourteen wide column">
            </div>
            <div class="two wide column">
                <div class="ui secondary small compact menu">
                    <a class="item">
                        <i class="rss icon"></i>
                        Add
                    </a>
                </div>
            </div>
        </div>
        <div class="ui grid">
            <div class="two wide column">
                <div class="ui grey inverted vertical fluid tabular standalone menu">
                    <a :class="['item', i === 0 ? 'active' : '']" :data-tab="src.name" v-for="(src,i) in sources" @click="changeTab(src.name)">
                        {{src.name}}
                    </a>
                </div>
            </div>
            <div class="fourteen wide stretched column">
                <div :class="['ui bottom attached tab srctab segment', i === 0 ? 'active' : '']" :data-tab="src.name" v-for="(src,i) in sources">
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
        this.sources = [];
	},
	watch: {
	},
	data () {
		return  {
			sources : [],
		}
	},
	computed: {
	},
	components: { StandaloneRelease, },
	methods: {
		refresh: function() {
			var self = this;
			this.sources = []; // reinit to force components to be rebuilt
			this.loading();
			axios.get(axios.defaults.baseURL + '/standalone/list')
			.then(response => {
				self.sources = response.data.result;
				self.loaded();
			})
			.catch(err => {
				console.log("Error getting list of biothings sources: " + err);
				self.loaderror(err);
            })
		},
        changeTab: function(tabname) {
            // semantic w/ jquery sometimes is confused with tab init and doesn't react
            // we'll do that ourself...
            $('.ui.standalone.menu').find('.item').tab('change tab', tabname);
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
