<template>
  <div id="app">
    <div class="ui fixed inverted menu">
      <div class="ui container">
        <a href="/" class="header item">
          <img class="logo" src="./assets/biothings_logo.png">
          Biothings Hub
        </a>

        <a class="active item" data-tab="home">
            Home
        </a>
        <a class="item" data-tab="datasources">
            Datasources
        </a>
        <a class="item" data-tab="builds">
            Builds
        </a>

        <div class="ui item right">
          <job-summary></job-summary>
        </div>

      </div>
    </div>

    <div class="ui active tab segment" data-tab="home">
        <h2 class="ui header">Home</h2>
        <stats></stats>
    </div>

    <div class="ui tab segment" data-tab="datasources">
        <h2 class="ui header">Datasources</h2>
        <data-source-grid></data-source-grid>
    </div>

    <div class="ui tab segment" data-tab="builds">
        <h2 class="ui header">Builds</h2>
    </div>

  </div>
</template>

<script>
import axios from 'axios';

import VueLocalStorage from 'vue-localstorage';
Vue.use(VueLocalStorage);

import Vue2Filters from 'vue2-filters';
import Vue from 'vue';
Vue.use(Vue2Filters);
Vue.use(require('vue-moment'));


var UNITS = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
var STEP = 1024;


function pretty_size(bytes,precision=2) {
	var units = [
		'bytes',
		'KB',
		'MB',
		'GB',
		'TB',
		'PB'
	];

	if ( isNaN( parseFloat( bytes )) || ! isFinite( bytes ) ) {
		return '?';
	}

	var unit = 0;

	while ( bytes >= 1024 ) {
		bytes /= 1024;
		unit ++;
	}

	return bytes.toFixed( + precision ) + ' ' + units[ unit ];
};
Vue.filter('pretty_size',pretty_size);

function split_and_join(str,sep="_",glue=" ") {
    return str.split(sep).join(" ");
}
Vue.filter('splitjoin',split_and_join);

import DataSourceGrid from './DataSourceGrid.vue';
import Stats from './Stats.vue';
import JobSummary from './JobSummary.vue';

export default {
  name: 'app',
  components: { DataSourceGrid, JobSummary, Stats},
  mounted () {
      $('.menu .item').tab({
          history: true,
          historyType: 'hash'
      });
  },
  methods: {
  }
}
</script>

<style>
#app {
  font-family: 'Avenir', Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
  margin-top: 60px;
}

h1, h2 {
  font-weight: normal;
}

ul {
  list-style-type: none;
  padding: 0;
}

li {
  display: inline-block;
  margin: 0 10px;
}

a {
  color: #42b983;
}
</style>
