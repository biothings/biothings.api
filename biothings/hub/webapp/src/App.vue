<template>
  <div id="app">
    <div class="ui fixed inverted menu">
      <div class="ui container">
        <a href="#" class="header item">
          <img class="logo" src="./assets/biothings_logo.png">
          Biothings Hub
        </a>

        <div class="ui item right">
          <job-summary></job-summary>
        </div>

      </div>
    </div>

    <div class="ui styled fluid accordion">
        <div class="title active">
            Statistics
        </div>
        <div class="content active">
            <stats></stats>
        </div>
        <div class="title">
            Datasources
        </div>
        <div class="content">
            <data-source-grid></data-source-grid>
        </div>
        <div class="title">
            Builds
        </div>
        <div class="content">
            Build
        </div>
    </div>

  </div>
</template>

<script>
import axios from 'axios';

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
      $('.ui.accordion')
        .accordion()
        ;
  },
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
