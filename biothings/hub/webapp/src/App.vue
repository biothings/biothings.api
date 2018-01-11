<template>
  <div id="app">
    <div class="ui fixed inverted menu">
      <div class="ui container">
        <a href="/" class="header item">
          <img class="logo" src="./assets/biothings_logo.png">
          Biothings Hub
        </a>

        <a class="active item" data-tab="home">
            <i class="ui home icon"></i>
            Home
        </a>
        <a class="item" data-tab="datasources">
            <i class="ui database icon"></i>
            Datasources
        </a>
        <a class="item" data-tab="builds">
            <i class="ui cubes icon"></i>
            Builds
        </a>

        <div class="ui item right">
          <job-summary></job-summary>
        </div>

      </div>
    </div>

    <div class="ui active tab segment" data-tab="home">
        <stats></stats>
    </div>

    <div class="ui tab segment" data-tab="datasources">
        <data-source-grid></data-source-grid>
    </div>

    <div class="ui tab segment" data-tab="builds">
        <build-grid></build-grid>
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

function timesofar(value) {
    let hours =  parseInt(Math.floor(value / 3600));
    let minutes = parseInt(Math.floor((value - (hours * 3600)) / 60));
    let seconds= parseInt((value - ((hours * 3600) + (minutes * 60))) % 60);

    let dHours = (hours > 9 ? hours : '0' + hours);
    let dMins = (minutes > 9 ? minutes : '0' + minutes);
    let dSecs = (seconds > 9 ? seconds : '0' + seconds);

    var res = "";
    if(hours) res += dHours + "h";
    if(minutes) res += dMins + "m";
    if(seconds) res += dSecs + "s";

    return res;
};
Vue.filter('timesofar',timesofar);


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
import BuildGrid from './BuildGrid.vue';
import JobSummary from './JobSummary.vue';
import Stats from './Stats.vue';

export default {
  name: 'app',
  components: { DataSourceGrid, BuildGrid, JobSummary, Stats},
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

table .nowrap {
  white-space:  nowrap;
}

@keyframes pulse {
  0% {transform: scale(1, 1);}
  50% {transform: scale(1.2, 1.2);}
  100% {transform: scale(1, 1);}
}

.pulsing {
  animation: pulse 1s linear infinite;
}

.running { animation: 1s rotate360 infinite linear; }

@keyframes pulse {
  0% {transform: scale(1, 1);}
  50% {transform: scale(1.2, 1.2);}
  100% {transform: scale(1, 1);}
}
.pulsing {
  animation: pulse 1s linear infinite;
}

</style>
