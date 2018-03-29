<template>
  <div id="app">
    <div class="ui fixed inverted menu">
      <div class="ui container">
        <a href="/" class="header item">
          <img class="logo" src="./assets/biothings_logo.png">
          Biothings Hub
        </a>

        <a class="item">
            <i class="ui home icon"></i>
            <router-link to="/">Home</router-link>
        </a>
        <a class="item">
            <i class="ui database icon"></i>
            <router-link to="/sources">Sources</router-link>
        </a>
        <a class="item">
            <i class="ui cubes icon"></i>
            <router-link to="/builds">Builds</router-link>
        </a>
        <a class="item">
            <i class="ui shield alternate icon"></i>
            <router-link to="/apis">API</router-link>
        </a>

        <div class="ui item right">
          <job-summary></job-summary>
        </div>

        <div class="ui item">
            <div v-if="connected" :data-tooltip="'Connected using ' + socket.protocol" data-position="bottom center">
                <i class="green power off icon"></i>
            </div>
            <div v-else>
                <button class="mini circular ui icon button" @click="setupSocket"
                    data-tooltip="Click to reconnect"
                    data-position="bottom center">
                    <i class="red power off icon"></i>
                </button>
            </div>
        </div>

      </div>
    </div>

    <div id="page_content" class="ui active tab segment">
    <router-view></router-view>
    </div>

  </div>
</template>

<script>
import axios from 'axios';

import VueLocalStorage from 'vue-localstorage';
Vue.use(VueLocalStorage);

import Vue2Filters from 'vue2-filters';
import VueRouter from 'vue-router';
import Vue from 'vue';
Vue.use(Vue2Filters);
Vue.use(require('vue-moment'));
Vue.use(VueRouter)

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

import JobSummary from './JobSummary.vue';
import Stats from './Stats.vue';
import DataSourceGrid from './DataSourceGrid.vue';
import DataSourceDetailed from './DataSourceDetailed.vue';
import BuildGrid from './BuildGrid.vue';
import BuildDetailed from './BuildDetailed.vue';
import ApiGrid from './ApiGrid.vue';

const routes = [
    { path: '/', component: Stats },
    { path: '/sources', component: DataSourceGrid },
    { path: '/source/:_id', component: DataSourceDetailed, props: true },
    { path: '/builds', component: BuildGrid },
    { path: '/build/:_id', component: BuildDetailed, props: true, name: "build"},
    { path: '/apis', component: ApiGrid },
]

const router = new VueRouter({
    routes // short for `routes: routes`
})

export default {
  name: 'app',
  router: router,
  components: { JobSummary },
  mounted () {
      $('.menu .item').tab();
  },
  created () {
      console.log("app created");
      this.setupSocket();
  },
  data() {
      return {
          connected: false,
          socket_msg: '',
          socket : null,
      }
  },
  methods: {
      setupSocket() {
          var self = this;
          var transports = null;//["websocket","xhr-polling"];
          this.socket = new SockJS(axios.defaults.baseURL + '/ws', transports);
          this.socket.onopen = function() {
	          self.connected = true;
          };
          this.socket.onmessage = function (evt) {
              self.socket_msg = evt.data;
          };
          this.socket.onclose = function() {
              self.connected = false;
              self.socket  =null;
          };
          console.log("socket");
          console.log(this.socket);
      },
	  pingServer() {
          console.log("pingServer");
		  // Send the "pingServer" event to the server.
          this.socket.send(JSON.stringify({'pingServer': 'PING!'}));
	  }
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

html,
body,
#page_content {
    min-height: 100%;
    height: 100%;
}

html,
body,
#app {
    min-height: 100%;
    height: 100%;
}

.red {color: #c31616;}

</style>
