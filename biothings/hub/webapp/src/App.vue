<template>
  <div id="app">
    <div class="ui fixed inverted menu">
      <div class="ui container">
        <a href="/" class="header item">
          <img class="logo" src="./assets/biothings_logo.png">
          Biothings Hub
        </a>

        <a class="clickable blurred item">
            <i class="ui home icon"></i>
            <router-link to="/">Home</router-link>
        </a>
        <a class="clickable blurred item">
            <i class="ui database icon"></i>
            <router-link to="/sources">Sources</router-link>
        </a>
        <a class="clickable blurred item">
            <i class="ui cubes icon"></i>
            <router-link to="/builds">Builds</router-link>
        </a>
        <a class="clickable blurred item">
            <i class="ui shield alternate icon"></i>
            <router-link to="/apis">API</router-link>
        </a>

        <div class="clickable blurred ui item right">
          <job-summary></job-summary>
        </div>

        <div class="ui item">
            <div v-if="socket && socket.readyState == 1" :data-tooltip="'Connection: ' + socket.protocol" data-position="bottom center">
                <button class="mini circular ui icon button" @click="closeSocket">
                    <i class="green power off icon"></i>
                </button>
            </div>
            <div v-else>
                <button class="mini circular ui icon button" @click="setupSocket"
                    data-tooltip="Click to reconnect"
                    data-position="bottom center">
                    <i class="red plug icon"></i>
                </button>
            </div>
            <div id="connected" v-if="socket && socket.readyState == 1" :data-tooltip="'Quality: unknown'" data-position="bottom center">
                <i class="inverted circular signal icon"></i>
            </div>
        </div>

      </div>
    </div>

    <div id="page_content" class="clickable blurred ui active tab segment">
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

import bus from './bus.js';

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

const PING_INTERVAL_MS = 10000;

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
  beforeDestroy() {
  },
  data() {
    return {
      connected: false,
      socket_msg: '',
      socket : null,
      msg_timestamp : null,
      latency_value : null,
    }
  },
  computed : {
  },
  watch: {
    latency_value: function (newv, oldv) {
      if(newv != oldv) {
        //console.log(`new: ${newv} old: ${oldv}`);
        this.evalLatency(oldv,newv);
      }
    }
  },
  methods: {
    dispatchMessage(msg) {
      if(msg.obj) {
        var event = `change_${msg.obj}`;
        console.log(`emit ${event} (${msg._id}): ${msg.op}`);
        bus.$emit(event,msg._id,msg.op);
      }
    },
    evalLatency : function(oldv,newv) {
      var info = {}
      function getInfo(val) {
        // depending on websocket latency, adjust color and text info
        if(val == null) {
          info["color"] = "grey";
          info["quality"] = "unknown";
        } else if(val > 0 && val <= 20) {
          info["color"] = "green";
          info["quality"] = "excellent";
        } else if(val > 20 && val <= 30) {
          info["color"] = "olive";
          info["quality"] = "good";
        } else if(val > 30 && val <= 50) {
          info["color"] = "yellow";
          info["quality"] = "average";
        } else if(val > 50 && val <= 100) {
          info["color"] = "orange";
          info["quality"] = "poor";
        } else if(val > 100) {
          info["color"] = "red";
          info["quality"] = "very poor";
        } else {
          info["color"] = "brown";
          info["quality"] = "???";
        }
        return info;
      }
      var oldinfo = getInfo(oldv);
      var newinfo = getInfo(newv);
      $("#connected i").removeClass("grey brown red orange yellow olive green").addClass(newinfo.color);
      $("#connected").attr("data-tooltip",'Quality: ' + newinfo.quality);
    },
    setupSocket() {
      var self = this;
      var transports = null;//["websocket","xhr-polling"];
      // re-init timestamp so we can monitor it again
      this.msg_timestamp = null;
      // first check we can access a websocket
      axios.get(axios.defaults.baseURL + '/ws/info')
      .then(response => {
        console.log("WebSocket available");
        this.socket = new SockJS(axios.defaults.baseURL + '/ws', transports);
        this.socket.onopen = function() {
          self.connected = true;
          self.pingServer();
          $(".clickable").removeClass("blurred");
        };
        this.socket.onmessage = function (evt) {
          var newts = Date.now();
          self.latency_value = newts - self.msg_timestamp;
          self.socket_msg = evt.data;
          self.dispatchMessage(evt.data);
          self.msg_timestamp = null;
        };
        this.socket.onclose = function() {
          self.closeSocket();
        },
        this.socket.ontimeout = function(err) {
          console.log("got error");
          console.log(err);
        }

      })
      .catch(err => {
        console.log("Can't connect using websocket");
        console.log(err);
      });
    },
    closeSocket() {
      this.connected = false;
      this.socket.close();
      this.msg_timestamp = null;
      $(".clickable").addClass("blurred");
    },
    pingServer() {
      // check if we got a reply before, it not, we have a connection issue
      if(this.msg_timestamp != null) {
        console.log("Sent a ping but got no pong, disconnect");
        this.closeSocket();
      }
      console.log("pingServer");
      // Send the "pingServer" event to the server.
      this.msg_timestamp = Date.now();
      this.socket.send(JSON.stringify({'op': 'ping'}));
      if(this.connected)
        setTimeout(this.pingServer,PING_INTERVAL_MS);
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
    .blurred {
      filter: blur(2px);
      pointer-events: none;
    }

    </style>
