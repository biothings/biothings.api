<template>
  <div id="app">
    <div class="ui fixed inverted menu">
      <div class="ui studio container">

		<div class="item">
			<div class="ui middle aligned mini">
				<choose-hub></choose-hub>
			</div>
		</div>


        <div class="header item">
            <img class="logo" src="./assets/biothings-studio-color.svg">
            <div id="conn" :data-html="
                '<div style=\'width:30em;\'>' +
                '<a>' + conn.url + '</a><br>' +
                    'App. version: <b>' + conn.app_version + '</b><br>' +
                    'Biothings version: <b>' + conn.biothings_version + '</b><br></div>' 
                    " data-position="bottom center">{{conn.name}}</div>
            </div>

            <a class="clickable item">
                <i class="ui home icon"></i>
                <router-link to="/">Home</router-link>
            </a>
            <a class="clickable item">
                <i class="ui database icon"></i>
                <router-link to="/sources">Sources</router-link>
            </a>
            <a class="clickable item">
                <i class="ui cubes icon"></i>
                <router-link to="/builds">Builds</router-link>
            </a>
            <a class="clickable item">
                <i class="ui shield alternate icon"></i>
                <router-link to="/apis">API</router-link>
            </a>

            <div class="clickable ui item right">
              <job-summary></job-summary>
            </div>

            <div class="clickable ui item">
                <event-messages>
                </event-messages>
            </div>

            <div class="ui item">
                <loader></loader>
                <div id="connected" v-if="socket && socket.readyState == 1" :data-tooltip="'Quality: unknown'" data-position="bottom center">
                    <i class="inverted circular signal icon"></i>
                </div>
                <div v-if="socket && socket.readyState == 1" :data-tooltip="'Connection: ' + socket.protocol" data-position="bottom center">
                    <button class="mini circular ui icon button" @click="closeConnection">
                        <i class="green power off icon"></i>
                    </button>
                </div>
                <div v-else>
                    <button class="mini circular ui icon button" @click="openConnection"
                        data-tooltip="Click to reconnect"
                        data-position="bottom center">
                        <i class="red plug icon"></i>
                    </button>
                </div>
            </div>

          </div>
        </div>

        <div class="ui basic redirect modal">
            <div class="content" v-if="redirect_url">
                <p>
                    Hub requires a different Studio version, found here: <a :href="redirect_url">{{redirect_url}}</a>
                </p>
                <p>
                    This page will automatically redirect to this URL in {{redirect_delay/1000}} second(s) unless cancelled.
                </p>
            </div>
            <div class="content" v-else>
                <p>
                    Hub requires a different Studio version than the one currently running.<br/>
                    No other URLs could be found as a compatible Studio version.<br/>
                </p>
                <p>
                    For your information, required version is: <code><b>{{required_studio_version}}</b></code><br/>
                    Tested URLs were:
                    <div class="ui inverted segment">
                    <div class="ui ordered inverted list">
                        <div class="item" v-for="url in compat_urls"><a :href="url">{{url}}</a></div>
                    </div>
                </div>
                </p>
            </div>
            <div class="content">
                <div class="ui form">
                    <div class="ui checkbox">
                        <input id="skip_compat" type="checkbox" @click="toggleCheckCompat($event)">
                        <label class="white">Skip compatility check</label>
                    </div>
                </div>
            </div>
            <div class="actions">
                <div class="ui red basic cancel inverted button" v-if="redirect_url">
                    <i class="remove icon"></i>
                    Keep this version (not recommended)
                </div>
                <div class="ui green ok inverted button">
                    <i class="checkmark icon"></i>
                    Validate
                </div>
            </div>
        </div>

        <div id="page_content" class="clickable ui active tab segment">
            <router-view></router-view>
        </div>

        <event-alert></event-alert>

      </div>
    </template>

    <script>
    import axios from 'axios';
    import URI from 'urijs';
    import regeneratorRuntime from "regenerator-runtime"; // await/async support

    import VueLocalStorage from 'vue-localstorage';
    Vue.use(VueLocalStorage);
    import Loader from './Loader.vue'

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
    function jsonstr(value) {
        return JSON.stringify(value);
    }
    Vue.filter('json',jsonstr);

    var numeral = require("numeral");
    numeral.register('locale', 'mine', {
        delimiters: {
            thousands: ',',
            decimal: '.'
        },
        abbreviations: {
            thousand: 'thousand',
            million: 'million',
            billion: 'billion',
            trillion: 'trillion'
        },
    });
    numeral.locale('mine');



    Vue.filter("formatNumber", function (value) {
        return numeral(value).format("0.00 a");
    });

    Vue.filter("replace", function (value,what,repl) {
        return value.replace(what,repl);
    });

    import JobSummary from './JobSummary.vue';
    import Status from './Status.vue';
    import DataSourceGrid from './DataSourceGrid.vue';
    import DataSourceDetailed from './DataSourceDetailed.vue';
    import BuildGrid from './BuildGrid.vue';
    import BuildDetailed from './BuildDetailed.vue';
    import ApiGrid from './ApiGrid.vue';
    import EventMessages from './EventMessages.vue';
    import EventAlert from './EventAlert.vue';
    import ChooseHub from './ChooseHub.vue';

    const routes = [
        { path: '/', component: Status },
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
        components: { JobSummary, EventMessages, EventAlert, ChooseHub, Loader},
        mounted () {
            $('#conn')
            .popup({
                on: 'hover'
            });
            $('.menu .item').tab();
            $('.ui.sticky')
            .sticky({
                context: '#page_content'
            })
            ;
            var last = Vue.localStorage.get('last_conn');
            this.conn = this.default_conn;
            if(last) {
                this.conn = JSON.parse(last);
            }
            this.setupConnection();
        },
        created () {
            console.log("App created");
            bus.$on("reconnect",this.setupConnection);
            bus.$on("connect",this.setupConnection,null,"/");
            // connect to default one to start
            this.conn = this.default_conn;
        },
        beforeDestroy() {
            bus.$off("reconnect",this.setupConnection);
            bus.$off("connect",this.setupConnection);
        },
        data() {
            return {
                connected: false,
                socket_msg: '',
                socket : null,
                msg_timestamp : null,
                latency_value : null,
                ping_interval : PING_INTERVAL_MS, // adjustable delay
                default_conn: {
                    "icon" : "/dist/biothings-studio-color.svg",
                    "name" : "BioThings Studio",
                    "app_version" : null,
                    "biothings_version" : null,
                    "url" : "http://localhost:7080",
                },
                conn: null,
                // studio version compatibility checks
                cancel_redirect: false,
                redirect_url: null,
                required_studio_version: null,
                compat_urls: [],
                redirect_delay: 5000,
            }
        },
        computed : {
        },
        watch: {
            latency_value: function (newv, oldv) {
                if(newv != oldv) {
                    this.evalLatency(oldv,newv);
                }
            },
            conn: function(newv,oldv) {
                if(newv != oldv) {
                    if(this.conn.icon)
                        $(".logo").attr("src",this.conn.icon);
                    else
                        $(".logo").attr("src",this.default_conn.icon);
                }
            }
        },
        methods: {
            getCompatList () {
                try {
                var compat = require('./compat.json');
                } catch(e) {
                  console.log(e);
                  console.log("Coulnd't find compat list");
                  var compat = [];
                }
                return compat;
            },
            dispatchEvent(evt) {
                if(evt.obj) {
                    // is it a structured event (jsonifiable) or a standard string event
                    var invalid_json = false;
                    if(evt.data && evt.data.msg.startsWith("{") && evt.data.msg.endsWith("}")) {
                        // try to avoid json process if not even a dict
                        try {
                            var dmsg = JSON.parse(evt.data.msg);
                            // we only know this type for now...
                            if(dmsg["type"] == "alert") {
                                bus.$emit("alert",dmsg);
                                return
                            } else {
                                console.log(`Unknown structured event type: ${dmsg["type"]}`);
                            }
                        } catch(e) {
                            // will be processed as a basic/standard event
                        }
                    }
                    var event = `change_${evt.obj}`;
                    console.log(`dispatch event ${event} (${evt._id}): ${evt.op} [${evt.data}]`);
                    bus.$emit(event,evt._id,evt.op,evt.data);
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
            openConnection() {
                this.setupConnection(null,false);
            },
            setupConnection(conn=null,redirect=false) {
                if(conn != null) {
                    this.conn = conn;
                }
                var url = this.conn["url"].replace(/\/$/,"");
                console.log(`Connecting to ${this.conn.name} (${url})`);
                axios.defaults.baseURL = url;
                this.refreshConnection(url);
                Vue.localStorage.set('last_conn',JSON.stringify(this.conn));
                this.setupSocket(redirect);
            },
            checkCompat: function(data) {
                // user asked to skip compat checks ?
                if(Vue.localStorage.get('skip_studio_compat') == "true") {
                    console.log("Skip Studio version compatibility, as instructed in local storage");
                    return;
                }
                // where should we look for compatible studio webapp
                var current_host_port = new URI(location.protocol+'//'+location.hostname+(location.port ? ':'+location.port: ''));
                var studio_roots = [current_host_port.toString()];
                var remote_webapps = ["https://studio.biothings.io"]; // default remote root if none configured
                if(Vue.localStorage.get("remote_webapps")) {
                    studio_roots = Vue.localStorage.get("remote_webapps");
                    if(typeof studio_roots !== Array)
                        studio_roots = [studio_roots];
                }
                Array.prototype.push.apply(studio_roots,remote_webapps);
                console.log(`Studio webapp roots: ${studio_roots}`);

                function evalDateCompat(refd,d) {
                    // check comparator operator if any (>, <, >= or <=)
                    var res = /^\D+/.exec(refd);
                    var op = null;
                    if(res) {
                        var op = res[0];
                        // adjust actual date string
                        refd = refd.slice(op.length);
                    } else {
                        var op = "===";
                    }
                    var jsd = new Date(d);
                    var jsrefd = new Date(refd);
                    // "+" in front to allow comparison involving "=". https://stackoverflow.com/questions/492994/compare-two-dates-with-javascript
                    console.log(`eval: ${jsd} ${op} ${jsrefd}`);
                    var exp = String.prototype.concat("+jsd " , op , " +jsrefd");
                    var isok = eval(String.prototype.concat("+jsd " , op , " +jsrefd"));
                    return isok;
                }


                // start checks
                var self = this;
                var compat = self.getCompatList();
                // find the first studio version compatible with current hub
                for(var i in compat) {
                    var when = compat[i]["when"];
                    for(var k in when) {
                        if(data[k]) {
                            var vers = data[k].split(" ").map(function(e) { var r = /\[(.*)\]/.exec(e); return r && r[1] || e });
                            var branch = vers[0];
                            switch(vers.length) {
                                case 2:
                                    var commit = vers[1];
                                    var commitdate = null;
                                    break;
                                case 3:
                                    var commit = vers[1];
                                    var commitdate = vers[2];
                                    break;
                                default:
                                    var commit = null;
                                    var commitdate = null;
                            }
                            var required_branch = when[k]["branch"];
                            var required_commit = when[k]["commit"];
                            var required_date = when[k]["date"];
                            console.log(`Hub run ${k} branch:${branch} commit:${commit} commit-date:${commitdate}`);
                            console.log(`Checking compat branch:${required_branch} commit:${required_commit} commit-date:${required_date}`);
                            // first check branch
                            if(required_branch == branch) {
                              // then commit
                              if(required_commit) {
                                if(required_commit != commit) {
                                  console.log(`Commit mismatch, need ${required_commit} but got ${commit}`);
                                  continue;
                                } else {
                                  // commit more restrictive than date, if match, keep that version
                                  console.log("Commits matches");
                                  self.required_studio_version = compat[i]["requires"];
                                  break;
                                }
                              }
                              // then branch
                              if(required_date) {
                                if(!commitdate) {
                                  console.log("Date compat needed but no commit date returned from Hub");
                                  continue;
                                }
                                // can be an expression ("more recent than", "older than", etc...)
                                var interval = required_date.split(",");
                                if(interval.length == 2) {
                                  var fromd = interval[0];
                                  var tod = interval[1];
                                  var dateok = evalDateCompat(fromd,commitdate) && evalDateCompat(tod,commitdate);
                                } else {
                                  var dateok = evalDateCompat(required_date,commitdate);
                                }
                                if(!dateok) {
                                  console.log(`Date mismatch, need ${required_date} but got ${commitdate}`);
                                  continue;
                                } else {
                                  console.log("Dates match");
                                  self.required_studio_version = compat[i]["requires"];
                                  break;
                                }
                              } else {
                                // no commit, no date, but branches match
                                console.log("Branches match");
                                self.required_studio_version = compat[i]["requires"];
                                break;
                              }
                            } else {
                              console.log(`Branch mismatch, need ${required_branch} but got ${branch}`);
                            }
                        }
                    }
                    if(self.required_studio_version) {
                      break; // again to exit main for loop
                    } else {
                      console.log("No match found or no required version specified");
                    }
                }
                if(!self.required_studio_version) {
                  console.log("Couldn't find any suitable version, will keep current as failover");
                  return;
                } else {
                  console.log(`Selected compatible version ${self.required_studio_version}`);
                }
                for(var idx in studio_roots) {
                     var root = studio_roots[idx];
                     if(self.required_studio_version == "this") {
                         // this is the current studio version according to local compat.js
                         var uri = new URI(root);
                     } else {
                         var uri = new URI([root,self.required_studio_version].join("/"));
                     }
                     uri.normalize(); // prevent double slashes
                     var url = uri.toString();
                     self.compat_urls.push(url);
                 }

                if(!self.compat_urls) {
                    console.log("Could not ensure compatibity");
                    return;
                }

                var checked = [];
                for(var i in self.compat_urls) {
                    // check URL is valid
                    console.log(`Checking ${self.compat_urls[i]}`);
                    // opaque response only needed so we can avoid CORS security, we just wanna know if
                    // there's something valid on the other side before redirection
                    // Note: this function is tricky, fetch() is async and we want to access
                    // compat_urls using index "i", but in then() we may treat "i" as the one from the
                    // loop occurence since it's async (fetch() will immediately return, moving the next loop)
                    // so we need to store "i" as "idx" on this function (like a clojure)
                    function doFetch(idx) {
                        fetch(self.compat_urls[idx],{"mode":"no-cors"})
                        .then(function(response) {
                            checked.push({"url" : self.compat_urls[idx], "valid": response.ok, "order":idx});
                        });
                    }
                    doFetch(i);
                }
                // ok fetch() are running, we now wait for the results
                var poll_delay = 500;
                var max_iter = self.redirect_delay / 500; // try at least the number of seconds we would wait before redirect
                var count = 0;
                var inter = setInterval(function() {
                    if(Object.keys(checked).length != self.compat_urls.length) {
                        count++;
                        console.log(`Checking URL for compatible version ${count}/${max_iter}`);
                        if(count > max_iter) {
                            console.log("Give up");
                            clearInterval(inter);
                            $(".redirect.modal")
                            .modal("show");
                        }
                    } else {
                        console.log("All checked:");
                        console.log(checked);
                        clearInterval(inter);
                        self.cancel_redirect = false;
                        // select the best redirect, following studio_roots order
                        self.redirect_url = null;
                        var validsorted = checked.filter(url => url.valid == true).sort((u1,u2) => u1.order - u2.order);
                        if(!validsorted.length) {
                            console.log("No valid redirection, weird...");
                            $(".redirect.modal")
                            .modal("show");
                            return;
                        }
                        self.redirect_url = validsorted[0].url;
                        if(self.redirect_url) {
                            console.log(current_host_port);
                            if(current_host_port.toString() == self.redirect_url) {
                                console.log("Current Studio is compatible");
                                return;
                            }
                            $(".redirect.modal")
                            .modal("setting", {
                                detachable : false,
                                closable: false,
                                onApprove: function () {
                                    window.location.replace(self.redirect_url);
                                },
                                onDeny: function() {
                                    self.cancel_redirect = true;
                                }
                            }).modal("show");
                            setTimeout(
                            function() {
                                if(!self.cancel_redirect) {
                                    console.log(`Redirecting to ${self.compat_urls[i]}`);
                                    window.location.replace(self.redirect_url);
                                }
                            }, self.redirect_delay);
                        }
                    }
                }, poll_delay);

        },
        refreshConnection: function(url) {
            var self = this;
            axios.get(url)
            .then(response => {
                this.checkCompat(response.data.result);
                this.conn = response.data.result;
                this.conn["url"] = url;
            })
            .catch(err => {
                console.log(err);
                console.log("Error creating new connection: " + err.data.error);
            })
        },
        setupSocket(redirect=false) {
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
                    this.ping_interval = PING_INTERVAL_MS;
                    self.pingServer();
                    if(redirect) {
                        window.location.assign(redirect)
                    }
                };
                this.socket.onmessage = function (evt) {
                    var newts = Date.now();
                    self.latency_value = newts - self.msg_timestamp;
                    self.socket_msg = evt.data;
                    self.dispatchEvent(evt.data);
                    self.msg_timestamp = null;
                };
                this.socket.onclose = function() {
                    //bus.$emit("alert",{type: "alert", event: "hub_stop", msg: "Lost connection"})
                    self.closeConnection();
                },
                this.socket.ontimeout = function(err) {
                    console.log("got error");
                    console.log(err);
                }

            })
            .catch(err => {
                console.log("Can't connect using websocket");
                // invalidate connection and use default
                this.conn = this.default_conn;
            });
        },
        closeConnection() {
            this.connected = false;
            this.socket.close();
            this.msg_timestamp = null;
        },
        pingServer() {
            // check if we got a reply before, it not, we have a connection issue
            if(this.msg_timestamp != null) {
                console.log("Sent a ping but got no pong, disconnect");
                this.closeConnection();
            }
            // Send the "pingServer" event to the server.
            this.msg_timestamp = Date.now();
            this.socket.send(JSON.stringify({'op': 'ping'}));
            if(this.connected) {
                setTimeout(this.pingServer,this.ping_interval);
                this.ping_interval = Math.min(this.ping_interval * 1.2,PING_INTERVAL_MS * 6);
            }
        },
        toggleCheckCompat(event) {
            console.log("here");
            var skip = $("#skip_compat").prop("checked");
            Vue.localStorage.set("skip_studio_compat",skip.toString());
            console.log(`skip ${skip}`);

        }
    }
}

</script>

<style>
    #app {
      font-family: 'Avenir', Helvetica, Arial, sans-serif;
      -webkit-font-smoothing: antialiased;
      -moz-osx-font-smoothing: grayscale;
      //text-align: center;
      color: #2c3e50;
      margin-top: 60px;
    }

    .logo {
      margin-right: 0.5em !important;
    }

    .white {
        color: white !important;
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
    .green {color: #0e7948;}

    .ui.studio.container {
        width: 100%;
        margin-left: 1em !important;
        margin-right: 2em !important;
    }

</style>
