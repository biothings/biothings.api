<template>
    <span>
        <div class="choosehub ui floating dropdown mini icon button">
            <i class="caret square down icon"></i>
            <div class="menu largechoose">
                <div class="item" data-value="new">
                    <i class="plus circle icon"></i>
                    <b>Create new connection</b>
                </div>
                <div class="divider" v-if="Object.keys(existings).length"></div>
                <div class="header" v-if="Object.keys(existings).length">
                    Existing connections
                </div>
                <div class="scrolling menu" v-if="Object.keys(existings).length">
                    <div class="item hubconnect" :data-value="v.name" v-for="v in existings">
                        <table class="ui small compact table hubconnect">
                            <tbody>
                                <tr>
                                    <td class="collapsing tdhubicon">
                                        <img class="hubicon" :src="v.icon"></img>
                                    </td>
                                    <td class="collapsing twelve wide">
                                        <b>{{v.name}}</b>
                                    </td>
                                    <td class="collapsing hubconnect">
                                        <a :href="v.url">
                                            <i class="external alternate icon">
                                            </i>
                                        </a>
                                    </td>
                                    <td class="collapsing hubconnect">
                                        <button class="ui small icon button" @click="editConnection($event,v)">
                                            <i class="grey edit icon right floated"></i>
                                        </button>
                                    </td>
                                    <td class="collapsing hubconnect">
                                        <button class="ui small icon button" @click="deleteConnection($event,v)">
                                            <i class="grey trash alternate outline icon right floated"></i>
                                        </button>
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>

        <div class="ui basic newhuburl modal">
            <h3 class="ui icon">
                <i class="plug icon"></i>
                Create a new connection
            </h3>
            <div class="ui newhuburl form">
                <div class="fields">
                    <div class="required ten wide field">
                        <input type="text" id="huburl" placeholder="Hub address" autofocus>
                    </div>
                </div>
                <div v-if="connection_error" class="connectionerror ui orange label" v-html="connection_error"></div>
            </div>
            <br>
            <div class="ui inverted accordion advanced">
                <div class="title">
                    <i class="dropdown icon"></i>
                    Advanced
                </div>
                <div class="content">
                    <div v-if="logged_username">Currently logged as {{logged_username}}</div>
                    <form class="ui inverted form login" method="post" v-else>
                        <div class="field">
                            <label>Username</label>
                            <input type="text" name="username" placeholder="Email">
                        </div>
                        <div class="field">
                            <label>Password</label>
                            <input type="password" name="password" placeholder="Password">
                        </div>
                        <button class="ui button" type="submit">Login</button>
                    </form>
                </div>
            </div>
            <div class="actions">
                <div class="ui red basic cancel inverted button">
                    <i class="remove icon"></i>
                    Cancel
                </div>
                <div class="ui green ok inverted button">
                    <i class="checkmark icon"></i>
                    OK
                </div>
            </div>
        </div>
    </span>

</template>

<script>
import axios from 'axios';
import bus from './bus.js'
import auth from './auth.js'
import hubapi from './hubapi.js'
import Vue from 'vue';

import Loader from './Loader.vue'

export default {
    name: 'choose-hub',
    props: [],
    mixins: [ Loader, ],
    mounted() {
        console.log("ChooseHub mounted");
        var self = this;
        this.getExistings();
        var self = this;
        $('.choosehub.ui.floating.dropdown').dropdown({
          onChange: function(value, text, $selectedItem) {
            if(value == "new") {
              self.newConnection();
            } else {
              var conn = self.existings[value];
              var url = conn["url"].replace(/\/$/,"");
              self.refreshConnection(url);
              if(conn)
                  bus.$emit("connect",conn,"/");
              else
                  console.log(`Can't find connection details for ${value}`);
            }
          },
        });
        $('.ui.accordion.advanced').accordion();
        $('.ui.form.login').submit(function() {
            console.log("subsmubt mec");
            self.doLogin();
            return false;
        });
    },
    created() {
        bus.$on("connection_failed",this.failedConnection);
        bus.$on("logged",this.logged);
        bus.$on("logerror",this.logerror);
    },
    beforeDestroy() {
        $('.ui.basic.newhuburl.modal').remove();
        bus.$off("connection_failed",this.failedConnection);
    },
    data() {
        return {
            existings : {},
            connection_error: null,
            logged_username: null,
            tokens: {},
            log_error: null,
            log_error_reason: null,
        };
    },
    components: { },
    computed: {
    },
    methods: {
        buildConnections: function() {
        },
        getExistings: function() {
            var previous = Vue.localStorage.get('hub_connections');
            if(!previous)
                previous = {};
            else
                //previous = JSON.parse(JSON.parse(previous));
                previous = JSON.parse(previous);
            this.existings = previous;
        },
        refreshConnection: function(url) {
            var self = this;
            if(!url.startsWith("http")) {
                url = `http://${url}`;
            }
            self.loading();
            hubapi.base(url);
            axios.get(url)
            .then(response => {
                var data = response.data.result;
                self.getExistings();
                data["url"] = url.replace(/\/$/,"");
                if(!data["name"])
                    data["name"] = self.$parent.default_conn["name"];
                if(!data["icon"])
                    data["icon"] = self.$parent.default_conn["icon"];
                self.existings[data["name"]] = data;
                Vue.localStorage.set('hub_connections',JSON.stringify(self.existings));
                // update base URL for all API calls
                // auto-connect to newly created connection, and redirect to home
                bus.$emit("connect",data,"/");
                self.connection_error = null;
                self.loaded();
            })
            .catch(err => {
                self.loaderror(err);
                this.failedConnection(url,err);
            })
        },
        failedConnection: function(url, error) {
            var errmsg = this.extractError(error);
            this.connection_error = errmsg;
            this.newConnection(url);
        },
        newConnection: function(url=null) {
            var self = this;
            if(url) {
                $(".ui.newhuburl.form").form('get field', "huburl").val(url);
            }
            $('.ui.basic.newhuburl.modal')
            .modal("setting", {
                detachable : false,
                closable: false,
                onApprove: function () {
                    var url = $(".ui.newhuburl.form").form('get field', "huburl").val();
                    self.refreshConnection(url);
                }
            })
            .modal("show");
        },
        deleteConnection(event,conn) {
          // avoid onChange to be triggered
          event.stopPropagation();
          console.log(event);
          console.log(`Delete connection named "${conn.name}"`);
          delete this.existings[conn.name];
          Vue.localStorage.set('hub_connections',JSON.stringify(this.existings));
          console.log(this.existings);
          this.getExistings();
        },
        editConnection(event,conn) {
          // avoid onChange to be triggered
          event.stopPropagation();
          console.log(event);
          this.newConnection(conn.url);
        },
        doLogin() {
            const username = $(".ui.form.login").form("get field","username").val();
            const password = $(".ui.form.login").form("get field","password").val();
            auth.signIn(username,password);
            return false;
        },
        logged: function(username,tokens) {
            this.logged_username = username;
            this.tokens = tokens; // TODO: use Secure Cookie
            document.cookie = "biothings-access-token=" + tokens.accessToken.jwtToken;
            document.cookie = "biothings-id-token=" + tokens.idToken.jwtToken;
            document.cookie = "biothings-refresh-token=" + tokens.refreshToken.token;
            // reset errors
            this.log_error = null;
            this.log_error_reason = null;
        },
        logerror: function(user,error,reason) {
            this.log_error = error;
            this.log_error_reason = reason;
            // not logged anymore
            this.logger_user = null;
            this.tokens = {}
        }
    },
}
</script>

<style scoped>
  @keyframes pulse {
    0% {transform: scale(1, 1);}
    50% {transform: scale(1.2, 1.2);}
    100% {transform: scale(1, 1);}
  }

  .pulsing {
    animation: pulse 1s linear infinite;
  }

  .conftag {
      margin-bottom: 1em !important;
  }

  a {
        color: #218cbc !important;
    }

  .hubicon { width:2.5em !important;}
  .largechoose {width:30em;}
  .ui.table.hubconnect {border: 0px !important;}
  .ui.menu .ui.dropdown .menu>.item.hubconnect {padding: 0 !important;}
  .ui.compact.table.hubconnect td {padding: .3em .0em .0em 0em;}
  .tdhubicon {padding: 0.3em 1em 0.3em 0.3em !important;}

</style>
