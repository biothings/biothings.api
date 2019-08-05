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
                <div v-if="connection_error" class="connectionerror ui red basic label">
                    <div>Unable to connect to Hub API because:</div>
                    <pre v-html="connection_error"></pre>

                </div>
            </div>
            <br>
            <div class="ui inverted accordion advanced">
                <div class="title">
                    <i class="dropdown icon"></i>
                    Advanced
                </div>
                <div class="content">
                    <div v-if="signin_error" class="ui red basic label signin-error" v-html="signin_error"></div>
                    <span v-if="logged_username">
                        <div>
                            <span>Currently logged as <b class="logged-user">{{logged_username}}</b></span>
                            <a class="signout" @click="signOut">Sign out</a>
                        </div>
                        <br>
                    </span>
                <form class="ui inverted form login" method="post" v-else onsubmit="return false">
                    <div class="field">
                        <label>Username</label>
                        <input type="text" name="username" placeholder="Email">
                    </div>
                    <div class="field">
                        <label>Password</label>
                        <input type="password" name="password" placeholder="Password">
                    </div>
                    <button class="ui button" type="submit" @click="signIn">Login</button>
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
},
created() {
    bus.$on("connection_failed",this.failedConnection);
    bus.$on("logged",this.logged);
    bus.$on("logerror",this.logerror);
    bus.$on("logged_user",this.setLoggedUser);
},
beforeDestroy() {
    $('.ui.basic.newhuburl.modal').remove();
    bus.$off("connection_failed",this.failedConnection);
    bus.$off("logged",this.logged);
    bus.$off("logerror",this.logerror);
    bus.$off("logged_user",this.setLoggedUser);
},
data() {
    return {
        existings : {},
        connection_error: null,
        signin_error: null,
        logged_username: null,
        tokens: {},
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
            this.connection_error = this.extractError(error);
            this.newConnection(url);
        },
        newConnection: function(url=null) {
            var self = this;
            if(url) {
                $(".ui.newhuburl.form").form('get field', "huburl").val(url);
            }
            // reset errors
            self.signin_error = null;

            $('.ui.basic.newhuburl.modal')
            .modal("setting", {
                detachable : false,
                closable: false,
                onApprove: function () {
                    self.connection_error = null;
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
        signIn() {
            this.signin_error = null;
            const username = $(".ui.form.login").form("get field","username").val();
            const password = $(".ui.form.login").form("get field","password").val();
            auth.signIn(username,password);
            return false;
        },
        logged: function(username,tokens) {
            this.tokens = tokens; // TODO: use Secure Cookie
            document.cookie = "biothings-access-token=" + tokens.accessToken.jwtToken;
            document.cookie = "biothings-id-token=" + tokens.idToken.jwtToken;
            document.cookie = "biothings-refresh-token=" + tokens.refreshToken.token;
            document.cookie = "biothings-current-user=" + username;
            this.setLoggedUser(username);
            // reset errors
            this.signin_error = null;
        },
        logerror: function(user,error,reason) {
            this.signin_error = error;
            if(reason)
                this.signin_error += ": " + reason;
            // not logged anymore
            this.logger_user = null;
            this.tokens = {}
        },
        setLoggedUser(username) {
            this.logged_username = username;
        },
        signOut: function() {
            auth.signOut();
            hubapi.clearLoggedUser();
        },
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
  .signout {font-weight: bold; cursor: pointer; padding-left: 1em;}
  .logged-user {color:lightgrey;}
  .signin-error {margin-bottom: 1em;}

</style>
