<template>
    <span>
        <div class="choosehub ui floating dropdown mini icon button">
            <i class="caret square down icon"></i>
            <div class="menu">
                <div class="item" data-value="new">
                    <i class="plus circle icon"></i>
                    <b>Create new connection</b>
                </div>
                <div class="divider" v-if="Object.keys(existings).length"></div>
                <div class="header" v-if="Object.keys(existings).length">
                    Existing connections
                </div>
                <div class="scrolling menu" v-if="Object.keys(existings).length">
                    <div class="item" :data-value="v.name" v-for="v in existings">
                        <img class="hubicon" :src="v.icon"></img>
                        <b>{{v.name}}</b> <a class="bluelink">{{v.url}}</a>
                    </div>
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
import axios from 'axios'
import bus from './bus.js'
import Vue from 'vue';

export default {
    name: 'choose-hub',
    props: [],
    mounted() {
        console.log("ChooseHub mounted");
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
    },
    created() {
    },
    beforeDestroy() {
        $('.ui.basic.newhuburl.modal').remove();
    },
    data() {
        return {
            existings : {}
        };
    },
    mixins: [ ],
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
                previous = JSON.parse(previous);
            this.existings = previous;
        },
        refreshConnection: function(url) {
            var self = this;
            if(!url.startsWith("http")) {
                url = `http://${url}`;
            }
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
                axios.defaults.baseURL = url;
                // auto-connect to newly created connection, and redirect to home
                bus.$emit("connect",data,"/");
            })
            .catch(err => {
                console.log(err);
                console.log("Error creating new connection: " + err.data.error);
            })
        },
        newConnection: function() {
            var self = this;
            $('.ui.basic.newhuburl.modal')
            .modal("setting", {
                detachable : false,
                onApprove: function () {
                    var url = $(".ui.newhuburl.form").form('get field', "huburl").val();
                    self.refreshConnection(url);
                }
            })
            .modal("show");
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

</style>
