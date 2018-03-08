<template>
    <div class="ui fluid card">
        <div class="content">
            <div class="ui tiny red label right floated" v-if="api.status == 'running'">running</div>

            <!-- error -->
            <div class="ui"
                v-bind:data-tooltip="displayError()">
                <i class="right floated red alarm icon pulsing"
                    v-if="api.status == 'failed'"></i>
            </div>

            <div class="left aligned header" v-model="api">
                <a>{{ api._id }}</a>
            </div>
            <div class="left aligned description">
                <div v-if="api.status == 'running'">
                    <div class="ui bulleted list">
                        <div class="item">Metadata: <a :href="url_metadata">{{ url_metadata }}</a></div>
                        <div class="item">Query: <a :href="url_query">{{ url_query }}</a></div>
                    </div>
                </div>
            </div>
            <div class="meta">
            </div>

            <div class="ui clearing divider"></div>

            <div class="left aligned description">
                <p class="center aligned">
                    <b>{{api.description}}</b>
                </p>
                <p>
                    <table class="ui celled table">
                        <tbody>
                            <tr>
                                <td>ElasticSearch host</td>
                                <td><a :href="api.config.es_host">{{api.config.es_host}}</a></td>
                            </tr>
                            <tr>
                                <td>Index</td>
                                <td>{{api.config.index}}</td>
                            </tr>
                            <tr>
                                <td>Document type</td>
                                <td>{{api.config.doc_type}}</td>
                            </tr>
                            <tr>
                                <td>API port</td>
                                <td>{{api.config.port}}</td>
                            </tr>
                        </tbody>
                    </table>
                </p>
            </div>
        </div>

        <div class="extra content">
            <div class="ui icon buttons left floated mini" v-if="api.status != 'running'">
                <button class="ui button" v-on:click="startAPI">
                    <i class="play icon"></i>
                </button>
            </div>
            <div class="ui icon buttons left floated mini" v-else>
                <button class="ui button" v-on:click="stopAPI">
                    <i class="stop icon"></i>
                </button>
            </div>
            <div class="ui icon buttons right floated mini">
                <button class="ui button">
                    <i class="trash icon" @click="deleteAPI()"></i>
                </button>
            </div>
        </div>

        <div class="ui basic deleteapi modal" :id="api._id">
            <div class="ui icon header">
                <i class="remove icon"></i>
                Delete API
            </div>
            <div class="content">
                <p>Are you sure you want to delete API <b>{{api._id}}</b> ?</p>
            </div>
            <div class="actions">
                <div class="ui red basic cancel inverted button">
                    <i class="remove icon"></i>
                    No
                </div>
                <div class="ui green ok inverted button">
                    <i class="checkmark icon"></i>
                    Yes
                </div>
            </div>
        </div>

    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import Vue from 'vue';
import InspectForm from './InspectForm.vue'

export default {
    name: 'api',
    props: ['api',],
    mounted() {
        $('.menu .item')
        .tab()
        ;
    },
    beforeDestroy() {
        $(`#${this.api._id}.ui.basic.deleteapi.modal`).remove();
    },
    data () {
        return {
            errors : [],
        }
    },
    components: { InspectForm, },
    computed : {
        url_metadata : function() {
            return this.api.url + "/metadata";
        },
        url_query: function() {
            return this.api.url + `/query?q=*`;
        }
    },
    methods: {
        displayError : function() {
            var errs = [];
            if(this.api.err) 
                errs.push(this.api.err);
            return errs.join("<br>");
        },
        deleteAPI: function() {
            var self = this;
            $(`#${self.api._id}.ui.basic.deleteapi.modal`)
            .modal("setting", {
                onApprove: function () {
                    axios.delete(axios.defaults.baseURL + '/api',{"data":{"api_id":self.api._id}})
                    .then(response => {
                        console.log(response.data.result)
                        bus.$emit("refresh_apis");
                        return true;
                    })
                    .catch(err => {
                        console.log(err);
                        console.log("Error deleting api: " + err.data.error);
                        bus.$emit("refresh_apis");
                    })
                }
            })
            .modal("show");
        },
        startStopAPI: function(mode) {
            axios.put(axios.defaults.baseURL + `/api/${this.api._id}/${mode}`)
            .then(response => {
                console.log(response.data.result);
                bus.$emit("refresh_apis");
            })
            .catch(err => {
                 console.log(err);
                 console.log(`Error ${mode}ing api: ` + err.data.error);
                 bus.$emit("refresh_apis");
            });

        },
        startAPI: function() {
            return this.startStopAPI("start");
        },
        stopAPI: function() {
            return this.startStopAPI("stop");
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

  a {
        color: #930000;
    }

</style>
