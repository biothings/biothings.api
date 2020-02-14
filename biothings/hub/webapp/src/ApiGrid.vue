<template>
    <div id="apis">

        <div class="ui left vertical labeled icon small inverted sidebar menu">
            <a class="item"  v-on:click="createAPI">
                <i class="big icons">
                    <i class="shield icon"></i>
                    <i class="huge corner add icon"></i>
                </i>
                <br>
                <br>
                <div>New API</div>
            </a>
        </div>
        <div class="pusher">
            <div class="ui main container">
                <div class="ui segment">
                    <div class="ui secondary small menu">
                        <a class="item" id="side_menu">
                            <i class="sidebar icon"></i>
                            Menu
                        </a>
                    </div>
                </div>
                <div class="ui centered grid">
                    <div class="ui five wide column" v-for="api in apis">
                        <api v-bind:api="api"></api>
                    </div>
                </div>
            </div>
        </div>

        <!-- create new api -->
        <div class="ui basic createapi modal">
            <h3 class="ui icon">
                <i class="shield icon"></i>
                Create new API
            </h3>
            <div class="content">
                <div class="ui form">
                    <div class="ui centered grid">
                        <div class="eight wide column">

                            <label>Enter a name for the API</label>
                            <input type="text" name="api_id" placeholder="API name" autofocus>
                            <br>
                            <br>

                            <label>Enter a description for the API (optional)</label>
                            <input type="text" name="description" placeholder="Description">
                            <br>
                            <br>

                            <label>Select a backend the API will connect to to serve the data</label>
                            <div>
                                <select class="ui fluid apibackends dropdown" name="api_backend">
                                        <option v-if="backends.length" v-for="idx_info in backends"
                                                :data-es_host="idx_info.host"
                                                :data-index="idx_info.index"
                                                :data-doc_type="idx_info.doc_type">{{idx_info.env}} ({{idx_info.host}} | {{idx_info.index}})</option>
                                </select>
                                <br>
                                <br>
                            </div>

                            <label>Specify a port</label>
                            <input type="text" name="port" placeholder="API port">
                            <br>
                            <br>

                        </div>

                        <div class="eight wide column">
                        </div>
                    </div>
                </div>
            </div>

            <div class="ui error message" v-if="errors.length">
                <ul class="ui list">
                    <li v-for="err in errors">{{err}}</li>
                </ul>
            </div>

            <div class="actions">
                <div class="ui red basic cancel inverted button">
                    <i class="remove icon"></i>
                    Cancel
                </div>
                <div class="ui green ok inverted button" id="newapi_ok">
                    <i class="checkmark icon"></i>
                    OK
                </div>
            </div>
        </div>


    </div>


</template>

<script>
import axios from 'axios'
import Api from './Api.vue'
import Loader from './Loader.vue'
import bus from './bus.js'


export default {
    name: 'api-grid',
    mixins: [ Loader, ],
    mounted () {
        console.log("ApiGrid mounted");
        $('.ui.apibackends.dropdown').dropdown();
        $('#apis .ui.sidebar')
        .sidebar({context:$('#apis')})
        .sidebar('setting', 'transition', 'overlay')
        .sidebar('attach events', '#side_menu');
        $('.ui.form').form();
    },
    updated() {
        // there's some kind of race-condition regarding dropdown init, if
        // in mounted() they won't get init, prob. because data changed and needs to
        // be re-rendered
    },
    created() {
        this.getApis();
        bus.$on('change_api',this.onApiChanged);
    },
    beforeDestroy() {
        // hacky to remove modal from div outside of app, preventing having more than one
        // modal displayed when getting back to that page. https://github.com/Semantic-Org/Semantic-UI/issues/4049
        $('.ui.basic.createapi.modal').remove();
        bus.$off('change_api',this.onApiChanged);
    },
    watch: {
    },
    data () {
        return  {
            apis: [],
            errors: [],
            backends : [],
        }
    },
    components: { Api, },
    methods: {
        getApis: function() {
            this.loading();
            axios.get(axios.defaults.baseURL + '/api/list')
            .then(response => {
                this.apis = response.data.result;
                this.loaded();
            })
            .catch(err => {
                console.log("Error getting APIs information: " + err);
                this.loaderror(err);
            })
        },
        onApiChanged: function(_id=null,op=null) {
            // refresh all of them even if only one is involved
            // (there's not much events and data isn't big)
            this.getApis();
        },
        createAPI: function() {
            $('#apis .ui.sidebar').sidebar("hide");
            var self = this;
            self.loading();
            axios.get(axios.defaults.baseURL + '/index_manager?remote=1')
            .then(response => {
                self.backends = [];
                var envs = response.data.result;
                $.each(envs.env, function( env, value ) {
                    var fillbackend = function(idxs) {
                        for(var idx in idxs) {
                            self.backends.push({
                                "env":env, "host":value["host"],
                                "index":idxs[idx]["index"],
                                "doc_type":idxs[idx]["doc_type"]
                            });
                        }
                    }
                    // either directly a list of index definition
                    // or a dict with different
                    if(Array.isArray(value.index)) {
                        var idxs = value.index;
                        fillbackend(idxs);
                    } else {
                        for(var cat in value.index) {
                            var idxs = value.index[cat];
                            fillbackend(idxs);
                        }
                    }
                });
                $(".ui.apibackends.dropdown").dropdown();
                self.loaded();
            })
            .catch(err => {
                console.log("Error getting index environments: ");
                console.log(err);
                self.loaderror(err);
            })
            $(`.ui.basic.createapi.modal`)
            .modal("setting", {
                detachable : false,
                onApprove: function () {
                    self.errors = [];
                    var api_id = $(".ui.form input[name=api_id]").val();
                    var description = $(".ui.form input[name=description]").val();
                    var backend = $(".ui.form select[name=api_backend] :selected");
                    var es_host = $(backend).attr("data-es_host");
                    var index = $(backend).attr("data-index");
                    var doc_type = $(backend).attr("data-doc_type");
                    var port = parseInt($(".ui.form input[name=port]").val());
                    // form validation
                    if(!api_id)
                        self.errors.push("Provide a name for the API")
                    if(!port)
                        self.errors.push("Provide a port number")
                    if(self.errors.length)
                        return false;
                    self.loading();
                    axios.post(axios.defaults.baseURL + '/api',
                            {"api_id" : api_id,
                             "es_host" : es_host,
                             "index" : index,
                             "doc_type" : doc_type,
                             "port" : port,
                             "description" : description})
                        .then(response => {
                            console.log(response.data.result)
                            self.loaded();
                            return response.data.result;
                        })
                        .catch(err => {
                            console.log("Error creating API: ");
                            console.log(err);
                            self.loaderror(err);
                        })
                }
            })
            .modal("show");
        },
    }
}
</script>

<style>
.ui.sidebar {
    overflow: visible !important;
}
</style>
