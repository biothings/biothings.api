<template>
    <!-- Inspect -->
    <div class="ui basic inspect modal" v-bind:id="'inspect-' + _id">
        <div class="ui icon">
            <i class="large unhide icon"></i>
            <h3>Inspect data: {{_id}}</h3>
        </div>
        <div class="ui fluid grid">

            <div class="four wide column">
            </div>
            <div class="twelve wide column">
                <p><i>Selecting more than one mode won't affect much the performance, running time will roughly be the same.</i></p>
            </div>

            <div class="four wide column">
                <select id="select-mode" class="ui dropdown" multiple="">
                    <option value="mapping" selected>mapping</option>
                    <option value="type">type</option>
                    <option value="stats">stats (experimental)</option>
                </select>
            </div>
            <div class="ten wide column">
                <div class="ui inverted list">
                    <a class="item">
                        <i class="right triangle icon"></i>
                        <div class="content">
                            <div class="header">mapping</div>
                            <div class="description">Analyzes data so the inspection results can be converted into an ElasticSearch mapping (used during indexing step)</div>
                        </div>
                    </a>
                    <a class="item">
                        <i class="right triangle icon"></i>
                        <div class="content">
                            <div class="header">type</div>
                            <div class="description">Builds a map of all types involved in the data, providing a summary of its structure</div>
                        </div>
                    </a>
                    <a class="item">
                        <i class="right triangle icon"></i>
                        <div class="content">
                            <div class="header">stats</div>
                            <div class="description">Performs in-depth analysis about the data, including type map and basic statistics, showing how volumetry fits over data structure</div>
                        </div>
                    </a>
                </div>
            </div>
            <div class="two wide column">
            </div>

            <div class="four wide column">
            </div>
            <div class="twelve wide column">
                <p><i>Optional parameters</i></p>
            </div>

            <div class="sixteen wide column">
                <div class="ui fluid grid">
                    <div class="four wide column">
                        <div class="ui inverted input">
                            <input type="text" id="limit" placeholder="Limit...">
                        </div>
                    </div>
                    <div class="twelve wide column">
                        <div class="ui inverted list">
                            <a class="item">
                                <div class="content">
                                    <div class="header">Limit</div>
                                    <div class="description">Restrict inspection to this number of documents. If empty, all documents are inspected.</div>
                                </div>
                            </a>
                        </div>
                        <div v-if="$parent.$parent.limit_error" class="ui negative message">
                            <p>{{$parent.$parent.limit_error}}</p>
                        </div>
                    </div>
                </div>

                <div class="ui fluid grid">
                    <div class="four wide column">
                        <div class="ui inverted input">
                            <input type="text" id="sample" placeholder="Sampling (eg. 0.5)...">
                        </div>
                    </div>
                    <div class="twelve wide column">
                        <div class="ui inverted list">
                            <a class="item">
                                <div class="content">
                                    <div class="header">Sampling data</div>
                                    <div class="description">Randomly pick documents to inspect. Value is a float between 0 and 1.0. If sampling is 1.0, all documents are
                                        picked, if 0.0, none of them. Combined with parameter "limit", it allows to randomly inspect a subset of the data.
                                    </div>
                                </div>
                            </a>
                        </div>
                        <div v-if="$parent.$parent.sample_error" class="ui negative message">
                            <p>{{$parent.$parent.sample_error}}</p>
                        </div>
                    </div>
                </div>

            </div>

            <div class="two wide column">
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
</template>

<script>
import axios from 'axios'
import bus from './bus.js'

export default {
    name: 'inspect-form',
    props: ['_id'],
    mounted () {
        $('select.dropdown').dropdown();
    },
    created() {
        bus.$on('do_inspect',this.inspect);
    },
    beforeDestroy() {
        bus.$off('do_inspect',this.inspect);
        $(`#inspect-${this._id}.ui.basic.inspect.modal`).remove();
    },
    data () {
        return {
            limit_error: null,
            sample_error: null,
        }
    },
    methods: {
        inspect: function(data_provider) {
            if(typeof data_provider == "string") {
                // target collection
                var _id = data_provider;
            } else {
                // source collection, format ["src",id]
                var _id = data_provider[1];
            }
            var self = this;
            $(`#inspect-${_id}.ui.basic.inspect.modal`)
            .modal("setting", {
                onApprove: function () {
                    var modes = $(`#inspect-${_id}`).find("#select-mode").val();
                    var limit = $(`#inspect-${_id}`).find("#limit").val();
                    var sample = $(`#inspect-${_id}`).find("#sample").val();
                    var params = {"data_provider" : data_provider,"mode":modes};
                    if(limit) {
                        var plimit = parseInt(limit);
                        if(plimit)
                            params["limit"] = plimit;
                        else {
                            self.limit_error = `"${limit}" is not an integer`;
                            return false;
                        }
                    }
                    if(sample) {
                        var psample = parseFloat(sample);
                        if(psample)
                            params["sample"] = psample;
                        else {
                            if(psample == 0)
                                self.sample_error = "Sample must be greater than zero...";
                            else
                                self.sample_error = `"${sample}" is not a float`;
                            return false
                        }
                    }
                    axios.put(axios.defaults.baseURL + '/inspect',params)
                    .then(response => {
                        console.log(response.data.result)
                    })
                    .catch(err => {
                        console.log("Error getting job manager information: " + err);
                    })
                }
            })
            .modal("show");
        },
    },
}
</script>

<style>
</style>
