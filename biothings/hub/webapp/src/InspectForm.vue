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
                    <!--option selected>type</option-->
                    <option selected>mapping</option>
                    <!--option>stats</option-->
                </select>
            </div>
            <div class="ten wide column">
                <div class="ui inverted list">
                    <!--a class="item">
                        <i class="right triangle icon"></i>
                        <div class="content">
                            <div class="header">type</div>
                            <div class="description">Builds a map of all types involved in the data, providing a summary of its structure</div>
                        </div>
                    </a-->
                    <a class="item">
                        <i class="right triangle icon"></i>
                        <div class="content">
                            <div class="header">mapping</div>
                            <div class="description">Analyzes data so the inspection results can be converted into an ElasticSearch mapping (used during indexing step)</div>
                        </div>
                    </a>
                    <!--a class="item">
                        <i class="right triangle icon"></i>
                        <div class="content">
                            <div class="header">stats</div>
                            <div class="description">Performs in-depth analysis about the data, including type map and basic statistics, showing how volumetry fits over data structure</div>
                        </div>
                    </a-->
                </div>
            </div>
            <div class="two wide column">
            </div>

            <div class="four wide column" v-if="select_data_provider">
                <select id="select-data_provider" class="ui dropdown">
                    <option value="src">Data Collection</option>
                    <option value="uploader">Uploader</option>
                </select>
            </div>
            <div class="ten wide column" v-if="select_data_provider">
                <div class="ui inverted list">
                    <a class="item">
                        <i class="right triangle icon"></i>
                        <div class="content">
                            <div class="header">Data Collection</div>
                            <div class="description">Data is fetched from collection (stored data)</div>
                        </div>
                    </a>
                    <a class="item">
                        <i class="right triangle icon"></i>
                        <div class="content">
                            <div class="header">Uploader</div>
                            <div class="description">Data is fetched directly from the parser, before storage into a collection</div>
                        </div>
                    </a>
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
    props: ['_id','select_data_provider'],
    mounted () {
        $('select.dropdown').dropdown();
    },
    beforeDestroy() {
        $(`#inspect-${this._id}.ui.basic.inspect.modal`).remove();
    },
    methods: {
        inspect: function() {
            var self = this;
            $(`#inspect-${this._id}.ui.basic.inspect.modal`)
            .modal("setting", {
                onApprove: function () {
                    var modes = $(`#inspect-${self._id}`).find("#select-mode").val();
                    var dp = $(`#inspect-${self._id}`).find("#select-data_provider").val();
                    console.log(modes);
                    console.log(dp);
                    axios.put(axios.defaults.baseURL + '/inspect',
                              {"data_provider" : [dp,self._id],"mode":modes})
                    .then(response => {
                        console.log(response.data.result)
                        bus.$emit("refresh_sources");
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
