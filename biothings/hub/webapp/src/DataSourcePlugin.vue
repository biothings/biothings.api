<template>
    <div class="ui two grid">
        <div class="row">

            <div class="ten wide column">
                <table class="ui small very compact definition collapsing table">
                    <tbody>
                        <tr>
                            <td class="ui grey">URL</td>
                            <td v-if="source.data_plugin.plugin.type == 'github'">
                                <i class="github icon"></i>
                                <span><a :href="source.data_plugin.plugin.url">{{source.data_plugin.plugin.url}}</a></span>
                            </td>
                            <td v-else>No information available</td>
                        </tr>
                        <tr>
                            <td >Release</td>
                            <td>
                                {{source.data_plugin.download.release}}
                            </td>
                        </tr>
                        <tr>
                            <td >Source folder</td>
                            <td>
                                {{source.data_plugin.download.data_folder}}
                            </td>
                        </tr>
                        <tr v-if="source.data_plugin.download.error">
                            <td >Error</td>
                            <td>
                                <div class="red">{{source.data_plugin.download.error}}</div>
                            </td>
                        </tr>
                        <tr>
                            <td >Last download</td>
                            <td>{{source.data_plugin.download.started_at}} <i v-if="source.data_plugin.download.started_at">({{source.data_plugin.download.started_at | moment("from", "now")}})</i></td>
                        </tr>
                        <tr>
                            <td >Duration</td>
                            <td>{{source.data_plugin.download.time}}</td>
                        </tr>
                    </tbody>
                </table>
            </div>
            <div class="six wide column">
                actions
            </div>
        </div>
    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'

export default {
    name: 'data-source-plugin',
    props: ['source'],
    mounted () {
    },
    components: { },
    methods: {
        dump_plugin: function() {
            // note: plugin name has the same name as the source
            axios.put(axios.defaults.baseURL + `/plugin/${this.source.name}/dump`)
            .then(response => {
                console.log(response.data.result)
            })
            .catch(err => {
                console.log("Error getting job manager information: " + err);
            })
        },
    },
}
</script>

<style>
</style>
