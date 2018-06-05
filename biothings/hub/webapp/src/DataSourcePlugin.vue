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
                <div :class="['ui plugin form',source._id]">
                    <div class="fields">
                        <div class="required ten wide field">
                            <input type="text" id="release" placeholder="Specify a commit hash or branch (optional)" autofocus>
                        </div>
                        <div class="required six wide field">
                            <button class="ui labeled small icon button" @click="onUpdatePlugin();">
                                <i class="database icon"></i>
                                Update
                            </button>
                        </div>
                    </div>
                </div>
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
        onUpdatePlugin: function() {
            var field = $(`.ui.plugin.form.${this.source._id}`).form('get field', "release");
            var release = null;
            if(field)
                release = field.val();
            return this.dumpPlugin(release=release);
        },
        dumpPlugin: function(release=null) {
            // note: plugin name has the same name as the source
            var data = null;
            if(release != null && release != "")
                data = {"release":release}
            console.log(data);
            axios.put(axios.defaults.baseURL + `/dataplugin/${this.source.name}/dump`,data)
            .then(response => {
                console.log(response.data.result)
            })
            .catch(err => {
                console.log("Error update plugin: " + err);
            })
        },
    },
}
</script>

<style>
</style>
