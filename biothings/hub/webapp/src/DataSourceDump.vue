<template>
    <div class="ui two grid">
        <div class="row">

            <div class="ten wide column">
                <table class="ui small very compact definition collapsing table">
                    <tbody>
                        <tr>
                            <td class="ui grey">Dumper</td>
                            <td>
                                {{source.download.dumper.name}}
                                <span v-if="source.download.dumper.manual">(manual)</span>
                            </td>
                        </tr>
                        <tr>
                            <td >Status</td>
                            <td><i>{{source.download.status}}</i></td>
                        </tr>
                        <tr>
                            <td >Last download</td>
                            <td>{{source.download.started_at}} <i v-if="source.download.started_at">({{source.download.started_at | moment("from", "now")}})</i></td>
                        </tr>
                        <tr>
                            <td >Duration</td>
                            <td>{{source.download.time}}</td>
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
    name: 'data-source-dump',
    props: ['source'],
    mounted () {
    },
    components: { },
    methods: {
        dump: function() {
            axios.put(axios.defaults.baseURL + `/source/${this.source.name}/dump`)
            .then(response => {
                console.log(response.data.result)
                this.$parent.getSourcesStatus();
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
