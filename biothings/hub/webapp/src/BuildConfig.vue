<template>
    <table class="ui small fixed padded compact grey table">
        <thead>
            <tr>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>Name</td>
                <td>{{build.build_config._id}}</td>
            </tr>
            <tr>
                <td>Document type</td>
                <td >{{build.build_config.doc_type}}</td>
            </tr>
            <tr>
                <td>Merged source(s)</td>
                <td v-if="build.build_config.sources">{{build.build_config.sources.join(", ")}}</td>
            </tr>
            <tr>
                <td>Root source(s)</td>
                <td v-if="build.build_config.root">{{build.build_config.root.join(", ")}}</td>
            </tr>
            <tr>
                <td>Others</td>
                <td >{{JSON.stringify(others)}}</td>
            </tr>
        </tbody>
    </table>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import Vue from 'vue';

export default {
    name: 'build-stats',
    props: ['build'],
    mounted() {
    },
    beforeDestroy() {
    },
    computed: {
        others: function() {
            var _d = {};
            for(var k in this.build.build_config) {
                if(["_id","name","sources","root","doc_type"].includes(k))
                    continue
                _d[k] = this.build.build_config[k];
            }
            return _d;
        }
    },
    components: { },
    methods: {
    },
}
</script>

<style scoped>
</style>
