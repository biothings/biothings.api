<template>
    <table class="ui small very compact blue table">
        <thead>
            <tr>
                <th>Datasource</th>
                <th>Version</th>
            </tr>
        </thead>
        <tbody v-if="build._meta">
            <tr v-for="(info,src) in build._meta.src">
                <td v-if="info.url"><a :href="info.url">{{src}}</a></td>
                <td v-else>{{src}}</td>
                <td>{{info.version}}</td>
            </tr>
        </tbody>
        <tfoot v-else>
            <tr><th>No data</th>
                <th></th>
            </tr>
        </tfoot>
    </table>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import Vue from 'vue';

export default {
    name: 'build-sources',
    props: ['build'],
    mounted() {
    },
    beforeDestroy() {
    },
    components: { },
    methods: {
    },
}
</script>

<style scoped>
</style>
