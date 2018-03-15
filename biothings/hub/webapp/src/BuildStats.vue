<template>
    <table class="ui small very compact grey table">
        <thead>
            <tr>
                <th>Stats</th>
                <th>Count</th>
            </tr>
        </thead>
        <tbody v-if="build._meta">
            <tr v-for="(count,stat) in build._meta.stats">
                <td >{{stat}}</td>
                <td>{{count | currency("",0)}}</td>
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
    name: 'build-stats',
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
