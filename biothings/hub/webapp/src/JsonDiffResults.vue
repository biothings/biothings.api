<template>
        <table class="ui small compact table">
            <thead>
                <tr>
                    <th>Operation</th>
                    <th>Path</th>
                    <th>Value</th>
                </tr>
            </thead>
            <tbody v-if="ops.length">
                <tr v-for="op in ops" v-if="ops.length">
                    <td>{{op.op}}</td>
                    <td>{{op.path}}</td>
                    <td>{{op.value}}</td>
                </tr>
            </tbody>
                <tr v-else>
                    <td colspan=3>No diff results, data are the same</td>
                </tr>
        </table>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'

export default {
    name: 'json-diff-results',
    mounted () {
        console.log("JsonDiffResults mounted");
    },
    created() {
        bus.$on("diffed",this.displayOps);
    },
    beforeDestroy() {
        bus.$off("diffed",this.displayOps);
    },
    data () {
        return {
            ops : null,
        }
    },
    components: { },
    methods: {
        displayOps : function(ops) {
            this.ops = ops;
            console.log("in displayOps");
            console.log(ops);
        },
    },
}
</script>

<style>
.list-label {
    padding-left: 1.5em;
}
</style>
