<template>
        <table class="ui small compact sortable celled table">
            <thead>
                <tr>
                    <th>Operation</th>
                    <th>Path</th>
                    <th>Value</th>
                </tr>
            </thead>
            <tbody v-if="ops && ops.length">
                <tr v-for="op in ops" v-if="ops.length" class="top aligned">
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
import tablesort from 'jquery-tablesort'

export default {
    name: 'json-diff-results',
    mounted () {
        console.log("JsonDiffResults mounted");
        $('table').tablesort();
    },
    created() {
        bus.$on("diffed",this.setOps);
    },
    beforeDestroy() {
        bus.$off("diffed",this.setOps);
    },
    data () {
        return {
            ops : null,
        }
    },
    components: { },
    methods: {
        setOps : function(ops) {
            this.ops = ops;
        },
    },
}
</script>

<style>
.list-label {
    padding-left: 1.5em;
}
</style>
