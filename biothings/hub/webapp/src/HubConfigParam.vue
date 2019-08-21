<template>
    <form class="ui form">
        <div class="field">
        <span v-if="Array.isArray(param.value)">
            <span v-if="readonly">
                <pre class="param-readonly">{{ JSON.stringify(param.value,null,4) }}</pre>
                <div class="param-legend">(value is read-only, it cannot be edited)</div>
            </span>
            <span v-else>
                <textarea>{{ JSON.stringify(param.value,null,4) }}</textarea>
                <div class="param-legend">(value is a list, json format required)</div>
            </span>
        </span>
        <span v-else-if="typeof param.value === 'object'">
            <span v-if="readonly">
                <pre class="param-readonly">{{ JSON.stringify(param.value,null,4) }}</pre>
                <div class="param-legend">(value is read-only, it cannot be edited)</div>
            </span>
            <span v-else>
                <textarea>{{ JSON.stringify(param.value, null, 4) }}</textarea>
                <div class="param-legend">(value is a dict, json format required)</div>
            </span>
        </span>
        <span v-else>
            <span v-if="readonly">
                <!--input :type="param.hidden? 'password' : 'text'" autocomplete="off" :value="param.value" readonly=""-->
                <div class="param-readonly">{{ JSON.stringify(param.value,null,4) }}</div>
                <div class="param-legend">(value is read-only, it cannot be edited)</div>
            </span>
            <span v-else>
                <input :type="param.hidden? 'password' : 'text'" autocomplete="off" :value="param.value">
            </span>
        </span>
    </div>
    </form>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'

export default {
    name: 'hub-config-param',
    props: ["param","readonly"],
    components: { },
    mixins : [ ],
    mounted () {
    },
    created() {
    },
    updated() {
    },
    beforeDestroy() {
    },
    data () {
        return {
        }
    },
    computed: {
        // a computed getter
    },
    methods: {
    },
}
</script>

<style>
.param-legend {
    font-style: italic;
    color: grey;
    font-size: smaller;
}
.param-readonly {
    color: grey;
}
</style>
