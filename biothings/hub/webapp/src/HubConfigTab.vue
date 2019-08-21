<template>
    <div>
            <table class="ui basic table">
                <tbody>
                    <tr v-for="param in params">
                        <td class="six wide">
                            <div class="param-name">{{ param.name }}</div>
                            <div class="param-desc">{{ param.desc }}</div>
                        </td>
                        <td class="ten wide">
                            <hub-config-param v-bind:param="param" v-bind:readonly="param.readonly"></hub-config-param>
                        </td>
                    </tr>
                </tbody>
            </table>
    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import HubConfigParam from './HubConfigParam.vue'

export default {
    name: 'hub-config-tab',
    props: ["section","params"],
    components: { HubConfigParam },
    mixins : [ ],
    mounted () {
        console.log("HubConfigTab mounted");
    },
    created() {
    },
    updated() {
        $('.menu .item').tab();
        $('.menu .item').tab('change tab', 'General')
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

<style scoped>
.param-name {
    font-family: monospace;
    font-weight: bold;
}

.param-desc {
    color: grey;
    word-wrap: break-word;
    hyphens: auto;
}
</style>
