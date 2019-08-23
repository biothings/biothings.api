<template>
    <div>
        <div class="ui info message">All values must be in JSON format</div>
        <table class="ui basic table">
            <tbody>
                <hub-config-param v-bind:param="param" v-for="param in params"></hub-config-param>
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
</style>
