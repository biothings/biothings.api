<template>
</template>

<script>

import Vue from 'vue';
import bus from './bus.js'

export default {
    name: 'actionable',
    mounted () {
        bus.$on("readonly_mode",this.switchMode);
    },
    components: { },
    created() {
    },
    beforeDestroy() {
        bus.$off("readonly_mode",this.switchMode);
    },
    data() {
        return {
            readonly : Vue.localStorage.get('readonly') == "true",
        }
    },
    computed: {
        actionable: {
            get: function() {
                return this.readonly ? "readonly" : "";
            },
            set: function(mode) {
                this.readonly = mode;
                Vue.localStorage.set('readonly',JSON.stringify(this.readonly));
            }
        },
    },
    methods: {
        switchMode(mode) {
            this.readonly = mode;
            console.log(`switching to mode read-only: ${this.readonly}`);
        },
    },
}
</script>

<style>
.readonly{
    display: none !important;
}
</style>


