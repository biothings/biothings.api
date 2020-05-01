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
        // this is the class name for elements which cause actions (not GET)
        // so if we're in r/o mode, actionable class is readonly, else, it's
        // nothing and elements are displayed
        actionable: {
            // for conveniency (from caller point of view, ie. templates and controllers)
            // the get() method returns the class name, while...
            get: function() {
                return this.readonly ? "readonly" : "";
            },
            // ... the set() method takes true/false as input, and stores
            // on underlying flag (this.readonly). So:
            //   this.actionable = true // switch to readonly
            //   console.log(this.actionable)
            // will print "readonly", *not* "true".
            set: function(mode) {
                this.readonly = mode;
                Vue.localStorage.set('readonly',JSON.stringify(this.readonly));
            }
        },
    },
    methods: {
        switchMode(mode) {
            this.readonly = mode;
        },
    },
}
</script>

<style>
.readonly{
    display: none !important;
}
</style>


