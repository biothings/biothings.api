<template>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'

export default {
    name: 'base-build',
    // Note: we don't declare "build", it must be defined in subclass/mixed
    // (sometimes it's a prop, sometimes it's a data field
    mounted () {
    },
    components: { },
    created() {
    },
    beforeDestroy() {
    },
    computed: {
    },
    methods: {
        inspect: function(event) {
            if($(event.target).attr("data-build_id") == this.build._id)
                bus.$emit("do_inspect",this.build._id);
        },
    },
}
</script>

<style>
  a {
        color: #0b0089;
    }

</style>

