<template>
    <span>
        <div class="ui tiny compact positive nopad message">
            <div class="header">
                <a :href="publish.metadata.url">Release published</a>
            </div>
            <p>on {{ moment(publish.metadata.release_date).format('MMMM Do YYYY, h:mm:ss a') }}</p>
        </div>
    </span>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import Vue from 'vue';
import Loader from './Loader.vue'

export default {
    name: 'publish-summary',
    mixins: [ Loader, ],
    props: ['publish','type'],
    mounted() {
    },
    created() {
    },
    beforeDestroy() {
    },
    components: {  },
    data () {
        return {
        }
    },
    computed: {
    },
    watch: {
    },
    methods: {
    }
}
</script>

<style scoped>
.tinytiny {
    padding: .5em 1em .5em;
    font-size: .6rem;
}
.relnotecontent {
    font-size: .8em;
    overflow: visible !important;
}
.envdetails {
    font-size: .8em;
    overflow: visible !important;
}
.darkbluey {
    background-color: #3c515d !important;
}
.nopad {
    padding: 0.2em 1.5em;
}
</style>
