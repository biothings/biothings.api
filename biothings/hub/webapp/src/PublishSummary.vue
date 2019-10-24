<template>
    <span>
        <div class="ui tiny compact positive nopad message">
            <div class="header">
                <div class="ui horizontal bulleted list">
                    <div class="item">
                        Release published
                    </div>
                    <div class="item">
                        <a :href="version_url">version.json</a>
                    </div>
                </div>
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
        version_url: function() {
            var url = new URL(this.publish.metadata.url);
            var parts = url.pathname.split("/").slice(0,-1);
            parts.push("versions.json");
            url.pathname = parts.join("/");
            return url.toString();
        }
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
