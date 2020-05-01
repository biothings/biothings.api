<template>
    <div class="ui fluid container">
            <div class="ui sections accordion">
                <div class="section title">
                    <i class="dropdown icon"></i>
                    Application
                    <i class="small upgrade sync icon" v-if="app_version && app_version.upgrade"></i>
                </div>
                <div class="content">
                    <p class="transition hidden">
                        <commits v-bind:section="app_version" v-bind:codebase="'application'"></commits>
                    </p>
                </div>
                <div class="section title">
                    <i class="dropdown icon"></i>
                    BioThings SDK
                    <i class="small upgrade sync icon" v-if="biothings_version && biothings_version.upgrade"></i>
                </div>
                <div class="content">
                    <p class="transition hidden">
                        <commits v-bind:section="biothings_version" v-bind:codebase="'biothings_sdk'"></commits>
                    </p>
                </div>
            </div>

    </div>
</template>

<script>
import Vue from 'vue';
import axios from 'axios'
import bus from './bus.js'
import Commits from './Commits.vue'

export default {
    name: 'system-upgrade',
    props: ["app_version","biothings_version"],
    components: { Commits, },
    mixins : [ ],
    mounted () {
        $('.ui.sections.accordion').accordion();
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
    },
    methods: {
    },
}
</script>

<style scoped>
.ui.config.segment {
    color: black;
}
.configmenu {
    padding: 0;
}

.configmenu .right.item {
    padding: 0;
}

.configmenu .right.item .item {
    padding: 0;
}

.section.title {
    color: lightblue !important;
    font-size: 1.2em !important;
}

.upgrade {
    color: #d95959;
}
</style>
