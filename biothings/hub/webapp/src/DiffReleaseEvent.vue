<template>
    <div class="event-container">
        <div class="label">
            <i class="large exchange alternate icon"></i>
        </div>
        <div class="content">
            <div class="summary">
                <a>diff</a> with <i>{{release.old.backend || '?'}}</i> has been computed.<br>
                Old version: <b>{{release.old.version}}</b>, current version: <b>{{release.new.version}}</b>
                <div class="date">
                    {{release.created_at | moment("from", "now")}}
                    (<i>on {{moment(release.created_at).format('MMMM Do YYYY, h:mm:ss a') }}</i>)

                </div>
            </div>
            <div class="meta">
                <i class="file alternate icon"></i> {{release.diff.files.length}} diff files created
                <button class="ui mini labeled icon button" @click="applyDiff(release)">
                    <i class="external link square alternate
                        icon"></i>
                    Apply
                </button>
                <button class="ui mini labeled icon button" @click="publish(release)">
                    <i class="share alternate square icon"></i>
                    Publish
                </button>
            </div>
        </div>
    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import Vue from 'vue';

export default {
    name: 'diff-release-event',
    props: ['release'],
    mounted() {
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
    methods: {
        displayError : function() {
        },
        snapshot(release) {
            console.log("TODO snapshot");
            console.log(release);
        },
        publish(release) {
            console.log("TODO publish");
            console.log(release);
        },
    }
}
</script>

<style>
.event-container {
    margin-bottom: 1em;
}
</style>
