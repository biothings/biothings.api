<template>
    <div class="event-container">
        <div class="content">
            <div class="summary">
                <i class="ui bookmark icon"></i>
                Index
                <b class="user">
                    {{release.index_name}}
                </b> was created on <b>{{release.environment}}</b> environment (<b>{{release.host}}</b>)
                <div class="date">
                    {{release.created_at | moment("from", "now")}}
                    (<i>on {{moment(release.created_at).format('MMMM Do YYYY, h:mm:ss a') }}</i>)

                </div>
            </div>
            <div class="meta">
                <i class="file alternate icon"></i> {{ num_indexed | formatNumber }} documents indexed
                <release-note-summary :build="build" :release="release" :type="type"></release-note-summary>
                <!--button :class="[release.snapshot ? 'disabled' : '','ui mini labeled icon button']"
                        @click="snapshot(release)">
                    <i class="bookmark icon"></i>
                    Snapshot
                </button>
                <button :class="[release.snapshot ? '' : 'disabled', 'ui mini labeled icon button']"
                        @click="publish(release)">
                    <i class="share alternate square icon"></i>
                    Publish
                </button-->
            </div>
        </div>
    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import Vue from 'vue';
import ReleaseNoteSummary from './ReleaseNoteSummary.vue';

export default {
    name: 'index-release-event',
    props: ['release','build','type'],
    mounted() {
    },
    beforeDestroy() {
    },
    components: { ReleaseNoteSummary, },
    data () {
        return {
        }
    },
    computed: {
        num_indexed: function() {
            return this.release.count || 0;
        }
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

<style scoped>
.event-container {
    margin-bottom: 1em;
    width: inherit;

}
.pubactions {
    padding: 0 !important;
}
</style>
