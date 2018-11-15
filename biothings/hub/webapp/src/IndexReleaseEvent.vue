<template>
    <div class="event-container">
        <div class="label">
            <i class="ui bookmark icon"></i>
        </div>
        <div class="content">
            <div class="summary">
                Index
                <a class="user">
                    {{release.index_name}}
                </a> was created on <b>{{release.environment}}</b> environment (<a>{{release.host}}</a>)
                <div class="date">
                    {{release.created_at | moment("from", "now")}}
                    (<i>on {{moment(release.created_at).format('MMMM Do YYYY, h:mm:ss a') }}</i>)

                </div>
            </div>
            <div class="meta">
                <i class="file alternate icon"></i> {{num_indexed}} documents indexed
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

export default {
    name: 'index-release-event',
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

<style>
.event-container {
    margin-bottom: 1em;
}
</style>
