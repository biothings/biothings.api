<template>
    <span>
        <div class="ui feed"  v-if="releases">
            <div class="event" v-for="rel in releases">
                <index-release-event :release="rel" v-if="rel.index_name"></index-release-event>
                <diff-release-event :release="rel" v-if="rel.diff"></diff-release-event>

            </div>
        </div>
        <div>
            No release found
        </div>
    </span>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import Vue from 'vue';
import IndexReleaseEvent from './IndexReleaseEvent.vue';
import DiffReleaseEvent from './DiffReleaseEvent.vue';

export default {
    name: 'build-releases',
    props: ['build'],
    mounted() {
    },
    beforeDestroy() {
    },
    components: { IndexReleaseEvent, DiffReleaseEvent},
    data () {
        return {
        }
    },
    computed: {
        releases: function () {
            // sort index and diff releases by dates
            var _releases = [];
            if(this.build.index) {
                _releases = _releases.concat(Object.values(this.build.index));
            }
            if(this.build.diff)
                _releases = _releases.concat(Object.values(this.build.diff));
            _releases.reverse(function(a,b) {
                var da = a.created_at && Date.parse(a.created_at)
                var db = b.created_at && Date.parse(b.created_at)
                console.log(`da ${da} db ${db}`);
                return da - db;
            });
            return _releases;
        }
    },
    methods: {
        displayError : function() {
        },
    }
}
</script>

<style>
</style>
