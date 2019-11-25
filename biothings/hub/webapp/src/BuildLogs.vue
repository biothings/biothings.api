<template>
    <div class="ui small feed" v-if="build.jobs">
        <div class="event">
            <i class="ui hourglass start icon"></i>
            <div class="content">
                <div class="summary">
                    Build starts
                    <div class="date">
                        {{build.started_at | moment('MMM Do YYYY, h:mm:ss a')}}
                    </div>
                </div>
            </div>
        </div>

        <div class="event" v-for="job in build.jobs">
            <i class="ui green checkmark icon" v-if="job.status == 'success'"></i>
            <i class="ui orange exclamation circle icon" v-else-if="job.status == 'canceled'"></i>
            <i class="ui red warning sign icon" v-else-if="job.status == 'failed'"></i>
            <i class="ui pulsing unhide icon" v-else-if="job.status == 'inspecting'"></i>
            <i class="ui pulsing exchange icon" v-else-if="job.status == 'diffing'"></i>
            <i class="ui pulsing bookmark icon" v-else-if="job.status == 'indexing'"></i>
            <i class="ui pulsing cube icon" v-else></i>
            <div class="content">
                <div class="summary">
                    {{job.step}}
                    <div class="date">
                        {{job.time}}
                    </div>
                </div>
                <div class="meta" v-if="job.sources">
                    <i class="database icon"></i>{{job.sources.join(", ")}}
                </div>
                <div class="meta" v-if="job.err">
                    <i class="warning icon"></i>{{job.err}}
                </div>
            </div>

        </div>
    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import Vue from 'vue';

export default {
    name: 'build-logs',
    props: ['build'],
    mounted() {
    },
    beforeDestroy() {
    },
    components: { },
    methods: {
    },
}
</script>

<style scoped>
</style>
