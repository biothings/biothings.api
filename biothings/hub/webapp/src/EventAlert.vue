<template>
    <span v-if="event">
        <span class="content" v-if="event.event == 'hub_restart'">
            <alert-restart v-bind:event="event"></alert-restart>
        </span>
        <span class="content" v-else-if="event.event == 'hub_stop'">
            <alert-stop v-bind:event="event"></alert-stop>
        </span>
    </span>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import AlertStop from './AlertStop.vue'
import AlertRestart from './AlertRestart.vue'


export default {
    name: 'event-alert',
	mounted () {
		console.log("EventAlert mounted");
	},
    updated() {
    },
    created() {
        bus.$on('alert',this.onAlert);
    },
    beforeDestroy() {
    },
    watch: {
    },
    computed: {
    },
    data () {
        return  {
            event : null,
        }
    },
    components: { AlertStop, AlertRestart },
    methods: {
        onAlert: function(evt) {
            console.log("on alert");
            console.log(evt);
            this.event = evt;
        },
    }
}
</script>

<style>
</style>
