<template>
    <div :class="['ui tiny',color,'message']">
        <div><div :class="['ui',color,'horizontal label']">{{event.name}}</div>
            <div class="ui right floated"><i>{{event.asctime}}</i></div>
        </div>
        <p>{{event.msg}}</p>
    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'


export default {
    name: 'event-message',
    props: ['event'],
    mounted () {
        console.log("EventMessage mounted");
	},
    updated() {
    },
    created() {
    },
    beforeDestroy() {
    },
    watch: {
    },
    computed: {
        color: function () {
            switch(this.event.level.toUpperCase()) {
                case "ERROR":
                    return "red";
                    break;
                case "WARNING":
                    return "orange";
                    break;
                case "INFO":
                    return "green";
                    break;
                case "DEBUG":
                    return "blue";
                    break;
                default:
                    console.log("onela");
                    return "black";
            }
        },
    },
    data () {
        return  {
        }
    },
    components: {  },
    methods: {
    }
}
</script>

<style>
</style>
