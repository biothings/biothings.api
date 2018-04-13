<template>
    <div :class="['ui tiny',color,'message']">
        <i class="close icon" :data-id="event._id"></i>
        <div><div :class="['ui',color,'horizontal label']">{{event.name}}</div>
        </div>
        <div>{{event.msg}}<div class="ui right floated"><i>{{event.asctime}}</i></div>
    </div>
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
        var self = this;
        $('.message .close')
        .on('click', function() {

            var evtid = $(this).attr("data-id");
            // filter proper event
            if(evtid == self.event._id) {
                $(this)
                .closest('.message')
                .transition('fade');
                console.log(evtid);
                bus.$emit("event_deleted",evtid);
            }
        })
        ;
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
