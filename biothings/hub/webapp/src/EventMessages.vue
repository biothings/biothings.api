<template>
	<div>
		<button class="mini circular ui icon events button">
			<i class="bullhorn icon"></i>
			<a :class="['ui mini',notifnumcolor,'circular label']">{{events.length}}</a>
		</button>
		<div class="ui messages popup top left transition hidden">
			<div class="ui messages list" id="messages" v-if="events.length > 0">
                <div class="item event" v-for="evt in events" :key="evt._id">
					<event-message v-bind:event="evt"></event-message>
				</div>
			</div>
			<div v-else>No new notifications</div>
		</div>
	</div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'

import EventMessage from './EventMessage.vue';

const MAX_EVENTS = 20;

export default {
    name: 'event-messages',
    mounted () {
        console.log("EventMessages mounted");
		$('.events.button').popup({
			popup: $('.events.popup'),
			on: 'click' ,
			onVisible: () => {this.hasnew = false;},
			lastResort: 'bottom right',
		});
	},
	components: { EventMessage, },
    updated() {
        // there's some kind of race-condition regarding dropdown init, if
        // in mounted() they won't get init, prob. because data changed and needs to
        // be re-rendered
    },
    created() {
        bus.$on('change_event',this.onEventChanged);
    },
    beforeDestroy() {
        // hacky to remove modal from div outside of app, preventing having more than one
        // modal displayed when getting back to that page. https://github.com/Semantic-Org/Semantic-UI/issues/4049
        bus.$off('change_event',this.onEventChanged);
    },
    watch: {
    },
    data () {
        return  {
			//events: [],
			events: [{'pid': 31239, '_id': 1523566534.4344828, 'pname': 'mainprocess', 'name': 'cgi_upload', 'asctime': '13:55:34', 'msg': 'success [steps=data,post,master,clean]', 'level': 'info'}],
			hasnew: false,
        }
    },
    computed: {
        notifnumcolor: function () {
			if(this.hasnew)
				return "red";
		}
	},
    methods: {
        onEventChanged: function(_id=null,op=null,data=null) {
            if(data != null) {
				if(data.name == "hub") {
					// too generic, skip
					return;
				}
				this.events.unshift(data)
				this.hasnew = true;
				if(this.events.length > MAX_EVENTS)
					this.events.pop();
            }
        },
    }
}
</script>

<style>
.ui.sidebar {
    overflow: visible !important;
}
.item.event {
    min-width: 25em;
}
</style>
