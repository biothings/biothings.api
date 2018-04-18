<template>
    <div class="ui mini alert modal">
        <div class="header">
            {{event.msg}}
        </div>
        <div class="content">
            <p>You have been disconnected</p>
        </div>
        <div class="actions" >
            <div class="ui positive right labeled icon button">
                OK
                <i class="checkmark icon"></i>
            </div>
        </div>
    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'

export default {
    name: 'alert-stop',
    props: ["event"],
	mounted () {
		console.log("AlertStop mounted");
		$('.mini.alert.modal')
		.modal('setting', 'closable', false)
        .modal("show");
	},
    updated() {
    },
    created() {
    },
    beforeDestroy() {
        bus.$on('alert',this.onAlert);
    },
    watch: {
    },
    computed: {
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
