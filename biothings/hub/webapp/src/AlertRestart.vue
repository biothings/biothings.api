<template>
    <div class="ui mini alert modal">
        <div class="header">
            {{event.msg}}
        </div>
        <div class="content">
            <p>Do you want to reconnect ?</p>
        </div>
        <div class="actions" >
            <div class="ui negative button">
                No
            </div>
            <div class="ui positive right labeled icon button">
                Yes
                <i class="checkmark icon"></i>
            </div>
        </div>
    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'

export default {
    name: 'alert-restart',
    props: ["event"],
	mounted () {
		console.log("AlertRestart mounted");
        $('.mini.alert.modal')
        .modal({
            closable  : false,
            onApprove : function() {
                bus.$emit("reconnect");
            }
        })
        .modal("show");
	},
    updated() {
    },
    created() {
    },
    beforeDestroy() {
		$('.mini.alert.modal').remove();
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
