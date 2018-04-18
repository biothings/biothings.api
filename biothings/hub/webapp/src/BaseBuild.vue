<template>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'

export default {
    name: 'base-build',
    // Note: we don't declare "build", it must be defined in subclass/mixed
    // (sometimes it's a prop, sometimes it's a data field
    mounted () {
    },
    components: { },
    created() {
    },
    beforeDestroy() {
    },
    computed: {
    },
    methods: {
        inspect: function() {
            var self = this;
            $(`#inspect-${this.build._id}.ui.basic.inspect.modal`)
            .modal("setting", {
                onApprove: function () {
                    var modes = $(`#inspect-${self.build._id}`).find("#select-mode").val();
                    axios.put(axios.defaults.baseURL + '/inspect',
                              {"data_provider" : self.build._id,"mode":modes})
                    .then(response => {
                        console.log(response.data.result)
                    })
                    .catch(err => {
                        console.log("Error getting job manager information: " + err);
                    })
                }
            })
            .modal("show");
        },
    },
}
</script>

<style>
  a {
        color: #0b0089;
    }

</style>

