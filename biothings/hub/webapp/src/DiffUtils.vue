<template>
</template>

<script>

import axios from 'axios'
import bus from './bus.js'
import Utils from './Utils.vue'

export default {
    mixins: [Utils],
    methods: {
        diffMapping: function(map_elem_id_left, map_elem_id_right,subsrc) {
            var lefthtml = $(`#${subsrc}-${map_elem_id_left}`).html();
            var leftjson = this.html2json(lefthtml);
            var righthtml = $(`#${subsrc}-${map_elem_id_right}`).html();
            var rightjson = this.html2json(righthtml);
            axios.post(axios.defaults.baseURL + `/jsondiff`,{"src" : leftjson, "dst" : rightjson})
            .then(response => {
                console.log(response.data.result)
                bus.$emit("diffed",response.data.result);
                bus.$emit("show_diffed");
            })
            .catch(err => {
                console.log("Error diffing mappings: " + err);
            })
        },
    },
}

</script>
