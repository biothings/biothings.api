<template>
    <div class="ui diff modal">
        <div class="ui header">
            <i class="exchange icon"></i>
            JSON diff results
        </div>
        <div class="content">
            <p>Operations describe what is required to get from the data on the left, to the data on the right</p>
            <json-diff-results></json-diff-results>
        </div>
        <div class="actions">
            <div class="ui green ok button">
                <i class="checkmark icon"></i>
                OK
            </div>
        </div>
    </div>
</template>

<script>

import axios from 'axios'
import bus from './bus.js'
import JsonDiffResults from './JsonDiffResults.vue'

export default {
    components : { JsonDiffResults, },
    created() {
        bus.$on("show_diffed",this.showDiffed);
    },
    beforeDestroy() {
        bus.$off("show_diffed",this.showDiffed);
    },
    methods: {
        showDiffed : function() {
            $('.ui.diff.modal').modal({
                observeChanges: true,
            })
            .modal("show")
        },
    },
}

</script>
