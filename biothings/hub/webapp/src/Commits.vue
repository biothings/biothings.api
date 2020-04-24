<template>
    <span v-if="section && section.upgrade">
        <div>
            A new update is available for branch <code class="branch">{{section.branch}}</code>, with <b class="total">{{section.upgrade.total}}</b> commit(s) available.
            <button class="mini ui icon button" :class="actionable" @click="upgrade(codebase)">
                Upgrade
            </button>
        </div>
        <table class="ui small collapsing table">
            <thead>
                <tr>
                    <th>Commit</th>
                    <th>Date</th>
                    <th>Message</th>
                </tr>
            </thead>
            <tbody>
                <tr v-for="commit in section.upgrade.commits">
                    <td>
                        <span v-if="commit.url">
                            <a :href="commit.url">{{commit.hash}}</a>
                        </span>
                        <span v-else>
                            {{commit.hash}}
                        </span>
                    </td>
                    <td>{{commit.date}}</td>
                    <td>{{commit.message.replace(/\n$/,"")}}</td>
                </tr>
            </tbody>
            <tfoot v-if="section.upgrade.commits.length < section.upgrade.total">
                <tr>
                    <th colspan="3">
                        <i>More commits available in this update, only showing the first ones...</i>
                    </th>
                </tr>
            </tfoot>
        </table>
    </span>
    <span v-else>
        No updates available for now.
    </span>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import Loader from './Loader.vue'
import Actionable from './Actionable.vue'

export default {
    name: 'commits',
    props: ["section","codebase"],
    components: { },
    mixins : [ Loader, Actionable, ],
    mounted () {
    },
    created() {
    },
    updated() {
    },
    beforeDestroy() {
    },
    data () {
        return {}
    },
    computed: {
    },
    methods: {
        upgrade: function(codebase) {
            console.log(`Upgrading code for ${codebase}`);
            var self = this;
            this.loading();
            axios.put(axios.defaults.baseURL + `/code/upgrade/${codebase}`)
            .then(response => {
                console.log(response.data.result)
                self.loaded();
                bus.$emit("restart_hub");
                return true;
            })
            .catch(err => {
                console.log(err);
                self.loaderror(err);
            })
        }
    },
}
</script>

<style scoped>
.branch {
    color: lightgreen !important;
}
.total {
    color: lightblue !important;
}
</style>
