<template>
    <div>
        <div class="ui grid">
            <div class="four wide column">
                <div class="ui tiny label" v-if="threads.running">
                    Running<div class="detail">{{threads.running.length}}</div>
                </div>
            </div>
            <div class="four wide column">
                <div class="ui tiny label" v-if="threads.pending">
                    Pending<div class="detail">{{threads.pending.length}}</div>
                </div>
            </div>
            <div class="four wide column">
                <div class="ui tiny label" v-if="threads.all">
                    Ready<div class="detail">{{Object.keys(threads.all).length}}</div>
                </div>
            </div>
            <div class="four wide column">
                <div class="ui tiny label">
                    Max<div class="detail">{{threads.max}}</div>
                </div>
            </div>
        </div>
        <table class="ui compact celled table" v-if="threads.running">
            <tr v-for="(thread, pid) in threads.all" v-bind:class="[thread.job ? 'positive' : '', 'nowrap']">
                <td class="nowrap">
                    <div>
                        <i v-if="thread.job" class="pulsing ui lightning icon"></i>
                        <i v-else class="ui hotel icon"></i>
                    </div>
                </td>
                <td class="right aligned">{{pid}}</td>
                <td class="nowrap" v-if="thread.job">{{thread.job.category}}</td>
                <td class="nowrap" v-if="thread.job">{{thread.job.step}}</td>
                <td class="nowrap" v-if="thread.job">{{thread.job.source}}</td>
                <td class="nowrap" v-if="thread.job">{{thread.job.duration}}</td>
                <td class="nowrap center aligned" v-else colspan="4"></td>
            </tr>
        </table>
        <div v-else>No thread to show</div>
    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'

export default {
  name: 'threads-list',
  props: ['threads'],
  methods: {
  },
  mounted () {
    console.log("threads list mounted");
    console.log(this.threads);
    $('.ui.toggle.checkbox')
    .checkbox()
    ;
  },
  ready() {
    console.log("thread item ready");
  },
  watch: {
  },
}
</script>

<style>
</style>
