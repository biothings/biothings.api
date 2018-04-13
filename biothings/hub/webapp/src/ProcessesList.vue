<template>
    <div v-if="processes.all">
        <div class="ui three columns centered grid">
            <div class="three wide column">
                <div class="ui tiny label" v-if="processes.running">
                    Running<div class="detail">{{processes.running.length}}</div>
                </div>
            </div>
            <div class="three wide column">
                <div class="ui tiny label" v-if="processes.pending">
                    Pending<div class="detail">{{processes.pending.length}}</div>
                </div>
            </div>
            <div class="three wide column">
                <div class="ui tiny label">
                    Max<div class="detail" v-if="processes">{{processes.max}}</div>
                </div>
            </div>
        </div>
        <table class="ui nowrap compact celled table" v-if="Object.keys(processes.all).length">
            <tr v-for="(process, pid) in processes.all"
                v-bind:class="[process.cpu.status == 'running'? 'positive' : '', 'nowrap']">
                <td>
                    <div v-bind:data-tooltip="process.cpu.status">
                        <i v-if="process.cpu.status == 'running'" class="ui notched circle loading icon"></i>
                        <i v-else-if="process.cpu.status == 'sleeping'" class="ui hotel icon"></i>
                        <i v-else class="ui wait icon"></i>
                    </div>
                </td>
                <td class="right aligned">{{pid}}</td>
                <td v-if="process.job">{{process.job.category}}</td>
                <td v-if="process.job">{{process.job.step}}</td>
                <td v-if="process.job">{{process.job.source}}</td>
                <td v-if="process.job">{{process.job.duration}}</td>
                <td class="center aligned" v-else colspan="4"><i>ready</i></td>
                <td class="right aligned">{{process.cpu.percent}}%</td>
                <td class="right aligned">{{process.memory.size | pretty_size }}</td>
            </tr>
        </table>
        <div v-else>No process to show</div>
    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'

export default {
  name: 'processes-list',
  props: ['processes'],
  methods: {
  },
  mounted () {
    console.log("ProcessesList mounted");
    $('.ui.toggle.checkbox')
    .checkbox()
    ;
  },
  ready() {
    console.log("process item ready");
  },
  watch: {
  },
}
</script>

<style>
</style>
