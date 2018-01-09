<template>
  <div class="ui center aligned tiny">
    <!-- commands -->
    <button
        class="ui compact labeled icon commands button tiny"
        v-if="commands"
        >
        <i class="settings icon"></i>
        {{Object.keys(commands).length}}
    </button>
    <div class="ui commands popup top left transition hidden">
        <commands-list v-bind:commands="commands"></commands-list>
    </div>
    <!-- jobs (processes) -->
    <button
        class="ui compact labeled icon button tiny"
      data-tooltip="Number of running processes"
      data-position="bottom center"
      v-if="job_manager.queue"
      v-on:click="processDetails">
      <i class="rocket icon"></i>
      {{job_manager.queue.process.running.length}}/{{job_manager.queue.process.max}}
    </button>
    <!-- jobs (threads) -->
    <button
        class="ui compact labeled icon button tiny"
      data-tooltip="Number of running threads"
      data-position="bottom center"
      v-if="job_manager.queue"
      v-on:click="threadDetails">
      <i class="lightning icon"></i>
      {{job_manager.queue.thread.running.length}}/{{job_manager.queue.thread.max}}
    </button>
    <!-- jobs (pendings) -->
    <button
        class="ui compact labeled icon button tiny"
      data-tooltip="Number of queued jobs"
      data-position="bottom center"
      v-if="job_manager.queue"
      v-on:click="pendingDetails">
      <i class="hourglass start icon"></i>
      {{job_manager.queue.thread.pending.length + job_manager.queue.process.pending.length }}
    </button>
    <!-- memory -->
    <div class="ui small grey label"
      data-tooltip="Amount of memory hub is currently using"
      data-position="bottom center"
      v-if="job_manager.queue">
      <i class="right microchip icon"></i>
      {{ job_manager.memory | pretty_size}}
    </div>
  </div>
</template>

<script>
import axios from 'axios';
import CommandsList from './CommandsList.vue';
import bus from './bus.js';

export default {
  name: 'job-summary',
  mounted () {
    console.log("mounted");
    this.getJobSummary();
    this.refreshCommands();
    setInterval(this.getJobSummary,10000);
    setInterval(this.refreshCommands,10000);
    // setup menu
    $('.commands.button').popup({popup: $('.commands.popup'), on: 'click' });
  },
  created() {
      bus.$on('refresh_commands',this.refreshCommands);
  },
  data () {
    return  {
      job_manager : {},
      commands : {},
      show_allcmds : false,
      errors: [],
    }
  },
  components: { CommandsList, },
  methods: {
    getJobSummary: function() {
      axios.get(axios.defaults.baseURL + '/job_manager')
      .then(response => {
        this.job_manager = response.data.result;
      })
      .catch(err => {
        console.log("Error getting job manager information: " + err);
      })
    },
    refreshCommands: function(all) {
      // only update if explicitely passed (from event)
      if('undefined' !== typeof all)
        this.show_allcmds = all;
      var url = axios.defaults.baseURL + '/commands';
      if(!this.show_allcmds)
        url += "?running=1";
      axios.get(url)
      .then(response => {
        this.commands = response.data.result;
        console.log(this.commands);
      })
      .catch(err => {
        console.log("Error getting runnings commands: " + err);
      })
    },
    processDetails: function() {
        console.log(this.processes);
    },
    threadDetails: function() {
        console.log(this.threads);
    },
    pendingDetails: function() {
        console.log(this.threads);
    },
  }
}
</script>

<style>
</style>
