<template>
  <div class="ui center aligned tiny">
    <!-- commands -->
    <button
        class="ui compact labeled icon commands button tiny">
        <i class="settings icon"></i>
        {{num_commands}}
    </button>
    <div class="ui commands popup top left transition hidden">
        <commands-list></commands-list>
    </div>
    <!-- jobs (processes) -->
    <button
        class="ui compact labeled icon processes button tiny"
        >
        <i class="rocket icon"></i>
        {{job_manager.queue ? job_manager.queue.process.running.length : '?'}}/{{job_manager.queue ? job_manager.queue.process.max : '?'}}
    </button>
    <div class="ui processes popup top left transition hidden">
        <processes-list v-bind:processes="processes"></processes-list>
    </div>
    <!-- jobs (threads) -->
    <button
        class="ui compact labeled icon threads button tiny">
      <i class="lightning icon"></i>
      {{job_manager.queue ? job_manager.queue.thread.running.length : '?'}}/{{job_manager.queue ? job_manager.queue.thread.max : '?' }}
    </button>
    <div class="ui threads popup top left transition hidden">
        <threads-list v-bind:threads="threads"></threads-list>
    </div>
    <!-- jobs (pendings) -->
    <div class="ui small grey label"
      data-tooltip="Number of queued jobs"
      data-position="bottom center"
      v-if="job_manager.queue">
      <i class="hourglass start icon"></i>
      {{job_manager.queue.thread.pending.length + job_manager.queue.process.pending.length }}
    </div>
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
import ProcessesList from './ProcessesList.vue';
import ThreadsList from './ThreadsList.vue';
import bus from './bus.js';

const POLL_JOBS_INTERVAL = 3000; // when needed, poll job manager every X ms

export default {
  name: 'job-summary',
  mounted () {
    console.log("mounted");
    this.getJobSummary();
    // setup menu
    $('.processes.button').popup({
        popup: $('.processes.popup'),
        on: 'click' ,
        lastResort: 'bottom right',
        onVisible: () => {this.onJobsOpened(true)},
        onHide: () => {this.onJobsOpened(false)},
    });
    $('.threads.button').popup({
        popup: $('.threads.popup'),
        on: 'click' ,
        lastResort: 'bottom right',
        onVisible: () => {this.onJobsOpened(true)},
        onHide: () => {this.onJobsOpened(false)},
    });
  },
  created() {
      bus.$on('num_commands',this.updateNumCommands);
  },
  beforeDestroy() {
      bus.$off('num_commands',this.updateNumCommands);
  },
  data () {
    return  {
      num_commands : 0,
      job_manager : {},
      processes : {},
      threads : {},
      show_allcmds : false,
      jobs_interval: null,
      //errors: [],
    }
  },
  watch: {
      num_commands: function(newv,oldv) {
          if(newv == 0) {
              console.log("Stop polling jobs"),
              this.stopPollJobs();
          }
          if(oldv == 0 && newv > 0) {
              console.log("Start polling jobs"),
              this.startPollJobs();
          }
      },
  },
  components: { CommandsList, ProcessesList, ThreadsList, },
  methods: {
    updateNumCommands: function(num) {
        this.num_commands = num;
        this.getJobSummary();
    },
    startPollJobs: function() {
        if(this.jobs_interval == null)
            this.jobs_interval = setInterval(this.getJobSummary,POLL_JOBS_INTERVAL);
        else
            console.log("Needs to poll jobs but interval already set !!!");
    },
    stopPollJobs: function() {
        clearInterval(this.jobs_interval);
        this.jobs_interval = null;
    },
    getJobSummary: function() {
      axios.get(axios.defaults.baseURL + '/job_manager')
      .then(response => {
        this.job_manager = response.data.result;
        this.processes = this.job_manager.queue.process;
        this.threads = this.job_manager.queue.thread;
      })
      .catch(err => {
        console.log("Error getting job manager information: " + err);
      })
    },
    onJobsOpened: function(do_poll) {
        console.log(do_poll);
        if(do_poll) {
            // we force the poll
            this.getJobSummary();
            this.startPollJobs();
        } else if(this.num_commands == 0) {
            // but we let component to decide whether to continue polling or not
            this.stopPollJobs();
        }
    },
  }
}
</script>

<style>
</style>
