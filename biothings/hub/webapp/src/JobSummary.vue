<template>
  <div class="ui center aligned tiny">
    <!-- jobs (processes) -->
    <div class="ui small grey label"
      data-tooltip="Number of running processes"
      data-position="bottom center"
      v-if="job_manager.queue">
      <i class="rocket icon"></i>
      <div class="detail">{{job_manager.queue.process.running.length}}/{{job_manager.queue.process.max}}
      </div>
    </div>
    <!-- jobs (threads) -->
    <div class="ui small grey label"
      data-tooltip="Number of running threads"
      data-position="bottom center"
      v-if="job_manager.queue">
      <i class="lightning icon"></i>
      <div class="detail">{{job_manager.queue.thread.running.length}}/{{job_manager.queue.thread.max}}
      </div>
    </div>
    <!-- jobs (pendings) -->
    <div class="ui small grey label"
      data-tooltip="Number of queued jobs"
      data-position="bottom center"
      v-if="job_manager.queue">
      <i class="hourglass start icon"></i>
      <div class="detail">{{job_manager.queue.thread.pending.length + job_manager.queue.process.pending.length }}
      </div>
    </div>
    <!-- memory -->
    <div class="ui small grey label"
      data-tooltip="Amount of memory hub is currently using"
      data-position="bottom center"
      v-if="job_manager.queue">
      <i class="right microchip icon"></i>
      {{ job_manager.memory | pretty_size}}
    </div>
    <button class="ui compact button tiny" v-on:click="jobDetails">
      <i class="tasks icon"></i>
    </button>
  </div>
</template>

<script>
import axios from 'axios';

export default {
  name: 'job-summary',
  mounted () {
    console.log("mounted");
    this.getJobSummary();
    /*setInterval(function () {
      console.log("on set interval" + this);
      this.getJobSummary();
    }.bind(this), 10000);*/
    setInterval(this.getJobSummary,10000);
  },
  data () {
    return  {
      job_manager : {},//"queue" : {"process":{},"thread":{}}},
      errors: [],
    }
  },
  methods: {
    getJobSummary: function() {
      axios.get(axios.defaults.baseURL + '/manager/job_manager')
      .then(response => {
        this.job_manager = response.data.result.job_manager;
      })
      .catch(err => {
        console.log("Error getting job manager information: " + err);
      })
    },
    jobDetails: function() {
      axios.get(axios.defaults.baseURL + '/manager/job_manager?detailed=1')
      .then(response => {
        this.job_manager = response.data.result.job_manager;
      })
      .catch(err => {
        console.log("Error getting job manager information: " + err);
      })
    },
  }
}
</script>

<style>
</style>
