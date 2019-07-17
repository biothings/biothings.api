<template>
    <div id="logviewer" class="ui inverted segment" style="overflow: auto; width: 50vw; max-height: 33vh; max-width: 50vw;">
        <table class="ui single line super compact inverted table" v-if="records.length">
            <tbody>
                <log-record v-for="record of records" v-bind:record="record"></log-record>
            </tbody>
        </table>
        <div v-else>
            No logs yet...
        </div>
    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import LogRecord from './LogRecord.vue'

// TODO: this could be a setup in the app
const MAX_RECORDS = 1000;

export default {
  name: 'log-viewer',
  props: [],
  components: { LogRecord },
  mounted () {
    console.log("LogViewer mounted");
  },
  created() {
      bus.$on('log',this.onLog);
  },
  beforeDestroy() {
      bus.$off('log',this.onLog);
  },
  ready() {
  },
  data () {
    return  {
        records: [],
    }
  },
  watch: {
  },
  methods: {
      onLog(record) {
          this.records.push(record);
          while(this.records.length > MAX_RECORDS) {
              this.records.shift();
          }
          var d = $('#logviewer');
          d.scrollTop(d.prop("scrollHeight"));
      },
  },
}
</script>

<style>
table .nowrap {
        white-space:  nowrap;
    }
    .ui[class*="super compact"].table td {
        padding: 0.1em .6em;
    }

</style>
