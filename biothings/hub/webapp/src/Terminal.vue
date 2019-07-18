<template>
    <span>
    <div id="terminal" class="ui inverted segment" style="overflow: auto; width: 45vw; max-height: 33vh; max-width: 45vw;">
        <terminal-line v-for="line in buffer" v-bind:line="line"></terminal-line>
        <terminal-prompt></terminal-prompt>
        <div class="red term" v-if="error">{{error}}</div>
    </div>
    </span>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import TerminalLine from './TerminalLine.vue'
import TerminalPrompt from './TerminalPrompt.vue'


// TODO: this could be a setup in the app
const HISTORY = 1000;

export default {
  name: 'terminal',
  props: [],
  components: { TerminalLine, TerminalPrompt },
  mounted () {
    console.log("Terminal mounted");
  },
  created() {
      bus.$on('shell',this.onData);
  },
  beforeDestroy() {
      bus.$off('shell',this.onData);
  },
  ready() {
  },
  data () {
    return  {
        buffer: [],
        error: null,
    }
  },
  watch: {
  },
  methods: {
      onData(line) {
          this.buffer.push(line);
          while(this.buffer.length > HISTORY) {
              this.buffer.shift();
          }
          var d = $('#terminal');
          d.scrollTop(d.prop("scrollHeight"));
      },
  },
}
</script>

<style>
.ui[class*="super compact"].table td {
    padding: 0.1em .6em;
}

.term {
    font-family: monospace;
    font-size: 1em;
    padding:0;
    margin:0;
    letter-spacing:-1px;
    line-height:1;
    white-space: pre-wrap;
}

</style>
