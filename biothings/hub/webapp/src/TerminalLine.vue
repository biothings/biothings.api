<template>
    <pre :class="['term','term' + line.type]">{{line.type == 'input' ? 'hub> ' : ''}}{{line.cmd}}</pre>
</template>

<script>

export default {
  name: 'terminal-line',
  props: ["line"],
  computed: {
  }
}
</script>

<style scoped>
.term {
    font-family: monospace;
    font-size: 1em;
    padding:0;
    margin:0;
    letter-spacing:-1px;
    line-height:1;
    white-space: pre-wrap;
}
.terminput {
    color: white;
    font-weight: bold;
}
.termoutput {
    color: darkgrey;
}
</style>
