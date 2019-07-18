<template>
    <tr class="record">
        <td :class="level">{{record.msg}}</td>
        <td class="logger">{{record.logger}}</td>
        <td class="ts">{{ts}}</td>
    </tr>
</template>

<script>

export default {
  name: 'log-record',
  props: ["record"],
  computed: {
      level: function() {
          return "level-" + this.record.level.toLowerCase();
      },
      ts: function() {
          return this.record.ts.replace(/\..*/,"");
      }

  }
}
</script>

<style scoped>
.record {font-family: monospace; font-size: 0.90em; padding:0; margin:0; letter-spacing:-1px; line-height:1;}
.ts {color:lightblue;}
.logger {color:bisque;}
.level-info {color: white;}
.level-debug {color: lightgrey;}
.level-warning {color: orange;}
.level-error {color: red;}
</style>
