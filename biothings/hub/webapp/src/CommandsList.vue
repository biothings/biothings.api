<template>
    <div>
        <div class="ui right floated header">
            <div class="ui toggle checkbox">
                <label>Show all</label>
                <input v-model="allcmds" type="checkbox" tabindex="0" class="hidden">
            </div>
        </div>
        <table class="ui compact celled table" v-if="Object.keys(commands).length">
            <tr v-for="command in orderBy(Object.values(commands),'id',-1)"
                v-bind:class="[ command.is_done & command.failed ? 'negative' : '', command.is_done & !command.failed ? 'positive': '' ]"
                >
                <td>
                    <i v-if="command.is_done" v-bind:class="[ command.is_done & command.failed ? 'attention icon' : '', command.is_done & !command.failed ? 'icon checkmark': '' ]"></i>
                    <div v-else class="ui active tiny inline loader"></div>
                </td>
                <td class="right aligned" v-bind:data-tooltip="'kllklk'">{{command.id}}</td>
                <td>{{command.cmd}}</td>
                <td class="nowrap" v-if="command.is_done">{{command.duration}}</td>
                <td class="nowrap" v-else>{{new Date(0).setUTCSeconds(command.started_at) | moment("from", "now")}}</td>
            </tr>
        </table>
        <div v-else>No command to show</div>
    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'

export default {
  name: 'commands-list',
  props: ['commands'],
  methods: {
      showAllToggled() {
          console.log(`toggled, ${this.allcmds}`);
          bus.$emit("refresh_commands",this.allcmds);
      }
  },
  mounted () {
    console.log("command item mounted");
    console.log(this.command);
    $('.ui.toggle.checkbox')
    .checkbox()
    ;
  },
  ready() {
    console.log("command item ready");
  },
  data () {
    return  {
        allcmds : false,
    }
  },
  watch: {
      allcmds: 'showAllToggled'
  },
}
</script>

<style>
table .nowrap {
        white-space:  nowrap;
    }
</style>
