<template>
    <span>
    <div class="term terminput"><span>hub&gt;&nbsp;</span><input id="termcommand" class="term terminput termcommand"
               type="text"
               placeholder="Type a command..."
               v-on:keydown.enter="send"
               autofocus>
        </input>
    </div>
    </span>
</template>

<script>
import axios from 'axios'

export default {
  name: 'terminal-prompt',
  props: ["prompt"],
  computed: {
  },
  data () {
    return  {
    }
  },
  methods: {
      send(evt) {
          var cmd = evt.target.value;
          this.$parent.error = null;
          var self = this;
          axios.put(axios.defaults.baseURL + `/shell`,{"cmd":cmd},{validateStatus: false})
          .then(response => {
              // axios doesn't display error when response isn't 200, need to deal with that manually
              // TODO: this would def benefit all api calls...
              if(response.status >= 200 && response.status < 300) {
                  $("#termcommand").val("");
              } else {
                  if(response.data.error) {
                      self.$parent.error = response.data.error;
                  } else {
                      self.$parent.error = response.statusText;
                  }
              }
              var d = $('#terminal');
              d.scrollTop(d.prop("scrollHeight"));
          })
          .catch(err => {
              if(err.message) {
                  self.$parent.error = err.message;
              } else {
                  self.$parent.error = "Unknown error";
              }
          });
      }
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
.termcommand {
    width: 90%;
    background: transparent;
    outline: none;
    border: 0;
}
.termprompt{
    color: blue;
}
</style>
