<template>
  <div id="data-source" class="ui card">
    <div class="content">

      <!-- locked -->
      <i class="right floated lock icon blue"
        v-if="source.locked"></i>

      <!-- in progress -->
      <i class="right floated database icon pulsing"
        v-if="source.upload && source.upload.status == 'uploading'"></i>
      <i class="right floated cloud download icon pulsing"
        v-if="source.download && source.download.status == 'downloading'"></i>

      <!-- error -->
      <div class="ui"
        v-bind:data-tooltip="displayError()">
      <i class="right floated red alarm icon pulsing"
        v-if="(source.upload && source.upload.status == 'failed')
        || (source.download && source.download.status == 'failed')">
      </i></div>

      <div class="left aligned header" v-if="source.name">{{ source.name | splitjoin | capitalize }}</div>
      <div class="meta">
        <span class="right floated time" v-if="source.download && source.download.started_at">{{ source.download.started_at | moment("from", "now") }}</span>
        <span class="right floated time" v-else>Never</span>
        <span class="left floated category">{{ source.release }}</span>
      </div>
      <div class="left aligned description">
        <p>
          <div class="ui clearing divider"></div>
          <div>
            <i class="file outline icon"></i>
            {{ source.count | currency('',0) }} document{{ source.count &gt; 1 ? "s" : "" }}
          </div>
        </p>
      </div>
    </div>
    <div class="extra content">
      <div class="ui icon buttons left floated mini">
        <button class="ui button" v-on:click="dump" v-if="source.download">
          <i class="download cloud icon"></i>
        </button>
        <button class="ui button" v-on:click="upload" v-if="source.upload">
          <i class="database icon"></i>
        </button>
      </div>
      <div class="ui icon buttons right floated mini">
        <button class="ui disabled button"><i class="configure icon"></i></button>
      </div>
    </div>
  </div>
</template>

<script>
import axios from 'axios';
export default {
  name: 'data-source',
  props: ['source'],
  methods: {
    displayError : function() {
      var errs = [];
      if (this.source.download && this.source.download.status == "failed")
        errs.push("Download failed: " + this.source.download.error);
      if (this.source.upload && this.source.upload.status == "failed")
        errs.push("Upload failed: " + this.source.upload.error);
      return errs.join("<br>");
    },
    dump: function() {
      axios.post('http://localhost:7042/source/' + this.source.name + "/dump")
      .then(response => {
        console.log(response.data.result)
        this.$parent.getSourcesStatus();
      })
      .catch(err => {
        console.log("Error getting job manager information: " + err);
      })
    },
    upload: function() {
      axios.post('http://localhost:7042/source/' + this.source.name + "/upload")
      .then(response => {
        console.log(response.data.result)
        this.$parent.getSourcesStatus();
      })
      .catch(err => {
        console.log("Error getting job manager information: " + err);
      })
    }
  },
}
</script>

<style>
  @keyframes pulse {
    0% {transform: scale(1, 1);}
    50% {transform: scale(1.2, 1.2);}
    100% {transform: scale(1, 1);}
  }

  .pulsing {
    animation: pulse 1s linear infinite;
  }
</style>
