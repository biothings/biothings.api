<template>
  <div class="ui main container" id="list_sources">
    <div class="ui segment">
      <h2 class="ui right floated header">Data sources</h2>
      <div class="ui clearing divider"></div>
      <div id="data-source-grid" class="ui grid">
        <div class="four wide column" v-for="source in orderBy(sources, 'name')">
          <data-source v-bind:source="source"></data-source>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import axios from 'axios';
import DataSource from './DataSource.vue';
export default {
  name: 'data-source-grid',
  mounted () {
    console.log("DataSourceGrid mounted");
    this.getSourcesStatus();
    setInterval(this.getSourcesStatus,15000);
  },
  data () {
    return  {
      sources: [],
      errors: [],
    }
  },
  components: { DataSource, },
  methods: {
    getSourcesStatus: function() {
      console.log("getSourcesStatus");
      axios.get('http://localhost:7042/source')
      .then(response => {
        this.sources = response.data.result;
      })
      .catch(err => {
        console.log("Error getting sources information: " + err);
      })
    }
  }
  }
</script>

<style>
</style>
