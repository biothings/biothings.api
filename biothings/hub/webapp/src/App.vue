<template>
  <div id="app">
    <div class="ui fixed inverted menu">
      <div class="ui container">
        <a href="#" class="header item">
          <img class="logo" src="./assets/biothings_logo.png">
          Biothings Hub
        </a>
        <a href="#" class="item">Home</a>
        <!--div class="ui simple dropdown item">
        Dropdown <i class="dropdown icon"></i>
        <div class="menu">
          <a class="item" href="#">Link Item</a>
          <a class="item" href="#">Link Item</a>
          <div class="divider"></div>
          <div class="header">Header Item</div>
          <div class="item">
            <i class="dropdown icon"></i>
            Sub Menu
            <div class="menu">
              <a class="item" href="#">Link Item</a>
              <a class="item" href="#">Link Item</a>
            </div>
          </div>
          <a class="item" href="#">Link Item</a>
        </div>
        </div-->
      </div>
    </div>

    <data-source-grid v-bind:sources="sources"></data-source-grid>

  </div>
</template>

<script>
import axios from 'axios';

import Vue2Filters from 'vue2-filters'
import Vue from 'vue'
Vue.use(Vue2Filters)

import DataSourceGrid from './DataSourceGrid.vue';

export default {
  name: 'app',
  components: { DataSourceGrid, },
  mounted () {
    console.log("mounted");
    this.getSourcesStatus();
  },
  data () {
    return  {
      source : {},
      sources: [{"_id":"blbal"}],
      errors: [],
    }
  },
  methods: {
    getSourcesStatus: function() {
      axios.get('http://localhost:7042/source')
      .then(response => {
        console.log(response.data.result);
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
#app {
  font-family: 'Avenir', Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
  margin-top: 60px;
}

h1, h2 {
  font-weight: normal;
}

ul {
  list-style-type: none;
  padding: 0;
}

li {
  display: inline-block;
  margin: 0 10px;
}

a {
  color: #42b983;
}
</style>
