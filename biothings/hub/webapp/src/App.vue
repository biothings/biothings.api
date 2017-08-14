<template>
  <div id="app">
    <div class="ui fixed inverted menu">
      <div class="ui container">
        <a href="#" class="header item">
          <img class="logo" src="./assets/biothings_logo.png">
          Biothings Hub
        </a>
        <a href="#" class="item">Home</a>

        <!--div class="ui simple dropdown item right">
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

        <div class="ui item right">
          <job-summary></job-summary>
        </div>

      </div>
    </div>

    <data-source-grid v-bind:sources="sources"></data-source-grid>

  </div>
</template>

<script>
import axios from 'axios';

import Vue2Filters from 'vue2-filters';
import Vue from 'vue';
Vue.use(Vue2Filters);


var UNITS = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
var STEP = 1024;


function pretty_size(bytes,precision=2) {
	var units = [
		'bytes',
		'KB',
		'MB',
		'GB',
		'TB',
		'PB'
	];

	if ( isNaN( parseFloat( bytes )) || ! isFinite( bytes ) ) {
		return '?';
	}

	var unit = 0;

	while ( bytes >= 1024 ) {
		bytes /= 1024;
		unit ++;
	}

	return bytes.toFixed( + precision ) + ' ' + units[ unit ];
};

Vue.filter('pretty_size',pretty_size);

import DataSourceGrid from './DataSourceGrid.vue';
import JobSummary from './JobSummary.vue';

export default {
  name: 'app',
  components: { DataSourceGrid, JobSummary},
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
