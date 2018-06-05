import Vue from 'vue'
import App from './App.vue'
import axios from 'axios'
import VueRouter from 'vue-router';

import moment from 'moment'
Vue.prototype.moment = moment


var last = Vue.localStorage.get('last_conn');
if(last) {
   var conn = JSON.parse(last);
   axios.defaults.baseURL = conn.url;
} else {
  // default to same host as the webapp's one, standard hub api port
  axios.defaults.baseURL = `http://${window.location.hostname}:7080`
}

console.log(`axios default URl: ${axios.defaults.baseURL}`);

new Vue({
  el: '#app',
  render: h => h(App),
  router: App.router,
})
