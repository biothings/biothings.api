import Vue from 'vue'
import App from './App.vue'
import axios from 'axios'
import VueRouter from 'vue-router';

import moment from 'moment'
Vue.prototype.moment = moment

axios.defaults.baseURL = 'http://localhost:7080'


new Vue({
  el: '#app',
  render: h => h(App),
  router: App.router,
})
