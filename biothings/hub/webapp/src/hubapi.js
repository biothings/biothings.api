import axios from 'axios'
import bus from './bus.js'
import auth from './auth.js';
const jwt  = require('jsonwebtoken');


function getCookieValue(a) {
  var b = document.cookie.match('(^|[^;]+)\\s*' + a + '\\s*=\\s*([^;]+)');
  return b ? b.pop() : '';
}


class ApiService {

  get(endpoint) {
    return axios.get(endpoint);
  }

  base(url) {
    // set base URL for all API calls, setup auth if needed and retrieve last logged user if any
    axios.defaults.baseURL = url;
    const token = this.getAccessToken();
    if(token) {
      axios.defaults.headers.common['X-Biothings-Access-Token'] = getCookieValue("biothings-access-token");
      this.monitorExpiry();
    }
    const user = this.getLoggedUser();
    if(user) {
      bus.$emit("logged_user",user);
    }
  }

  monitorExpiry() {
    const token = this.getAccessToken();
    if(!token) {
      console.log("Access token not found, stop monitoring expiry date");
      return;
    }
    var expiry = this.getExpiryDate(token);
    // we'll refresh that token when reaching 90% of the time
    var now = new Date();
    var refreshIn = Math.max(Math.floor((expiry - (now.getTime() / 1000)) * .9),0);
    console.log(`Refresh access token in ${refreshIn} seconds`);
    setTimeout(() => {
      auth.refreshAccessToken();
      this.monitorExpiry();
    }, refreshIn * 1000);
  }

  getExpiryDate(token) {
    var dtok = jwt.decode(token);
    return dtok.exp;
  }

  checkTokenExpiryDate(token) {
    console.log(jwt.decode(hubapi.getAccessToken()));
  }

  getAccessToken() {
    return getCookieValue("biothings-access-token");
  }

  getRefreshToken() {
    return getCookieValue("biothings-refresh-token");
  }

  getLoggedUser() {
    return getCookieValue("biothings-current-user");
  }

  clearLoggedUser() {
    ["biothings-current-user", "biothings-access-token", "biothings-refresh-token", "biothings-id-token"].map( name => {
      document.cookie = name + '=; expires=Thu, 01 Jan 1970 00:00:01 GMT;';
    });
    delete axios.defaults.headers.common['X-Biothings-Access-Token'];
    bus.$emit("logged_user",null);
  }

}


export default new ApiService();
