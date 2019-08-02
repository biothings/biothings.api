import axios from 'axios'
import bus from './bus.js'


function getCookieValue(a) {
    var b = document.cookie.match('(^|[^;]+)\\s*' + a + '\\s*=\\s*([^;]+)');
    return b ? b.pop() : '';
}


class ApiService {

  get(endpoint) {
    console.log(`api get ${endpoint}`);
    return axios.get(endpoint);
  }

  base(url) {
    // set base URL for all API calls, setup auth if needed and retrieve last logged user if any
    axios.defaults.baseURL = url;
    const token = this.getAccessToken();
    if(token) {
      axios.defaults.headers.common['X-Biothings-Access-Token'] = getCookieValue("biothings-access-token");
    }
    const user = this.getLoggedUser();
    if(user) {
      bus.$emit("logged_user",user);
    }
  }

  getAccessToken() {
    return getCookieValue("biothings-access-token");
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
