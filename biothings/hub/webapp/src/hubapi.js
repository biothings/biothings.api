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
    axios.defaults.baseURL = url;
    const token = this.getAccessToken();
    if(token) {
      axios.defaults.headers.common['X-Biothings-Access-Token'] = getCookieValue("biothings-access-token");
    }
  }

  getAccessToken() {
    return getCookieValue("biothings-access-token");
  }

}

export default new ApiService();
