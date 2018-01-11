<template>

    <div class="ui two column centered grid">
        <div class="four column centered row">
        </div>
        <div class="fourteen wide column centered row">
        <div class="column">
            <div class="ui statistics">
                <div class="statistic">
                    <div class="value">
                        <i class="database icon"></i>22
                    </div>
                    <div class="label">
                        Datasources
                    </div>
                </div>
                <div class="statistic">
                    <div class="text value">
                        1<br>
                        Billion
                    </div>
                    <div class="label">
                        Documents
                    </div>
                </div>
                <div class="statistic">
                    <div class="value">
                        <i class="cubes icon"></i> 5
                    </div>
                    <div class="label">
                        Builds
                    </div>
                </div>
                <div class="statistic">
                    <div class="value">
                        <i class="warning red sign icon"></i>
                        3
                    </div>
                    <div class="label">
                        Errors
                    </div>
                </div>
            </div>
        </div>
        </div>

    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'


export default {
  name: 'stats',
  mounted () {
    console.log("Stats mounted");
    this.refreshStats();
    setInterval(this.refreshStats,60000);
  },
  created() {
    bus.$on('refresh_stats',this.refreshStats);
  },
  data () {
      return  {
          stats: {},
          errors: [],
      }
  },
  components: {},
  methods: {
    refreshStats: function() {
      axios.get(axios.defaults.baseURL + '/source')
      .then(response => {
        this.sources = response.data.result;
      })
      .catch(err => {
        console.log("Error getting sources information: " + err);
      })
    },
  }
}
</script>

<style>
</style>
