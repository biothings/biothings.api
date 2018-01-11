<template>
    <div class="ui main container" id="list_sources">
        <div class="ui segment mini right aligned">
            <div class="ui icon buttons">
                <button class="ui button" v-on:click="register">
                    <i class="plug icon"></i>
                </button>
            </div>
        </div>
        <div id="data-source-grid" class="ui grid">
            <div class="four wide column" v-for="source in orderBy(sources, 'name')">
                <data-source v-bind:source="source"></data-source>
            </div>
        </div>

        <div class="ui basic newdatasource modal">
            <div class="ui icon">
                <i class="plug icon"></i>
                Register a new datasource
            </div>
            <div class="content">
                <p>Specify a repository type and URL</p>
            </div>
            <div class="ui form">
                <div class="fields">
                    <div class="required four wide field">
                        <select class="ui search dropdown">
                            <option data-value="github" selected>Github</option>
                        </select>
                    </div>
                    <div class="required ten wide field">
                        <input type="text" id="repo_url" placeholder="Repository URL" autofocus>
                    </div>
                </div>
            </div>
            <div class="actions">
                <div class="ui red basic cancel inverted button">
                    <i class="remove icon"></i>
                    Cancel
                </div>
                <div class="ui green ok inverted button">
                    <i class="checkmark icon"></i>
                    OK
                </div>
            </div>

        </div>

        <div class="ui basic registering modal">
            <div class="ui segment">
                <div class="ui active dimmer">
                    <div class="ui indeterminate text loader">Registering...</div>
                </div>
                <p></p>
            </div>
        </div>


    </div>

</template>

<script>
import axios from 'axios'
import DataSource from './DataSource.vue'
import bus from './bus.js'


export default {
  name: 'data-source-grid',
  mounted () {
    console.log("DataSourceGrid mounted");
    this.getSourcesStatus();
    setInterval(this.getSourcesStatus,15000);
    $('select.dropdown').dropdown();
  },
  created() {
    bus.$on('refresh_sources',this.refreshSources);
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
      axios.get(axios.defaults.baseURL + '/source')
      .then(response => {
        this.sources = response.data.result;
      })
      .catch(err => {
        console.log("Error getting sources information: " + err);
      })
    },
    register: function() {
      $('.ui.basic.newdatasource.modal')
        .modal("setting", {
          onApprove: function () {
            var url = $(".ui.form").form('get field', "repo_url").val();
            axios.post(axios.defaults.baseURL + '/register_url',{"url":url})
            .then(response => {
              console.log(response.data.result)
              bus.$emit("refresh_sources");
              return true;
            })
            .catch(err => {
              console.log(err);
              console.log("Error registering repository URL: " + err.data.error);
            })
          }
        })
        .modal("show");
    },
    refreshSources: function() {
      console.log("Refreshing datasources");
      this.getSourcesStatus();
    }
  }
}
</script>

<style>
</style>
