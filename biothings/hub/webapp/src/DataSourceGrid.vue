<template>
    <div id="sources">
        <div class="ui left vertical labeled icon small inverted sidebar menu">
            <a class="item" v-on:click="register">
                <i class="plug icon"></i>
                New data plugin
            </a>
            <a class="item">
                <i class="unhide icon"></i>
                Inspect
            </a>
        </div>
        <div class="pusher">
            <div class="ui main container" id="list_build">
                <div class="ui segment left aligned">
                    <div class="ui secondary small menu">
                        <a class="item">
                            <i class="sidebar icon"></i>
                            Menu
                        </a>
                    </div>
                </div>
                <div id="data-source-grid" class="ui grid">
                    <div class="four wide column" v-for="source in orderBy(sources, 'name')">
                        <data-source v-bind:source="source"></data-source>
                    </div>
                </div>
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
                        <select class="ui dropdown">
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
    $('#sources .ui.sidebar')
    .sidebar({context:$('#sources')})
    .sidebar('setting', 'transition', 'overlay')
    .sidebar('attach events', '#sources .menu .item');
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
      axios.get(axios.defaults.baseURL + '/sources')
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
            axios.post(axios.defaults.baseURL + '/dataplugin/register_url',{"url":url})
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
