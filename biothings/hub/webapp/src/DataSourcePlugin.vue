<template>
  <div class="ui two grid">
    <div class="row">

      <div class="ten wide column">
        <table class="ui small very compact definition collapsing table">
          <tbody>
            <tr>
              <td class="ui grey">URL</td>
              <td v-if="source.data_plugin.plugin.type == 'github'">
                <i class="github icon"></i>
                <span><a :href="source.data_plugin.plugin.url">{{source.data_plugin.plugin.url}}</a></span>
              </td>
              <td v-else>No information available</td>
            </tr>
            <tr>
              <td >Release</td>
              <td>
                {{source.data_plugin.download.release}}
              </td>
            </tr>
            <tr>
              <td >Source folder</td>
              <td>
                <a v-if="source.data_plugin.download.data_folder" :href="source.data_plugin.download.data_folder | replace('/data/biothings_studio','')">{{  source.data_plugin.download.data_folder}}</a>
              </td>
            </tr>
            <tr v-if="source.data_plugin.download.error">
              <td >Error</td>
              <td>
                <div class="red">{{source.data_plugin.download.error}}</div>
              </td>
            </tr>
            <tr>
              <td >Last download</td>
              <td>{{source.data_plugin.download.started_at}} <i v-if="source.data_plugin.download.started_at">({{source.data_plugin.download.started_at | moment("from", "now")}})</i></td>
            </tr>
            <tr>
              <td >Duration</td>
              <td>{{source.data_plugin.download.time}}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <div class="six wide column" :class="actionable">
        <div :class="['ui plugin form',source._id]">
          <div class="fields">
            <div class="required ten wide field">
              <input type="text" id="release" placeholder="Specify a commit hash or branch (optional)" autofocus>
            </div>
            <div class="required six wide field">
              <button class="ui labeled small icon button" @click="onUpdatePlugin();">
                <i class="database icon"></i>
                Update
              </button>
            </div>
          </div>
          <div class="fields">
            <div class="required ten wide field">
            </div>
            <div class="required six wide field">
              <button class="ui labeled small icon button" @click="onExportCode();">
                <i class="upload icon"></i>
                Export code
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- create new build configuration -->
    <div class="ui basic exportplugin modal">
      <h3 class="ui icon">
        <i class="upload icon"></i>
        Export plugin code
      </h3>
      <p>
        Plugin code can be exported to be able to modify directly the source code. This allows to
        create a "manual datasource" (as opposed to automatically generated trought the plugin architecture)
        which can be customized in more depth, using BioThings SDK.
      </p>
      <div class="content">
        <div class="ui exportform form">
          <div class="ui centered grid">
            <div class="sixteen wide column">

              <label>Plugin parts to export</label>
              <div class="grouped fields">
                <div class="field">
                  <div class="ui checkbox">
                    <input type="checkbox" name="dumper" checked="checked">
                    <label class="white">Dumper</label>
                  </div>
                  <plugin-part-export v-if="export_results" v-bind:part="export_results.dumper" v-bind:ptype="'dumper'"></plugin-part-export>
                  </span>
                </div>
                <div class="field">
                  <div class="ui checkbox">
                    <input type="checkbox" name="uploader" checked="checked">
                    <label class="white">Uploader</label>
                  </div>
                  <plugin-part-export v-if="export_results" v-bind:part="export_results.uploader" v-bind:ptype="'uploader'"></plugin-part-export>
                </div>
                <div class="field">
                  <div class="ui checkbox">
                    <input type="checkbox" name="mapping" checked="checked">
                    <label class="white">Mapping (<i>registered</i> or <i>generated</i> from inspection)</label>
                  </div>
                  <plugin-part-export v-if="export_results" v-bind:part="export_results.mapping" v-bind:ptype="'mapping'"></plugin-part-export>
                </div>
              </div>
              <br/>
              <label class="white">Delete any previously exported code</label>
              <div class="grouped fields">
                <div class="field">
                  <div class="ui checkbox">
                    <input type="checkbox" name="purge">
                    <label class="white">Purge</label>
                  </div>
                </div>
              </div>
            </div>

            <div class="eight wide column">
              <div v-if="export_error" class="ui negative message">
                <p>{{export_error}}</p>
              </div>
              <div v-if="Object.keys(export_results).length" class="ui positive message">
                <p>
                  You may want to adjust ACTIVE_DATASOURCES parameter in
                  configuration to activate this new exported datasource.
                </p>
              </div>
            </div>

            <div class="eight wide column">
              <div class="actions">
                <div class="ui red basic cancel inverted button">
                  <i class="remove icon"></i>
                  <span id="exportcancel">Cancel</span>
                </div>
                <div class="ui green ok inverted button" id="exportok">
                  <i class="checkmark icon"></i>
                  <span id="exportok">OK</span>
                </div>
              </div>
            </div>

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
import Loader from './Loader.vue'
import Actionable from './Actionable.vue'
import PluginPartExport from './PluginPartExport.vue'

export default {
    name: 'data-source-plugin',
    mixins: [ Loader, Actionable, ],
    props: ['source'],
    mounted () {
    },
    beforeDestroy() {
        $('.ui.basic.exportplugin.modal').remove();
    },
    components: { PluginPartExport },
    data () {
      return {
        export_error: null,
        export_results: {},
      }
    },
    methods: {
        onUpdatePlugin: function() {
            var field = $(`.ui.plugin.form.${this.source._id}`).form('get field', "release");
            var release = null;
            if(field)
                release = field.val();
            return this.dumpPlugin(release=release);
        },
        showExportResults: function(res) {
          this.export_results = res.data.result;
          console.log(res);
        },
        onExportCode: function() {
          // restore original state
          $("#exportok").show();
          $("#exportcancel").text("Cancel");
          this.export_results = {};
          var self = this;
          $('.ui.basic.exportplugin.modal')
          .modal("setting", {
            detachable : false,
            closable: false,
            onApprove: function () {
              self.loading();
              self.export_error = null;
              var parts = [];
              $(".ui.exportform.form").form('get field', "dumper").prop("checked") && parts.push("dumper")
              $(".ui.exportform.form").form('get field', "uploader").prop("checked") && parts.push("uploader")
              $(".ui.exportform.form").form('get field', "mapping").prop("checked") && parts.push("mapping")
              var purge = $(".ui.exportform.form").form('get field', "purge").prop("checked")
              var data = {"what": parts, "purge": purge}
              axios.put(axios.defaults.baseURL + `/dataplugin/${self.source._id}/export`,data)
              .then(response => {
                self.loaded();
                self.showExportResults(response)
                $("#exportok").hide();
                $("#exportcancel").text("Back");
                $('.ui.basic.exportplugin.modal').modal("setting",{onDeny:function() {console.log("ok on valide");return true}});
              })
              .catch(err => {
                console.log("Error exporting plugin: " + err);
                self.loaderror(err);
                self.export_error = self.extractError(err);
              })
              return false;
            }
          })
          .modal("show");
        },
        dumpPlugin: function(release=null) {
            // note: plugin name has the same name as the source
            var data = null;
            if(release != null && release != "")
                data = {"release":release}
            console.log(data);
            axios.put(axios.defaults.baseURL + `/dataplugin/${this.source.name}/dump`,data)
            .then(response => {
                console.log(response.data.result)
            })
            .catch(err => {
                console.log("Error update plugin: " + err);
            })
        },
    },
}
</script>

<style scoped>
  .white {color:white !important;}
</style>
