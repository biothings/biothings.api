<template>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'

export default {
    name: 'base-data-source',
    // Note: we don't declare "source", it must be defined in subclass/mixed
    // (sometimes it's a prop, sometimes it's a data field
    mounted () {
    },
    components: { },
    created() {
        bus.$on('change_source',this.onSourceChanged);
    },
    beforeDestroy() {
        bus.$off('change_source',this.onSourceChanged);
        $('.ui.basic.unregister.modal').remove();
    },
    computed: {
        inspect_status: function() {
            return this.getStatus("inspect");
        },
        upload_status: function() {
            return this.getStatus("upload");
        },
        download_status: function() {
            if(this.source.download && this.source.download.status)
                return this.source.download.status;
            else
                return "unknown status";
        },
        inspect_error: function() {
          return this.getError("inspect");
        },
        upload_error: function() {
          return this.getError("upload");
        },
        download_error: function() {
            if(this.source.download && this.source.download.error)
                return this.source.download.error;
        },
        release: function() {
            if(this.source.download) {
                return this.source.download.release;
            } else if(this.source.upload) {
                var versions = [];
                $(this.source.upload.sources).each(function() {
                    versions.push(this.release);
                });
                if(versions.length > 1)
                    return "Multiple versions";
                else if(versions.length == 1)
                    return versions[0];
                else
                    return "?";
            } else {
                    return "Unknown";
            }
        }
    },
    methods: {
        getStatus: function(subkey) {
            var status = "unknown";
            if(this.source.hasOwnProperty(subkey)) {
                for(var subsrc in this.source[subkey].sources) {
                    if(["failed","inspecting","uploading"].indexOf(this.source[subkey].sources[subsrc]["status"]) != -1) {
                        status = this.source[subkey].sources[subsrc]["status"];
                        // precedence to these statuses
                        break;
                    }
                    else
                        status = this.source[subkey].sources[subsrc]["status"];
                }
            }
            return status;
        },
        getError: function(subkey) {
          var errors = [];
          if(this.source.hasOwnProperty(subkey)) {
            for(var subsrc in this.source[subkey].sources) {
              if(this.source[subkey].sources[subsrc]["error"])
                errors.push(this.source[subkey].sources[subsrc]["error"]);
            }
          }
          return errors;
        },
        getAllErrors: function() {
            var errs = [];
            if(this.download_error)
                errs.push(this.download_error);
            Array.prototype.push.apply(errs,this.getError("upload"));
            Array.prototype.push.apply(errs,this.getError("inspect"));
            return errs.join("<br>");
        },
        dump: function() {
            axios.put(axios.defaults.baseURL + `/source/${this.source.name}/dump`)
            .then(response => {
                console.log(response.data.result)
            })
            .catch(err => {
                console.log("Error getting job manager information: " + err);
            })
        },
        upload: function() {
            axios.put(axios.defaults.baseURL + `/source/${this.source.name}/upload`)
            .then(response => {
                console.log(response.data.result)
            })
            .catch(err => {
                console.log("Error getting job manager information: " + err);
            })
        },
        unregister: function() {
            $('.ui.basic.unregister.modal')
            .modal("setting", {
                onApprove: function () {
                    var url = $(this).find("input.plugin_url").val();
                    console.log(url);
                    axios.delete(axios.defaults.baseURL + '/dataplugin/unregister_url',{"data" : {"url":url}})
                    .then(response => {
                        console.log(response.data.result)
                        return true;
                    })
                    .catch(err => {
                        console.log(err);
                        console.log("Error unregistering repository URL: " + err.data.error);
                    })
                }
            })
            .modal("show");
        },
        inspect: function() {
            var self = this;
            $(`#inspect-${this.source._id}`)
            .modal("setting", {
                onApprove: function () {
                    var modes = $(`#inspect-${self.source._id}`).find("#select-mode").val();
                    var dp = $(`#inspect-${self.source._id}`).find("#select-data_provider").val();
                    axios.put(axios.defaults.baseURL + '/inspect',
                              {"data_provider" : [dp,self.source._id],"mode":modes})
                    .then(response => {
                        console.log(response.data.result)
                    })
                    .catch(err => {
                        console.log("Error getting job manager information: " + err);
                    })
                }
            })
            .modal("show");
        },
        onSourceChanged(_id=null,op=null) {
            // this method acts as a dispatcher, reacting to change_source events, filtering
            // them for the proper source
            // _id null: event containing change about a source but we don't know which one
            if(_id == null || this.source._id != _id) {
                //console.log(`I'm ${this.source._id} but they want ${_id}`);
                return;
            } else {
                console.log("_id was " + _id);
                return this.getSource();
            };
        },
    },
}
</script>

<style>
  a {
        color: #0b0089;
    }

</style>

