<template>
    <div class="event-container">
        <div class="ui grid">
            <div class="eight wide column">
                <div class="summary">
                    <i class="ui bookmark icon"></i>
                    Index
                    <b class="user">
                        {{release.index_name}}
                    </b> was created on <b>{{release.environment}}</b> environment (<b>{{release.host}}</b>)
                    <div class="date">
                        {{release.created_at | moment("from", "now")}}
                        (<i>on {{moment(release.created_at).format('MMMM Do YYYY, h:mm:ss a') }}</i>)

                    </div>
                </div>
                <div class="meta">
                  <div>
                    <i class="file alternate icon"></i> {{ num_indexed | formatNumber }} documents indexed
                      <button :class="[release.snapshot ? 'disabled' : '','ui tinytiny grey labeled icon button']" @click="snapshot(release)">
                          <i class="bookmark icon"></i>Snapshot
                      </button>
                  </div>
                </div>
            </div>
            <div class="eight wide column">
                <release-note-summary :build="build" :release="release" :type="type"></release-note-summary>
            </div>
        </div>

        <!-- create snapshot -->
        <div :class="['ui basic createsnapshot modal',release.index_name]">
            <h3 class="ui icon">
                <i class="bookmark icon"></i>
                Create snapshot
            </h3>
            <div class="content">
                <div class="ui form">
                    <div class="ui centered grid">
                        <div class="eight wide column">

                            <label>Select an environment, snapshot will be created according to its configuration:</label>
                            <div>
                                <select class="ui fluid snapshotenv dropdown" name="snapshot_env" v-model="selected_snapshot_env">
                                    <option v-for="_,env in snapshot_envs">{{ env }}</option>
                                </select>
                                <br>
                                <br>
                            </div>

                            <label>Enter a name for the snapshot (or leave it empty to have the same name as the index)</label>
                            <input type="text" name="snapshot_name" placeholder="Snapshot name" autofocus v-model="snapshot_name">
                        </div>

                        <div class="eight wide column">
                            <span v-if="selected_snapshot_env">
                                <label>Configuration details:</label>
                                <pre class="envdetails">{{ snapshot_envs[selected_snapshot_env] }}</pre>
                            </span>
                        </div>
                    </div>
                </div>
            </div>

            <div class="ui error message" v-if="error">
                {{error}}
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

    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import Vue from 'vue';
import ReleaseNoteSummary from './ReleaseNoteSummary.vue';
import Loader from './Loader.vue'

export default {
    name: 'index-release-event',
    mixins: [ Loader, ],
    props: ['release','build','type'],
    mounted() {
    },
    beforeDestroy() {
        $(`.ui.basic.createsnapshot.modal.${this.release.index_name}`).remove();
    },
    components: { ReleaseNoteSummary, },
    data () {
        return {
            snapshot_envs : {},
            error : null,
            selected_snapshot_env : null,
            snapshot_name : null,
        }
    },
    computed: {
        num_indexed: function() {
            return this.release.count || 0;
        }
    },
    methods: {
        displayError : function() {
        },
        snapshot(release) {
            console.log(release);
            var self = this;
            self.error = null;
            self.loading();
            axios.get(axios.defaults.baseURL + '/snapshot_manager')
            .then(response => {
                self.snapshot_envs = response.data.result.env;
                $(".ui.snapshotenv.dropdown").dropdown();
                self.loaded();
            })
            .catch(err => {
                console.log("Error getting snapshot environments: ");
                console.log(err);
                self.loaderror(err);
                self.error = err;
            })
            $(`.ui.basic.createsnapshot.modal.${this.release.index_name}`)
            .modal("setting", {
                detachable : false,
                closable: false,
                onApprove: function () {
                    if(!self.selected_snapshot_env)
                        return;
                    console.log(self.selected_snapshot_env);
                    console.log(self.snapshot_name);
                    axios.put(axios.defaults.baseURL + `/snapshot`,
                        {"snapshot_env" : self.selected_snapshot_env,
                         "index" : self.release.index_name,
                         "snapshot" : self.snapshot_name})
                    .then(response => {
                        console.log(response.data.result)
                        bus.$emit("reload_build_detailed");
                        return response.data.result;
                    })
                    .catch(err => {
                        console.log("Error creating snapshot: ");
                        console.log(err);
                    })
                }
            })
            .modal("show");
        },
    }
}
</script>

<style scoped>
.tinytiny {
    padding: .5em 1em .5em;
    font-size: .6em;
}
.event-container {
    margin-bottom: 1em;
    width: inherit;

}
.pubactions {
    padding: 0 !important;
}

.envdetails {
    font-size: .8em;
    overflow: visible !important;
}
</style>
