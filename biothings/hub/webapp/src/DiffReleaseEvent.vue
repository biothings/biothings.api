<template>
    <div class="event-container">
        <div class="content">
            <div class="summary">
                <i class="large exchange alternate icon"></i>
                <b>diff</b> with <i>{{release.old.backend || '?'}}</i> has been computed.<br>
                Old version: <b v-if="release.old.version">{{release.old.version}}</b><i v-else>None</i>, current version: <b>{{release.new.version}}</b>
                <div class="date">
                    Created 
                    {{release.created_at | moment("from", "now")}}
                    (<i>on {{moment(release.created_at).format('MMMM Do YYYY, h:mm:ss a') }}</i>)

                </div>
            </div>
            <div class="meta">
                <i class="file alternate icon"></i>{{release.diff.files.length}} diff file(s) created ({{ total_diff_size | pretty_size(precision=0) }})
                <i class="chart line icon"></i>{{release.diff.stats.update | formatInteger }} updated,
                {{release.diff.stats.add | formatInteger }} added,
                {{release.diff.stats.deleted | formatInteger }} deleted.
                <b v-if="release.diff.stats.mapping_changed">Mapping has changed.</b>
                <!-- search release note associated to this diff, ie. generated with "old" collection -->
                <release-note-summary :build="build" :release="release"  :type="type"></release-note-summary>
                <div class="ui pubactions basic compact segment">
                    <button class="ui mini grey labeled icon button" @click="applyDiff(release)">
                        <i class="external link square alternate
                            icon"></i>
                        Apply
                    </button>
                    <button class="ui mini grey labeled icon button" @click="publish(release)">
                        <i class="share alternate square icon"></i>
                        Publish
                    </button>
                </div>
            </div>
        </div>

        <!-- apply diff -->
        <div :class="['ui basic applydiff modal',release.old.backend]">
            <h3 class="ui icon">
                <i class="tag icon"></i>
                Apply increment update (diff)
            </h3>
            <div class="content">
                <div class="ui form">
                    <div class="ui centered grid">
                        <div class="eight wide column">

                            <label>Select a backend to apply the diff to</label>
                            <div>
                                <select class="ui fluid backendenv dropdown" name="target_backend">
                                        <option v-for="idx_info in compats"
                                                :data-es_host="idx_info.host"
                                                :data-index="idx_info.index">{{idx_info.env}} ({{idx_info.host}} | {{idx_info.index}})</option>
                                </select>
                                <br>
                                <br>
                            </div>
                        </div>

                        <div class="eight wide column">
                        </div>
                    </div>
                </div>
            </div>

            <div class="ui error message" v-if="errors.length">
                <ul class="ui list">
                    <li v-for="err in errors">{{err}}</li>
                </ul>
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

export default {
    name: 'diff-release-event',
    props: ['release','build','type'],
    mounted() {
        $(".ui.backendenv.dropdown").dropdown();
    },
    beforeDestroy() {
        $('.ui.basic.applydiff.modal').remove();
    },
    created() {
    },
    components: { ReleaseNoteSummary, },
    data () {
        return {
            errors : [],
            compats : {},
        }
    },
    computed: {
        total_diff_size: function() {
            var size = 0;
            if(this.release.diff && this.release.diff.files) {
                this.release.diff.files.map(function(e) { size += e.size})
            }
            return size;
        },
    },
    methods: {
        displayError : function() {
        },
        applyDiff(release) {
            axios.get(axios.defaults.baseURL + '/index_manager?remote=1')
            .then(response => {
                // expecting a syncer exists with (diff_type,"es")
                var envs = response.data.result;
                this.compats = this.selectCompatibles(envs);
                $(".ui.backendenv.dropdown").dropdown();
            })
            .catch(err => {
                console.log("Error getting index environments: ");
                console.log(err);
                throw err;
            })
            var oldcol = release.old.backend;
            var newcol = release.new.backend;
            var diff_type = release.diff.type;
            var backend_type = "es"; // TODO: should we support more ?
            var doc_type = this.build.build_config.doc_type;
            var self = this;
            $(`.ui.basic.applydiff.modal.${this.release.old.backend}`)
            .modal("setting", {
                detachable : false,
                onApprove: function () {
                        //var es_host = :
                        var backend = $(".ui.form select[name=target_backend] :selected");
                        var host = $(backend).attr("data-es_host");
                        var index = $(backend).attr("data-index");
                        var target_backend = [host,index,doc_type];
                        axios.post(axios.defaults.baseURL + `/sync`,
                                {"backend_type" : backend_type,
                                 "old_db_col_names" : oldcol,
                                 "new_db_col_names" : newcol,
                                 "target_backend" : target_backend})
                        .then(response => {
                            console.log(response.data.result)
                            bus.$emit("reload_build_detailed");
                            return response.data.result;
                        })
                        .catch(err => {
                            console.log("Error applying diff: ");
                            console.log(err);
                        })
                }
            })
            .modal("show");

        },
        publish(release) {
            console.log("TODO publish");
            console.log(release);
        },
        selectCompatibles(envs) {
            var _compat = [];
            var selecting = null;
            var self = this;
            if(envs.build_config_key) {
                selecting = this.build.build_config[envs.build_config_key];
            }
            $.each(envs.env, function( env, value ) {
                // check whether we can use one of build_config keys
                // to filter compatibles indices
                if(selecting) {
                    if(!value.index.hasOwnProperty(selecting))
                        return true;// continue next iter
                }
                for(var k in value.index) {
                    // make sure doc_type is the same
                    if(value.index[k]["doc_type"] != self.build.build_config.doc_type) {
                        continue;
                    }
                    if(selecting && (selecting != k))
                        continue;
                    _compat.push({"env":env, "host":value["host"],"index":value.index[k]["index"]});
                    //_compat[env]["index"].push(value.index[k]["index"]);
                }
            });
            return _compat;
        }
    }
}
</script>

<style scoped>
.event-container {
    margin-bottom: 1em;
    width: inherit;
}
.pubactions {
    padding: 0 0 1em 0 !important;
}
</style>
