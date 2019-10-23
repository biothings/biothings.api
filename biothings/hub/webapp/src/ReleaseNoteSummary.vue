<template>
    <div class="meta">
        <span v-if="release_notes">
            <span>
                <i class="bullhorn icon"></i><b>{{ release_notes.length }}</b> release note(s) available
                    <button class="ui tinytiny icon button" @click="generate">Generate</button>
                    <table class="ui compact collapsing small green table" v-if="release_notes.length">
                        <thead>
                          <tr>
                            <th>Compared with</th>
                            <th>Notes</th>
                          </tr>
                        </thead>
                        <tbody>
                          <tr v-for="reln in release_notes">
                            <td v-if="type == 'incremental'">{{ release_id }} </td>
                            <!-- don't know why, reln.changes.old.name is undefined here for incremental,
                                 but not in display() call... take it somewhere else -->
                            <td v-else>{{ reln.changes.old.name }} </td>
                            <td>
                                <a @click="display(reln.changes.old.name)">View</a>
                            </td>
                          </tr>
                        </tbody>
                    </table>
            </span>
            <b v-if="type == 'incremental' && release.diff.stats.mapping_changed">Mapping has changed.</b>
            <!-- search release note associated to this diff, ie. generated with "old" collection -->
        </span>
        <span v-else>
            <i class="red bullhorn icon"></i><i><b>No</b> release note generated</i>
        </span>

        <span v-if="error">
            <br><br>
            <div v-if="error" class="ui red basic label">{{error}}</div>
        </span>

        <div :class="['ui basic genrelnote modal',release_id]">
            <h3 class="ui icon">
                <i class="bullhorn icon"></i>
                Generate release note
            </h3>
            <div class="content">
                <div :class="['ui form',release_id]">
                    <div class="ui centered grid">
                        <div v-if="type == 'incremental'" class="eight wide column">
                            <label>A release note will be created comparing two different builds:
                                <ul>
                                    <li><b>{{release.old.backend}}</b>, with</li>
                                    <li><b>{{release.new.backend}}</b></li>
                                </ul>
                                If these builds were involved in the creation of an incremental release (<i>"diff"</i>),
                                more information will be added to the release note, reflecting the changes in this release.
                            </label>
                        </div>
                        <div v-else class="eight wide column">
                            <label>Select which previous build should this release be compared to in order to identify
                                   changes and generate the release note. You can choose "None", in that case, the release
                                   note will only contain information about this release (no comparison):
                            </label>
                            <br>
                            <br>
                            <select class="ui fluid previousbuilds dropdown" name="previous_build">
                                <option value="none">None (no comparison)</option>
                                <option v-for="build_name in compats" :value="build_name">{{build_name}}</option>
                            </select>
                        </div>
                        <div class="eight wide column">
                            <label>You can optionally add a free text at the end of the release note:</label>
                            <textarea name="note"></textarea>
                        </div>

                        <div class="eight wide column">
                        </div>
                    </div>
                </div>
            </div>

            <div class="ui error message" v-if="list_builds_error">
                {{list_builds_error}}
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

        <!-- display release note -->
        <div :class="['ui basic disprelnote modal',type,release_id]">
            <h3 class="ui icon">
                <i class="bullhorn icon"></i>
                Release note
            </h3>
            <div class="content">
                <pre class="relnotecontent"></pre>
            </div>

            <div class="actions">
                <div class="ui basic green ok inverted button">
                    <i class="remove icon"></i>
                    Close
                </div>
            </div>
        </div>

    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import Vue from 'vue';
import Loader from './Loader.vue'

export default {
    name: 'release-note-summary',
    mixins: [ Loader, ],
    props: ['release','build','type'],
    mounted() {
        this.normalizeReleaseNotes();
    },
    created() {
    },
    beforeDestroy() {
        $('.ui.basic.genrelnote.modal').remove();
        $('.ui.basic.disprelnote.modal').remove();
    },
    components: {  },
    data () {
        return {
            error : null,
            list_builds_error : null,
            compats : {},
            release_note_content: null,
            release_notes : [],
        }
    },
    computed: {
        release_id: function() {
            if(this.type == 'full') {
                return this.release.index_name;
            } else {
                // id in that case is the build against which the diff's been computed
                // not really well named, but we can live with that, right ?
                return this.release.old.backend;
            }
        },
    },
    watch: {
        build: function(newv,oldv) {
            if(newv != oldv) {
                this.normalizeReleaseNotes();
            }
        }
    },
    methods: {
        displayError : function() {
        },
        display: function(old=null) {
            this.error = null;
            if(this.type == "incremental") {
                var oldb = this.release_id;
                var newb = this.release.new.backend;
            } else {
                var oldb = old;
                var newb = this.release_id;
            }
            var self = this;
            var qargs = `old=${oldb}&new=${newb}`;
            self.loading();
            axios.get(axios.defaults.baseURL + '/release_note?' + qargs)
            .then(response => {
                self.release_note_content = response.data.result;
                self.loaded();
                $(`.ui.basic.disprelnote.modal.${self.type}.${self.release_id} pre`).text(response.data.result);
                $(`.ui.basic.disprelnote.modal.${self.type}.${self.release_id}`)
                .modal("setting", {
                    detachable : false,
                    closable: false,
                })
                .modal("show");
            })
            .catch(err => {
                console.log("Error retrieving release note: ");
                self.loaderror(err);
                self.error = self.extractError(err);
                throw err;
            })
        },
        normalizeReleaseNotes: function() {
            // case 1: incremental
            // build document contains release notes, and there's one that's been generated
            // with our current release (old = collection against which we compute the diff)
            if(this.build.release_note) {
                // deep copy so we don't change original object
                var rels = JSON.parse(JSON.stringify(this.build.release_note));
                if(this.type == "incremental") {
                    if(rels.hasOwnProperty(this.release.old.backend)) {
                        var rel = rels[this.release.old.backend];
                        rel["changes"]["old"]["name"] = self.release_id; // old collection name
                        this.release_notes = [rel];
                    }
                } else {
                    var relnotes = [];
                    for(var versus in rels) {
                        var rel = rels[versus];
                        // add old collection name in order to display it later
                        rel["changes"]["old"]["name"] = versus;
                        relnotes.push(rel);
                    }
                    this.release_notes = relnotes;
                }
            }
        },
        generate: function() {
            this.error = null;
            var self = this;
            if(this.type == "full") {
                self.loading();
                axios.get(axios.defaults.baseURL + '/builds')
                .then(response => {
                    var builds = response.data.result;
                    this.compats = this.selectCompatibles(builds);
                    $(".ui.previousbuilds.dropdown").dropdown();
                    self.loaded();
                })
                .catch(err => {
                    console.log("Error getting previous builds: ");
                    self.loaderror(err);
                    self.list_builds_error = self.extractError(err);
                })
            }
            $(`.ui.basic.genrelnote.modal.${this.release_id}`)
            .modal("setting", {
                detachable : false,
                closable: false,
                onApprove: function () {
                    var note = $(`.ui.form.${self.release_id} textarea[name=note]`).val();
                    if(self.type == "full") {
                        var newb = self.release_id;
                        var oldb = $(`.ui.form.${self.release_id} select[name=previous_build]`).val();
                    } else {
                        var oldb = self.release.old.backend;
                        var newb = self.release.new.backend;
                    }
                    self.loading();
                    axios.put(axios.defaults.baseURL + `/release_note/create`,
                    {"old" : oldb, "new" : newb, "note" : note})
                    .then(response => {
                        self.loaded();
                        return response.data.result;
                    })
                    .catch(err => {
                        console.log("Error generating release note: ");
                        self.loaderror(err);
                        self.error = self.extractError(err);
                        return false;
                    })
                }
            })
            .modal("show");
        },
        selectCompatibles(builds) {
            var _compat = [];
            var self = this;
            $.each(builds, function(i) {
                var b = builds[i];
                // comes from same build config and builds other than current one
                if(b.build_config._id == self.build.build_config._id && 
                  b._id != self.build._id) {
                    _compat.push(b._id);
                }
            });
            return _compat;
        },
    }
}
</script>

<style scoped>
.tinytiny {
    padding: .5em 1em .5em;
    font-size: .6rem;
}
.relnotecontent {
    font-size: .8em;
    overflow: visible !important;
}
.envdetails {
    font-size: .8em;
    overflow: visible !important;
}
.darkbluey {
    background-color: #3c515d !important;
}
</style>
