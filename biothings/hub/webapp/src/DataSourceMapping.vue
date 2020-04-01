<template>
    <span>
        <div class="ui fluid basic segment right aligned" :class="actionable">
            <button class="ui button mini" v-on:click="$parent.inspect">
                <i class="unhide icon"></i>
                Inspect data
            </button>
        </div>
        <inspect-form v-bind:_id="_id">
        </inspect-form>
        <span v-if="maps">
            <!-- multiple sub-source -->
            <span v-if="Object.keys(maps).length > 1">
                <p>Found sub-sources linked to main source <b>{{_id}}</b>, select one to see mapping</p>
                <div id="maps" class="ui top attached tabular menu">
                    <a :class="['green item', i === 0 ? 'active' : '']" v-for="(_,subsrc,i) in maps" :data-tab="'inspect_' + subsrc">{{subsrc}}
                        <button class="reset ui button" v-if="is_broken(subsrc)" @click="reset(subsrc)" data-tooltip="Datasource broken, click to remove">
                            <i class="close icon"></i>
                        </button>
                    </a>
                </div>
            </span>
            <div :class="['ui bottom attached tab segment', i === 0 ? 'active' : '']" v-for="(data,subsrc,i) in maps" :data-tab="'inspect_' + subsrc" v-if="maps">
                <p>
                    These are the mappings for source <b>{{subsrc}}</b>.
                </p>
                <p>
                    <i>Mapping from inspection</i> has been generated during data inspection, while <i>Registered mapping</i> is the actual active mapping, used during indexation.
                </p>
                <p>
                    Mappings can be manually edited and mapping from inspection can be saved as the new registered, active mapping.
                </p>
                <div class="ui warning message">
                    <ul class="ui list">
                        <li>If a mapping is hard-coded in source code, it can't be edited, saved or replaced.</li>
                        <li>When testing a mapping, an temporary index is created on the selection ElasticSearch environment. That index is then deleted.</li>
                    </ul>
                </div>
                <div class="ui grid">
                    <div :class="actionable" class="center aligned sixteen wide column" v-if="maps[subsrc]['inspect_mapping'] && !maps[subsrc]['inspect_mapping']['errors'] && !maps[subsrc]['inspect_mapping']['pre-mapping']">
                        <button class="ui labeled mini icon button"
                            v-on:click="diffMapping('tab_mapping_inspected','tab_mapping_registered',subsrc)">
                            <i class="exchange icon"></i>
                            Diff
                        </button>
                    </div>
                    <div class="eight wide column">
                        <mapping-map v-if="maps[subsrc]"
                            v-bind:entity="'source'"
                            v-bind:map="maps[subsrc]['inspect_mapping']"
                            v-bind:name="subsrc"
                            v-bind:map_origin="'inspect'"
                            v-bind:map_id="'tab_mapping_inspected'"
                            v-bind:read_only="maps[subsrc]['inspect_mapping'] && maps[subsrc]['inspect_mapping']['pre-mapping']"
                            v-bind:can_commit="maps[subsrc]['registered_mapping'] ? maps[subsrc]['registered_mapping']['origin'] != 'uploader' : true">
                        </mapping-map>
                    </div>
                    <div class="eight wide column">
                        <mapping-map v-bind:map="maps[subsrc]['registered_mapping']['mapping']" 
                            v-bind:entity="'source'"
                            v-bind:name="subsrc"
                            v-bind:map_origin="'master'"
                            v-bind:map_id="'tab_mapping_registered'"
                            v-bind:read_only="maps[subsrc]['registered_mapping'] && maps[subsrc]['registered_mapping']['origin'] == 'uploader'"
                            v-bind:can_commit="maps[subsrc]['registered_mapping'] ? maps[subsrc]['registered_mapping']['origin'] != 'uploader' : true"
                            v-if="maps[subsrc]['registered_mapping']">
                        </mapping-map>
                    </div>
                </div>
            </div>
        </span>
        <div v-else>
            No mapping data found for this source.
        </div>
    </span>

</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import Loader from './Loader.vue'
import Actionable from './Actionable.vue'
import MappingMap from './MappingMap.vue'
import DiffUtils from './DiffUtils.vue'
import InspectForm from './InspectForm.vue'

export default {
    name: 'data-source-mapping',
    props: ['_id','maps','source'],
    mixins: [DiffUtils, Loader, Actionable, ],
    mounted () {
        this.setup();
        //$('#maps .item:first').addClass('active');
        //$('.tab:first').addClass('active');
    },
    components: { MappingMap, InspectForm, },
    data () {
        return {
        }
    },
    watch: {
        maps: function(newv,oldv) {
            if(newv != oldv) {
                // there's a race condition here: if multiple mappings updated in very little time,
                // not all tabs will be setup properly (some could be ignored depending on the time
                // spent to set it up and the events telling us they have changed)
                this.setup(); // refresh tabs
            }
        },
    },
    methods: {
        setup: function() {
            $('.menu .item').tab();
        },
        is_broken: function(subsrc) {
            try {
                if(!this.source.upload.sources.hasOwnProperty(subsrc) ||
                    this.source.upload.sources[subsrc]['uploader'] === null) {
                        return true;
                    }
            } catch(e) {
                return false;
            }
        },
        reset: function(subsrc) {
            var self = this;
            self.loading();
            var data = {
                "name" : self.source._id,
                "key" : "inspect",
                "subkey": subsrc
            };
            axios.post(axios.defaults.baseURL + `/source/${self.source._id}/reset`,data)
            .then(response => {
                self.loaded();
            })
            .catch(err => {
                self.loaderror(err);
            });
        },
    },
}
</script>

<style scoped>
.reset.button {
    font-size: 0.5em !important;
    margin-left: 1em !important;
}
.reset > i {
    margin: 0em !important;
}
</style>
