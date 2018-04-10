<template>
    <div id="data-source" class="ui card">
        <div class="content">

            <!-- locked -->
            <i class="right floated lock icon blue"
                v-if="source.locked"></i>

            <!-- in progress -->
            <i class="right floated database icon pulsing"
                v-if="upload_status == 'uploading'"></i>
            <i class="right floated cloud download icon pulsing"
                v-if="download_status == 'downloading'"></i>
            <i class="right floated unhide icon pulsing"
                v-if="inspect_status == 'inspecting'"></i>

            <!-- error -->
            <div class="ui"
                v-bind:data-tooltip="getAllErrors()">
                <i class="right floated red alarm icon pulsing"
                  v-if="[download_status,upload_status,inspect_status].indexOf('failed') != -1">
            </i></div>

            <div class="left aligned header" v-if="source.name">
                <router-link :to="'/source/' + source._id"><a>{{ source.name }}</a></router-link>
            </div>
            <div class="meta">
                <span class="right floated time" v-if="source.download && source.download.started_at">Updated {{ source.download.started_at | moment("from", "now") }}</span>
                <span class="right floated time" v-else>Never updated</span>
                <span class="left floated category">{{ release }}</span>
            </div>
            <div class="left aligned description">
                <p>
                    <div class="ui clearing divider"></div>
                    <div>
                        <i class="file outline icon"></i>
                        {{ source.count | currency('',0) }} document{{ source.count &gt; 1 ? "s" : "" }}
                    </div>
                </p>
            </div>
        </div>
        <div class="extra content">
            <div class="ui icon buttons left floated mini">
                <button class="ui button" v-on:click="dump" v-if="source.download">
                    <i class="download cloud icon"></i>
                </button>
                <button class="ui button" v-on:click="upload" v-if="source.upload">
                    <i class="database icon"></i>
                </button>
            </div>
            <div class="ui icon buttons left floated mini">
                <button class="ui button" v-on:click="inspect">
                    <i class="unhide icon"></i>
                </button>
            </div>
            <div class="ui icon buttons right floated mini">
                <button class="ui button"
                    v-on:click="unregister" v-if="source.data_plugin">
                    <i class="trash icon"></i>
                </button>
            </div>
        </div>

        <inspect-form v-bind:toinspect="source" v-bind:select_data_provider="true">
        </inspect-form>

        <!-- Register new data plugin -->
        <div class="ui basic unregister modal" v-if="source.data_plugin">
            <input class="plugin_url" type="hidden" :value="source.data_plugin.plugin.url">
            <div class="ui icon header">
                <i class="remove icon"></i>
                Unregister data plugin
            </div>
            <div class="content">
                <p>Are you sure you want to unregister and delete data plugin <b>{{source.name}}</b> ?</p>
            </div>
            <div class="actions">
                <div class="ui red basic cancel inverted button">
                    <i class="remove icon"></i>
                    No
                </div>
                <div class="ui green ok inverted button">
                    <i class="checkmark icon"></i>
                    Yes
                </div>
            </div>
        </div>

    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import InspectForm from './InspectForm.vue'
import BaseDataSource from './BaseDataSource.vue'

export default {
    name: 'data-source',
    props: ['psource'],
    components: { InspectForm },
    mixins: [ BaseDataSource, ],
    mounted () {
        $('select.dropdown').dropdown();
    },
    data() {
        return {
            // this object is set by API call, whereas 'psource' prop
            // is set by the parent
            source_from_api: null,
        }
    },
    computed: {
        source: function () {
            // select source from API call preferably
            return this.source_from_api || this.psource;
        },
    },
    methods: {
        getSource: function() {
            console.log(`getSource ${this.source._id}`);
            var self = this;
            axios.get(axios.defaults.baseURL + '/source/' + this.source._id)
            .then(response => {
                this.source_from_api = response.data.result;
            })
            .catch(err => {
                console.log("Error getting sources information: " + err);
            })
        },
    },
}
</script>

<style>
  a {
        color: #0b0089;
    }

</style>

