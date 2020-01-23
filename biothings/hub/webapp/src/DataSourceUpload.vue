<template>
    <span>
        <span v-if="source.upload && source.upload.sources">
            <span v-if="Object.keys(source.upload.sources).length > 1">
                <div id="srcs" class="ui top attached tabular menu">
                    <span v-for="(_,subsrc,i) in source.upload.sources">
                        <a :class="['green item', i === 0 ? 'active' : '']" :data-tab="subsrc">
                            {{subsrc}}
                            <button class="reset ui button" v-if="source.upload.sources[subsrc]['uploader'] === null" @click="reset(subsrc)">
                                <i class="close icon"></i>
                            </button>
                        </a>
                    </span>
                </div>
            </span>
            <div :class="['ui bottom attached tab segment', i === 0 ? 'active' : '']" :data-tab="subsrc" v-for="(info,subsrc,i) in source.upload.sources">
                <div class="ui two grid">
                    <div class="row">
                        <div class="ten wide column">
                            <table class="ui small very compact definition collapsing table">
                                <tbody>
                                    <tr>
                                        <td >Release</td>
                                        <td>
                                            {{info.release}}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td>
                                            <!--i class="folder icon"></i-->
                                            Data folder
                                        </td>
                                        <td>
                                            <a v-if="info.data_folder" :href="info.data_folder | replace('/data/biothings_studio','')">{{ info.data_folder }}</a>
                                        </td>
                                    </tr>
                                    <tr>
                                        <td >Status</td>
                                        <td><i :class="info.status == 'failed' ? 'red' : 'green'">{{info.status}}</i></td>
                                    </tr>
                                    <tr v-if="info.error">
                                        <td >Error</td>
                                        <td>
                                            <div class="red">{{info.error}}</div>
                                        </td>
                                    </tr>
                                    <tr>
                                        <td >Last upload</td>
                                        <td>{{info.started_at}} <i v-if="info.started_at">({{info.started_at | moment("from", "now")}})</i></td>
                                    </tr>
                                    <tr>
                                        <td >Duration</td>
                                        <td>{{info.time}}</td>
                                    </tr>
                                    <tr>
                                        <td >Documents uploaded</td>
                                        <td>{{info.count | currency('',0)}}</td>
                                    </tr>
                                    <tr>
                                        <td class="ui grey">Uploader</td>
                                        <td v-if="info.uploader">
                                            {{info.uploader.name}}
                                            <span v-if="info.uploader.dummy">(dummy)</span>
                                        </td>
                                        <td v-else>
                                            <div class="red">No uploader found, datasource may be broken</div>
                                        </td>

                                    </tr>
                                </tbody>
                            </table>
                        </div>
                        <div class="six wide column" v-if="info.uploader">
                            <p v-if="info.uploader.dummy">This is a <i>dummy</i> uploader, meaning data isn't actually uploaded but rather already stored in a collection.
                                In order to register data, a <b>release</b> is required in order the uploader to run properly.</p>
                            <div :class="['ui upload form',subsrc]">
                                <div class="fields">
                                    <div class="required ten wide field">
                                        <input type="text" id="release" placeholder="Specify a release (optional)" autofocus v-if="info.uploader.dummy">
                                    </div>
                                    <div class="required six wide field">
                                        <button :class="['ui labeled small icon button',info.status == 'uploading' ? 'disabled' : '']" @click="do_upload(subsrc)">
                                            <i class="database icon"></i>
                                            Upload
                                        </button>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </span>
        <div v-else>
            No uploader found for this source.
        </div>
    </span>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import Loader from './Loader.vue'

export default {
    name: 'data-source-upload',
    props: ['source'],
    mixins: [ Loader, ],
    mounted () {
        this.setup();
    },
    components: { },
    watch: {
        maps: function(newv,oldv) {
            if(newv != oldv) {
                // there's a race condition here: if multiple mappings updated in very little time,
                // not all tabs will be setup properly (some could be ignored depending on the time
                // spent to set it up and the events telling us they have changed)
                // Note: same as in DataSourceMapping.vue
                this.setup(); // refresh tabs
            }
        },
    },
    methods: {
        setup: function() {
            $('.menu .item').tab();
        },
        do_upload: function(subsrc=null) {
            return this.$parent.upload(subsrc=subsrc);
        },
        reset: function(subsrc) {
            var self = this;
            self.loading();
            var data = {
                "name" : self.source._id,
                "key" : "upload",
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
