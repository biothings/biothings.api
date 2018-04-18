<template>
    <span>
        <span v-if="source.upload && source.upload.sources">
            <span v-if="Object.keys(source.upload.sources).length > 1">
                <div id="srcs" class="ui top attached tabular menu">
                    <a :class="['green item', i === 0 ? 'active' : '']" :data-tab="subsrc" v-for="(_,subsrc,i) in source.upload.sources">{{subsrc}}</a>
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
                                            {{info.data_folder}}
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
                                        <td>
                                            {{info.uploader.name}}
                                            <span v-if="info.uploader.dummy">(dummy)</span>
                                        </td>
                                    </tr>
                                </tbody>
                            </table>
                        </div>
                        <div class="six wide column">
                            <p v-if="info.uploader.dummy">This is a <i>dummy</i> uploader, meaning data isn't actually uploaded but rather already stored in a collection.
                                In order to register data, a <b>release</b> is required in order the uploader to run properly.</p>
                            <div :class="['ui upload form',subsrc]">
                                <div class="fields">
                                    <div class="required ten wide field">
                                        <input type="text" id="release" placeholder="Specify a release (optional)" autofocus v-if="info.uploader.dummy">
                                    </div>
                                    <div class="required six wide field">
                                        <button :class="['ui labeled small icon button',info.status == 'uploading' ? 'disabled' : '']" @click="do_upload(subsrc=subsrc);">
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

export default {
    name: 'data-source-upload',
    props: ['source'],
    mounted () {
        $('.menu .item').tab();
    },
    components: { },
    methods: {
        do_upload(subsrc=null) {
            var field = $(`.ui.upload.form.${subsrc}`).form('get field', "release");
            console.log(field);
            var release = null;
            if(field)
                release = field.val();
            console.log(release)
            return this.$parent.upload(release=release,subsrc=subsrc);
        },
    },
}
</script>

<style>
</style>
