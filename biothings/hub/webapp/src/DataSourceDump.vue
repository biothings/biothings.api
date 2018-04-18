<template>
    <div class="ui two grid">
        <div class="row">

            <div class="ten wide column">
                <table class="ui small very compact definition collapsing table">
                    <tbody>
                        <tr>
                            <td >Release</td>
                            <td>
                                {{source.download.release}}
                            </td>
                        </tr>
                        <tr>
                            <td >Status</td>
                            <td>
                                <i :class="source.download.status == 'failed' ? 'red' : 'green'">{{source.download.status}}</i>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <!--i class="folder icon"></i-->
                                Data folder
                            </td>
                            <td>
                                {{source.download.data_folder}}
                            </td>
                        </tr>
                        <tr v-if="source.download.error">
                            <td >Error</td>
                            <td>
                                <div class="red">{{source.download.error}}</div>
                            </td>
                        </tr>
                        <tr>
                            <td >Last download</td>
                            <td>{{source.download.started_at}} <i v-if="source.download.started_at">({{source.download.started_at | moment("from", "now")}})</i></td>
                        </tr>
                        <tr>
                            <td >Duration</td>
                            <td>{{source.download.time}}</td>
                        </tr>
                        <tr>
                            <td class="ui grey">Dumper</td>
                            <td>
                                {{source.download.dumper.name}}
                                <span v-if="source.download.dumper.manual">(manual)</span>
                            </td>
                        </tr>
                    </tbody>
                </table>
            </div>
            <div class="six wide column">
                <div :class="['ui dump form',source._id]">
                    <div class="fields">
                        <div class="ten wide field">
                            <div class="ui checkbox">
                                <input type="checkbox" tabindex="0" class="hidden" id="force">
                                <label>Bypass check for new release availability, and force dump</label>
                            </div>
                        </div>
                        <div class="required six wide field">
                            <button :class="['ui labeled small icon button', $parent.download_status == 'downloading' ? 'disabled' : '']" @click="do_dump();">
                                <i class="download cloud icon"></i>
                                Dump
                            </button>
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

export default {
    name: 'data-source-dump',
    props: ['source'],
    mounted () {
        $('.ui.checkbox')
        .checkbox();
    },
    components: { },
    methods: {
        do_dump() {
            var field = $(`.ui.dump.form.${this.source._id}`).form('get field', "force");
            var force = null;
            if(field)
                force = field.is(':checked')
            console.log(force);
            return this.$parent.dump(null,force);
        },
    },
}
</script>

<style>
</style>
