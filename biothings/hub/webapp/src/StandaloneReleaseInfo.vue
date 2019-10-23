<template>
    <span>
            <span v-if="selected_version && selected_version.info">
                <h2 class="ui icon">
                    <i class="info circle icon"></i>
                    Information about this release
                </h2>
                <div class="ui horizontal list">
                    <div class="item">
                        <i class="thumbtack icon"></i>
                        <div class="top aligned content">
                            <div class="header">Version</div>
                            {{ selected_version.info.build_version }}
                        </div>
                    </div>
                    <div class="item">
                        <i class="calendar alternate outline icon"></i>
                        <div class="top aligned content">
                            <div class="header">Date</div>
                            {{ moment(selected_version.info.release_date).format('MMMM Do YYYY, h:mm:ss a')}}
                        </div>
                    </div>
                    <div class="item">
                        <i class="calendar alternate outline icon"></i>
                        <div class="top aligned content">
                            <div class="header">Type</div>
                                <i :class="['ui ', selected_version.info.type == 'full' ? 'blue bookmark' : 'orange exchange alternate','icon']"></i>
                                {{ selected_version.info.type }}
                            <div v-if="selected_version.info.type == 'incremental'">
                                {{ selected_version.info.require_version }} <i class="long arrow alternate right icon"></i> {{ selected_version.info.target_version }}
                            </div>
                        </div>
                    </div>
                    <div class="item" v-if="selected_version.info.app_version || selected_version.info.biothings_version || selected_version.info.standalone_version">
                        <i class="github icon"></i>
                        <div class="top aligned content">
                            <div class="header">Required app. versions</div>
                            <div class="ui list nopad">
                                <div class="tinytiny item" v-if="selected_version.info.app_version">
                                    <!-- adjust according to version format (used to be strings, then dict) -->
                                    App. version:
                                        <span v-if="selected_version.info.app_version.branch">
                                            {{ selected_version.info.app_version.branch }} [{{selected_version.info.app_version.commit}}]
                                        </span>
                                        <span v-else>
                                            {{ selected_version.info.app_version }}
                                        </span>
                                </div>
                                <div class="tinytiny item" v-if="selected_version.info.biothings_version">
                                    Biothings:
                                        <span v-if="selected_version.info.biothings_version.branch">
                                            {{ selected_version.info.biothings_version.branch }} [{{selected_version.info.biothings_version.commit}}]
                                        </span>
                                        <span v-else>
                                            {{ selected_version.info.biothings_version }}
                                        </span>
                                </div>
                                <div class="tinytiny item" v-if="selected_version.info.standalone_version">
                                    Standalone:
                                        <span v-if="selected_version.info.standalone_version.branch">
                                            {{ selected_version.info.standalone_version.branch }} [{{selected_version.info.standalone_version.commit}}]
                                        </span>
                                        <span v-else>
                                            {{ selected_version.info.standalone_version }}
                                        </span>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>

                <h5 class="ui icon">
                    <i class="bullhorn icon"></i>
                    Release note
                </h5>
                <div class="content" v-if="selected_version.release_note && selected_version.release_note.txt">
                    <div class="ui form">
                        <div class="ui grid">
                            <div class="sixteen wide column">
                                <div class="content">
                                    <pre class="relnotecontent">{{selected_version.release_note.txt}}</pre>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="content" v-else>No release note found</div>

                <br>
                <div class="actions">
                    <div class="ui basic green ok inverted button">
                        <i class="remove icon"></i>
                        Close
                    </div>
                </div>
            </span>
            <!-- TODO: display info about release metadata: #diff files, sizes, etc... -->
            <span v-else>
                <div class="ui error message" v-if="info_error">
                    {{info_error}}
                </div>
            </span>
    </span>
</template>

<script>
import Vue from 'vue'
import bus from './bus.js'


export default {
    name: 'standalone-release-info',
    props: ['selected_version'],
    mixins: [ ],
    mounted () {
    },
    updated() {
    },
    created() {
    },
    beforeDestroy() {
    },
    watch: {
    },
    data () {
        return  {
            info_error: null,
        }
    },
    computed: {
    },
    components: { },
    methods: {
    },
}
</script>

<style scoped>
.actions {
    text-align: right !important;
}
.ui.list>.item .header {
    color: white !important;
}
.tinytiny {
    font-size: .8rem !important;
}
.nopad {
    padding-top: 0 !important;
}
.relnotecontent {
    font-size: .8em;
    overflow: visible !important;
}
</style>
