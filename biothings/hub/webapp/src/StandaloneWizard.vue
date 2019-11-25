<template>
    <span>
        <div class="ui main container">
            <div class="ui segment">
                <h2 class="ui header">
                    <i class="magic icon"></i>
                    <div class="content">
                        Standalone settings
                        <div class="sub header">Manage remote data releases</div>
                    </div>
                </h2>
            </div>
            <div class="ui info message" v-if="!version_urls">
                <div class="header">
                    Welcome to BioThings Standalone setup wizard!
                </div>
                <p>This hub isn't configured yet to access remote date releases, please follow the steps below.</p>
            </div>
            <div class="ui centered grid">
                <div class="ui sixteen wide column">
                    <div class="ui four top attached steps">
                        <a id="credentials" class="step">
                            <i class="lock icon"></i>
                            <div class="content">
                                <div class="title">Credentials</div>
                                <div class="description">Setup authentication details</div>
                            </div>
                        </a>
                        <a id="datareleases" class="step">
                            <i class="globe icon"></i>
                            <div class="content">
                                <div class="title">Data Releases</div>
                                <div class="description">Register remote data releases</div>
                            </div>
                        </a>
                        <a id="misc" class="step">
                            <i class="info icon"></i>
                            <div class="content">
                                <div class="title">Misc.</div>
                                <div class="description">Optional customizations</div>
                            </div>
                        </a>
                        <a id="restarthub" class="step">
                            <i class="sync alternate icon"></i>
                            <div class="content">
                                <div class="title">Restart Hub</div>
                                <div class="description">Make changes active</div>
                            </div>
                        </a>
                    </div>
                    <div class="ui attached credentials segment">
                        <p>
                            If remote data releases require authentication, please provide IAM Access Key ID and Secret Access Key:
                        </p>
                        <div class="ui input">
                            <input type="text" size="50" placeholder="Access Key ID" v-model="access_key">
                        </div>
                        <div class="ui left pointing blue basic label">
                            Enter an IAM Access Key ID
                        </div>
                        <br>
                        <div class="ui input">
                            <input type="text" size="50" placeholder="Secret Access Key" v-model="secret_key">
                        </div>
                        <div class="ui left pointing blue basic label">
                            Enter an IAM Secret Access Key
                        </div>
                        <br>
                        <div class="ui warning message">
                            To limit exposure of sensitive data, these credentials are only stored within this hub,
                            they're <b>not</b> stored in ElasticSearch.<br>
                            For extra security, it is recommanded to use credentials giving read-only permissions,
                            as well as regurlarly changing these.
                        </div>
                        <button class="ui green ok labeled icon button" @click="saveKeys()">
                            <i class="save icon"></i>
                            Save
                        </button>
                        <br>
                    </div>
                    <div class="ui hide attached datareleases segment">
                        <p>
                            Enter URL pointing to a file named <code>versions.json</code>. This file contains the list of
                            all available remote data releases. It is automatically created when a data release is published from
                            a Biothings Studio. You can enter multiple URLs, one per line<br>
                            <br>
                            Ex: drag & drop one the following <i>versions.json</i>.
                            <div class="ui bulleted list">
                                <div class="item">
                                    <b>MyGene (demo data)</b> (no auth.): <a href="http://biothings-releases.s3-website-us-west-2.amazonaws.com/mygene.info-demo/versions.json">versions.json</a>
                                </div>
                                <div class="item">
                                    <b>MyGene (production data)</b> (auth. required): <a href="https://biothings-releases.s3-us-west-2.amazonaws.com/mygene.info/versions.json">versions.json</a>
                                </div>
                            </div>
                        </p>
                        <br>
                        <div class="ui form">
                            <div class="field">
                                <textarea v-model="version_urls"></textarea>
                            </div>
                        </div>
                        <br>
                        <button class="ui green ok labeled icon button" @click="saveVersionURLs()">
                            <i class="save icon"></i>
                            Save
                        </button>
                    </div>
                    <div class="ui hide attached misc segment">
                        <p>
                            You can optionally rename this Hub and assign a custom icon.
                        </p>
                        <div class="ui input">
                            <input type="text" size="50" placeholder="Name" v-model="hub_name">
                        </div>
                        <div class="ui left pointing blue basic label">
                            Give a name to this standalone hub
                        </div>
                        <br>
                        <div class="ui input">
                            <input type="text" size="50" placeholder="Icon (url)" v-model="hub_icon">
                        </div>
                        <div class="ui left pointing blue basic label">
                            Enter URL pointing to a custom icon
                        </div>
                        <br>
                        <br>
                        <button class="ui green ok labeled icon button" @click="saveMisc()">
                            <i class="save icon"></i>
                            Save
                        </button>
                        <br>
                    </div>
                    <div class="ui hide attached restarthub segment">
                        <p>
                            Once configured, Hub needs to be restarted so the new configuration becomes active.
                            <div class="ui yellow compact message right floated" v-if="dirty_conf">Configuration has changed, Hub needs to restart</div>
                        </p>
                        <br>
                        <button class="ui red ok labeled icon button" @click="restartHub()">
                            <i class="sync alternate icon"></i>
                            Restart
                        </button>
                    </div>
                </div>
            </div>
        </div>
    </div>
</span>
</template>

<script>
import Vue from 'vue'
import axios from 'axios'
import Loader from './Loader.vue'
import bus from './bus.js'


export default {
	name: 'standalone-wizard',
	mixins: [ Loader, ],
	mounted () {
        console.log("StandaloneWizard mounted");
        this.setup();
        if(Object.keys(this.config).length) {
            this.fetchConfigKeys();
        }
	},
	updated() {
	},
	created() {
        bus.$on("current_config",this.onConfig);
	},
	beforeDestroy() {
        bus.$off("current_config",this.onConfig);
	},
    watch: {
        config: function(newv,oldv) {
            if(newv != oldv) {
                this.fetchConfigKeys();
            }
        }
    },
	data () {
		return  {
            config: {},
            access_key: null,
            secret_key: null,
            hub_name: null,
            hub_icon: null,
            version_urls: [],
            dirty_conf: false,
		}
	},
    computed: {
    },
	components: { },
	methods: {
        setup: function() {
            // make sure we have access to current config
            if(!Vue.config.hub_config) {
                // force a reload of config to get it there
                bus.$emit("change_config");
            } else {
                this.config = Vue.config.hub_config;
            }

            // wizard steps
            var setStep = function(id){
                // update steps
                $('#'+id).addClass('active').nextAll().removeClass('active');
                $('#'+id).prevAll().removeClass('active');
                // update segment
                $('.segment.'+id).show();
                $('.segment.'+id).nextAll().not(".steps").hide();
                $('.segment.'+id).prevAll().not(".steps").hide();
            };
            $('.step').click(function(){
                setStep($(this).attr('id'));
            })
            // init
            setStep("credentials");
        },
        onConfig: function(conf) {
            this.config = conf.scope.config;
            this.dirty_conf = conf._dirty;
        },
        fetchConfigKeys: function() {
            this.access_key = this.config.STANDALONE_AWS_CREDENTIALS && this.config.STANDALONE_AWS_CREDENTIALS["value"].AWS_ACCESS_KEY_ID;
            this.secret_key = this.config.STANDALONE_AWS_CREDENTIALS && this.config.STANDALONE_AWS_CREDENTIALS["value"].AWS_SECRET_ACCESS_KEY;
            this.version_urls = this.config.VERSION_URLS && this.config.VERSION_URLS["value"].join("\n");
            this.hub_name = this.config.HUB_NAME["value"];
            this.hub_icon = this.config.HUB_ICON["value"];
        },
        restartHub: function() {
            bus.$emit("restart_hub");
        },
        saveVersionURLs: function() {
            var arr = this.version_urls.split('\n').filter(function (el) {
                return el != "";
            });
            bus.$emit("save_config_param",{"name":"VERSION_URLS","value": JSON.stringify(arr)});
        },
        saveKeys: function() {
            var dat = {"AWS_ACCESS_KEY_ID" : this.access_key, "AWS_SECRET_ACCESS_KEY" : this.secret_key};
            bus.$emit("save_config_param",{"name":"STANDALONE_AWS_CREDENTIALS","value":JSON.stringify(dat)});
        },
        saveMisc: function() {
            bus.$emit("save_config_param",{"name":"HUB_NAME","value":JSON.stringify(this.hub_name)});
            bus.$emit("save_config_param",{"name":"HUB_ICON","value":JSON.stringify(this.hub_icon)});
        }
    }
}
</script>

<style scoped>
.hide {
    display: none;
}
.field textarea {
    font-family: monospace !important;
    font-size: 0.9em !important;
}
input {
    font-family: monospace !important;
    font-size: 0.9em !important;
}
.datareleases p ~ div  {
    color: black;
}
.datareleases p {
    color: black;
}
</style>
