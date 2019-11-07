<template>
    <tr>
        <td class="eight wide">
            <div class="param-name">
                {{ param.name }}
                <button :class="['ui mini label button', saving ? 'loading' : '']" v-bind:[save_disabled]="1" @click="saveParameter">
                    Save
                </button>
                <button :class="['ui mini label button', resetting ? 'loading' : '']" data-tooltip="Reset to default value" @click="resetParameter" v-bind:[reset_disabled]="1">
                    Reset
                </button>
                <a class="ui mini label" v-if="error" :data-tooltip="error">
                    <i class="red exclamation triangle icon"></i>
                </a>
                <a class="ui mini label" v-if="updated" data-tooltip="Value updated">
                    <i class="green check icon"></i>
                </a>
            </div>
            <div class="param-desc">{{ param.desc }}</div>
        </td>
        <td class="eight wide">
            <form class="ui form">
                <div class="field">
                    <span v-if="param.readonly">
                        <pre class="param-readonly">{{ displayed_value }}</pre>
                        <div class="param-legend">(value is read-only, it cannot be edited)</div>
                    </span>
                    <span v-else>
                        <span v-if="Array.isArray(param.value)">
                            <textarea v-model="displayed_value"></textarea>
                            <div class="param-legend" v-if="!param.diff">This is the default value</div>
                        </span>
                        <span v-else-if="typeof param.value === 'object'">
                            <textarea v-model="displayed_value"></textarea>
                            <div class="param-legend" v-if="!param.diff">This is the default value</div>
                        </span>
                        <span v-else>
                            <input :type="param.hidden? 'password' : 'text'" autocomplete="off" v-model="displayed_value"></input>
                            <div class="param-legend" v-if="!param.diff">This is the default value</div>
                        </span>
                    </span>
                </span>
                <div class="ui red basic label" v-if="json_error">Error parsing JSON: {{ json_error }}</div>
                </div>
            </form>
        </td>
    </tr>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'


// NOTE: this code is tricky, there's a lot of different states, inter-dependent,
// config values being stored but not taken into account until next reload,
// MODIFY WITH CAUTION
// Test case:
// 1. edit a paramter so it's not the default value
// 2. save
// 3. reset
// 4. re-modify
// 5. save
// 6. manualy revert back (don't use reset button)
// 7. save

export default {
    name: 'hub-config-param',
    props: ["param"],
    components: { },
    mixins : [ ],
    mounted () {
    },
    created() {
    },
    updated() {
    },
    beforeDestroy() {
    },
    data () {
        return {
            value: null, // edited value
            origvalue: null, // original value to track changes
            dirty: false,
            json_error: null,
            error: null,
            updated: false,
            saving: false,
            resetting: false,
        }
    },
    computed: {
        // a computed getter
        displayed_value: {
            get: function() { 
                if(this.origvalue == null) {
                    this.value = JSON.stringify(this.param.value,null,2);
                    this.origvalue = this.param.value; // track changes
                }
                return this.value;
            },
            set: function(val) {
                this.value = val;
            }
        },
        save_disabled: function() {
            return (this.dirty && !this.json_error) ? "" : "disabled";
        },
        reset_disabled: function() {
            return this.param.diff ? "" : "disabled";
        },
        is_default: function() {
            return JSON.stringify(this.param.default) == JSON.stringify(JSON.parse(this.displayed_value));
        },
    },
    watch: {
        value : function (newv, oldv) {
            // content has changed, reset global error
            this.error = null;
            this.updated = false;
            try {
                if(JSON.stringify(JSON.parse(newv)) != JSON.stringify(this.origvalue)) {
                    this.dirty = true;
                } else {
                    this.dirty = false;
                }
                // if we get there, no json syntax error
                this.json_error = null; // reset
            } catch (err) {
                this.dirty = true;
                this.json_error = err.name + ": " + err.message;
            }

        },
    },
    methods: {
        saveParameter() {
            // sanity check, just in case
            var val = JSON.parse(JSON.stringify(this.value));
            var self = this;
            self.saving = true;
            axios.put(axios.defaults.baseURL + `/config`, {"name" : self.param.name, "value" : val})
            .then(response => {
                    console.log(response);
                    if(response.status == 200) {
                        self.dirty = false;
                        self.updated = true;
                        // new original value to start tracking new changes
                        self.origvalue = JSON.parse(self.value);
                    }
                self.saving = false;
            })
            .catch(err => {
                console.log("Error saving parameter: " + err);
                self.error = err.message;
                self.saving = false;
            })
        },
        resetParameter() {
            var self = this;
            self.resetting = true;
            axios.delete(axios.defaults.baseURL + `/config`, {"data": {"name" : self.param.name}})
            .then(response => {
                    // back to default, should be done in displayed_value
                    // but can't find an easy way to do it...
                    self.value = JSON.stringify(self.param.default,null,2);
                    self.origvalue = self.param.default;
                    self.resetting = false;
            })
            .catch(err => {
                console.log("Error resetting parameter: " + err);
                self.error = err.message;
                self.resetting = false;
            })
        },
    },
}
</script>

<style scoped>
.param-name {
    font-family: monospace;
    font-weight: bold;
}
.param-desc {
    color: grey;
    word-wrap: break-word;
    hyphens: auto;
}
.param-legend {
    font-style: italic;
    color: grey;
    font-size: smaller;
}
.param-readonly {
    color: grey;
}
.field textarea {
    font-family: monospace !important;
    font-size: 0.9em !important;
}
.field input {
    font-family: monospace !important;
    font-size: 0.9em !important;
}
</style>
