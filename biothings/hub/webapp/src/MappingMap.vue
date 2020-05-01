<template>
    <span>

        <h3 class="center aligned" v-if="map_origin == 'inspect'">Mapping from inspection</h3>
        <h3 class="center aligned" v-else-if="map_origin == 'master'">Registered mapping</h3>
        <h3 class="center aligned" v-else-if="map_origin == 'build'">Merged mapping</h3>
        <div class="center aligned" :class="actionable">
            <button class="ui labeled mini icon button" 
                v-if="map && !read_only
                && !map['errors']
                && !map['pre-mapping']"
                v-on:click="saveMapping()">
                <i class="save icon"></i>
                Save
            </button>
            <div class="ui mini buttons" v-if="map">
                <div class="ui labeled icon button"
                    v-on:click="testMapping()">
                    <i class="check square outline icon"></i>Validate on <span :class='[name,map_id,"test-on"]'>...</span>
                </div>
                <div :class="['ui floating', map_id, 'dropdown icon button']">
                    <i class="dropdown icon"></i>
                    <div class="menu">
                    </div>
                </div>
            </div>
            <button class="ui labeled mini icon button"
                v-if="can_commit != false && map_origin == 'inspect'"
                v-on:click="commitMapping()">
                <i class="angle double right icon"></i>
                Commit
            </button>
        </div>

        <div class="ui orange basic label" v-if="dirty">Edited</div>
        <br>
        <div :class="['ui',mapping_error ? 'red' : 'green', 'label']" v-if="mapping_msg != null">
            <span v-if="mapping_error">
                An error occured while validating this mapping:<br>
                <code>{{mapping_msg}}</code>
            </span>
            <span v-else>
                Mapping has successfully been validated.
                <code>{{mapping_msg}}</code>
            </span>
        </div>
        <span v-if="map">
            <div v-if="has_errors">
                <a class="ui orange label"><i class="exclamation triangle icon"></i>Found errors while generating the mapping:</a>
                <div class="ui bulleted list">
                    <div class="item" v-for="err in map['errors']"><b>{{err}}</b></div>
                </div>
                <p>Mapping can't be generated until those errors are fixed. Please fix the parser or the data and try again.</p>
                <a class="ui grey label">For debugging purposes, below is a pre-mapping structure, where errors can be spot.</a>
            </div>
            <pre :id="name + '-' + map_id">
            </pre>
        </span>
        <div class="description" v-else>No mapping data</div>

        <div v-bind:id="'modal_' + name + '-' + map_id" class="ui modal">
            <div class="header">Modify indexing rules</div>
            <input class="path" type="hidden">
            <div class="content">
                <div class="ui centered grid">
                    <div class="six wide column">
                        <h5>
                            Field: <span class="key"></span><br>
                            Path: <span class="path"></span>
                        </h5>
                        <p>
                            <div class="index ui checkbox">
                                <input type="checkbox" name="index" id="index_checkbox" v-model="indexed">
                                <label>Index this field</label>
                            </div>
                        </p>
                        <p>
                            <div :class="['copy_to_all ui checkbox', indexed ? '' : 'disabled']">
                                <input type="checkbox" name="copy_to_all" id="copy_to_all_checkbox" v-model="copied_to_all">
                                <label>Search this field by default</label>
                            </div>
                        </p>
                        <p>
                            <div class="index ui">
                                <label>Change type</label>
                                <div class="ui selection estype dropdown" id="estype">
                                  <div class="text"></div>
                                  <i class="dropdown icon"></i>
                                </div>
                            </div>
                        </p>
                        <p>
                          <div class="six wide field">
                            <textarea class="json" rows="5" id="submap" v-model="strsubmap">{{ strsubmap }}</textarea>
                            <div class="ui mini negative message" v-if="json_err">
                                <p>Invalid JSON format</p>
                            </div>
                          </div>
                        </p>
                    </div>
                    <div class="ten wide column">
                        <div class="ui list">
                            <a class="item">
                                <i class="right triangle icon"></i>
                                <div class="content">
                                    <div class="description">
                                        <b>Enable index</b> allows a field to be searchable.
                                        If indexing is disabled, values are still stored and returned in results,
                                        but they can't be directly queried. Indexing takes disk space and can also
                                        impact performances, only index fields which make sense to query.
                                    </div>
                                </div>
                            </a>
                            <a class="item">
                                <i class="right triangle icon"></i>
                                <div class="content">
                                    <div class="description">
                                        When <b>Search by default</b> is enabled, field can be searched without specifying the full path.
                                        <i>Note: _id field is an exception, path is not required.</i><br>
                                        Ex:
                                        <ul class="ui list">
                                            <li>When searching by default is disabled, searching <b><span class="key"></span></b> field requires to specify the full path:<br>
                                                <code>/query?q=<b><span class="path"></span>:value_to_search</b></code></li>
                                            <li>When searching by default is enabled, path can be omitted, <i>value_to_search</i> will be searched
                                                in all fields declared as searchable by default.<br>
                                                <code>/query?q=<b>value_to_search</b></code>.
                                            </li>
                                        </ul>
                                    </div>
                                </div>
                            </a>
                            <a class="item">
                                <i class="right triangle icon"></i>
                                <div class="content">
                                    <div class="description">
                                      ElasticSearch field data type can be changed if needed. See 
                                      <a target="_blank" href="https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html">
                                        Field datatypes
                                      </a> for more information.
                                      <i>Note: only core datatypes are available in this list </i>
                                    </div>
                                </div>
                            </a>
                            <a class="item">
                                <i class="right triangle icon"></i>
                                <div class="content">
                                    <div class="description">
                                        Field definition can also be manually specificed in the text box, using JSON notation, for more advanced usage.
                                    </div>
                                </div>
                            </a>
                        </div>
                    </div>
                </div>
            </div>
            <div class="actions">
                <div class="ui red basic cancel button">
                    <i class="remove icon"></i>
                    Cancel
                </div>
                <div class="ui green ok button" :class="actionable">
                    <i class="checkmark icon"></i>
                    OK
                </div>
            </div>
        </div>

        <div v-bind:id="'modal_commit_' + name" class="ui modal">
            <div class="ui icon header">
                <i class="angle double right icon"></i>
                Commit new mapping
            </div>
            <div class="content">
                <p>Are you sure you want to commit mapping from inspection?</p>
                <p>This will be replace current registered one.</p>
            </div>
            <div class="actions">
                <div class="ui red basic cancel button">
                    <i class="remove icon"></i>
                    No
                </div>
                <div class="ui green ok button">
                    <i class="checkmark icon"></i>
                    Yes
                </div>
            </div>
        </div>

    </span>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import Vue from 'vue';
import Utils from './Utils.vue'
import Actionable from './Actionable.vue'


export default {
    name: 'mapping-map',
    props: ['map_id','map','name','read_only','map_origin','can_commit','entity'],
    mixins: [Utils, Actionable, ],
    mounted () {
        console.log("MappingMap mounted");
        this.htmlmap();
        $('.ui.checkbox').checkbox();
        this.$forceUpdate();
        this.buildIndexDropdown();
    },
    filters : {
      log: (value) => {console.log(`in log ${value}`); return value;},
    },
    // TODO: those events are mostly due because of bad page design, with actions on a mapping separated from
    // the actual component (see DataSourceMapping.vue and the relation)
    created() {
        bus.$on("reload_mapping_map",this.$forceUpdate);
        bus.$on(`mapping_test_${this.map_id}-${this.name}`,this.displayMappingTestResult);
    },
    beforeDestroy() {
        bus.$off("reload_mapping_map",this.$forceUpdate);
        bus.$off(`mapping_test_${this.map_id}-${this.name}`,this.displayMappingTestResult);
    },
    data () {
        return {
            path : [],
            submap : {},
            dirty : false,
            mapping_error : false,
            mapping_msg: null,
            environments : {},
            estypes : ["text","keyword","long", "integer", "short", "byte", "double", "float",
                "half_float", "scaled_float","date","boolean","binary","integer_range",
                "float_range", "long_range", "double_range", "date_range"],
            selected_type: null,
            json_err: false,
            tmp_strsubmap: null, // hold manually edited map until valid JSON
        }
    },
    computed: {
        strmap: function () {
            return JSON.stringify(this.map);
        },
        strsubmap: {
            get: function() {
              if(this.tmp_strsubmap)
                return this.tmp_strsubmap;
              else
                return JSON.stringify(this.submap,null,2);
            },
            set: function(val) {
                try {
                    this.submap = JSON.parse(val);
                    this.json_err = false;
                    this.tmp_strsubmap = null;
                } catch (err) {
                    this.json_err = true;
                    this.tmp_strsubmap = val;

                }
            }
        },
        indexed: {
            cache : false,
            get : function() {
                if("index" in this.submap && this.submap["index"] === false) {
                    return false;
                } else {
                    return true;
                }
            },
            set : function(val) {
                if(val)
                    $(`#modal_${this.name}-${this.map_id} .copy_to_all`).removeClass("disabled");
                else
                    $(`#modal_${this.name}-${this.map_id} .copy_to_all`).addClass("disabled");
                this.updateSubMap();
            }
        },
        copied_to_all: {
            cache : false,
            get : function() {
                if("copy_to" in this.submap 
                    //&& typeof this.submap["copy_to"] == "array"
                    &&  this.submap["copy_to"].indexOf("all") != -1) {
                    return true;
                } else {
                    return false;
                }
            },
            set : function(val) {
                this.updateSubMap();
            }
        },
        current_type : {
            cache : false,
            get : function() {
                return this.submap["type"];
            },
            set : function(val) {
            }
        },
        has_errors : function() {
            return "pre-mapping" in this.map && "errors" in this.map;
        }
    },
    watch: {
        submap : function(newv,oldv) {
            if(newv != oldv) {
                // we need to refresh component when submap is modified
                // so computed data related to explored key can be updated
                // propagate status
                this.$forceUpdate();
            }
        },
        map : function(newv,oldv) {
            if(newv != oldv) {
                this.htmlmap();
            }
        }
    },
    components: { },
    methods: {
        generateField: function(map_id,name,path,leaf) {
            if(leaf)
              return `<a class='mapkey leaf' id='${path}/${name}' map_id='${map_id}'>${name}</a>`;
            else
              return `<span class='non-leaf'>${name}</span>`;

        },
        walkSubMap: function(map,path,replace) {
            if(path.length) {
                if(path.length == 1) {
                    if(replace) {
                        // objects are passed by reference so original map will
                        // be modified in-place, inner
                        map[path[0]] = replace;
                        this.htmlmap();
                    } else {
                        // just continue to explore
                        this.walkSubMap(map[path[0]],path.slice(1),replace);
                    }
                } else {
                    this.walkSubMap(map[path[0]],path.slice(1),replace);
                }
            }
            else {
                this.submap = map;
            }
        },
        updateSubMap: function() {
            var index = $(`#modal_${this.name}-${this.map_id} #index_checkbox`).is(":checked");
            var copy_to_all = $(`#modal_${this.name}-${this.map_id} #copy_to_all_checkbox`).is(":checked");
            var path = $(`#modal_${this.name}-${this.map_id} input.path`).val()
            // TODO: for now we only support outter keys to be modified
            if("properties" in this.submap) {
                // this shouldn't happen since non-leaf aren't clickable
                throw new Error("Only 'leaf' keys can be modified");
            }
            // copy to all ?
            if(copy_to_all)
                Vue.set(this.submap,"copy_to",["all"]);
            else
                Vue.delete(this.submap,"copy_to");
            // index or not
            if(index)
                Vue.delete(this.submap,"index");
            else {
                Vue.set(this.submap,"index",false);
                // make sure to delete this one even if selected, makes no senses when index is false
                Vue.delete(this.submap,"copy_to");
            }
            Vue.set(this.submap,"type",this.selected_type);
        },
        modifyMapKey: function(event) {
            var key = event.currentTarget.innerText;
            var path = event.currentTarget.id;
            var map_id = event.currentTarget.getAttribute("map_id");
            if(`${this.name}-${this.map_id}` != map_id)
                return;
            $(`#modal_${this.name}-${this.map_id} span.key`).html(key);
            $(`#modal_${this.name}-${this.map_id} input.path`).val(path);
            $(`#modal_${this.name}-${this.map_id} span.path`).html(path.slice(1).replace(/\//g,'.').replace(".properties",""));
            // retrieve actual mapping rules
            var keys = path.split("/").slice(1)
            // position submap to explored key
            this.walkSubMap(this.map,keys);
            var self = this;
                var typs = [];
                typs = this.estypes.map(typ => ({"name":typ,"value":typ,"selected":typ == this.current_type}))
                $(`.ui.estype.dropdown`).dropdown({
                    values: typs,
                    onChange: function(value, text, $selectedItem) {
                        self.selected_type = value;
                        self.updateSubMap();
                    }
                });
            $(`#modal_${this.name}-${this.map_id}`)
            .modal("setting", {
                onApprove: function () {
                    self.walkSubMap(self.map,path.split("/").slice(1),self.submap)
                    self.dirty = true;
                    self.$forceUpdate();
                }
            })
            .modal(`show`);
        },
        htmlmap: function() {
            // deep copy
            if(!this.map)
                return;
            var html = JSON.parse(JSON.stringify(this.map));
            if(!this.read_only) {
                var self = this;
                var traverse = function(obj, fn, path = '') {
                    for (var i in obj) {
                        var to_delete = fn.apply(this,[i,obj[i],obj, path]);
                        if (obj[i] !== null && typeof(obj[i])=="object") {
                            traverse(obj[i], fn, `${path}/${i}`);
                            // now we can delete the fully explored key
                            if(to_delete) {
                                delete obj[i];
                            }
                        }
                    }
                }
                // usage
                var self = this;
                traverse(html, function(key,val,obj,path){
                    if(typeof val == "object" && ["properties","copy_to"].indexOf(key) == -1) {
                        var leaf = true;
                        if("properties" in obj[key])
                            leaf = false;
                        var field = self.generateField(`${self.name}-${self.map_id}`,key,path,leaf);
                        obj[field] = val;
                        // this key should later be deleted as it's been replaced.
                        // we can't do it there though as traversal would stop
                        return true;
                    }
                    return false;
                });
            }
            if(this.read_only && this.has_errors && "pre-mapping" in this.map)
                // text() to escape any <class ...>
                $(`#${this.name}-${this.map_id}`).text(JSON.stringify(this.map["pre-mapping"],null,4));
            else {
                $(`#${this.name}-${this.map_id}`).html(JSON.stringify(html,null,4));
                $(".mapkey").bind('click',this.modifyMapKey);
            }
        },
        cleaned : function() {
            this.dirty = false;
        },
        displayMappingTestResult: function(msg,type) {
            if(type == "error")
                this.mapping_error = true;
            else
                this.mapping_error = false;
            this.mapping_msg = msg;
        },
        saveMapping: function(dest=null) {
            var html = $(`#${this.name}-${this.map_id}`).html();
            var json = this.html2json(html);
            if(!dest)
              dest = this.map_origin;
            console.log(`Saving mapping for ${this.name} dest:${dest}`);
            axios.put(axios.defaults.baseURL + `/${this.entity}/${this.name}/mapping`,
                        {"mapping" : json, "dest" : dest})
            .then(response => {
                console.log(response.data.result)
                this.cleaned();
            })
            .catch(err => {
                console.log("Error : " + err);
            })
        },
        commitMapping: function() {
            var self = this;
            var dest = this.entity == 'source' ? 'master' : 'build';
            $(`#modal_commit_${self.name}`)
            .modal("setting", {
                onApprove: function () {
                    self.saveMapping(dest);
                }
            })
            .modal(`show`);
        },
        testMapping: function() {
            var html = $(`#${this.name}-${this.map_id}`).html();
            // while a mapping on its own can have multiple root keys, if sent as is,
            // ES will complain about more than one root key (doc_type) so embed the whole
            // under a defined root key
            var json = {"root_key" : {"properties" : this.html2json(html)}};
            var env = $(`.${this.name}.${this.map_id}.test-on`).text();
            axios.post(axios.defaults.baseURL + `/mapping/validate`,{"mapping" : json, "env" : env})
            .then(response => {
                console.log(response.data.result)
                bus.$emit(`mapping_test_${this.map_id}-${this.name}`,"","info");
            })
            .catch(err => {
                console.log("Error validating mapping: ");
                console.log(err);
                bus.$emit(`mapping_test_${this.map_id}-${this.name}`,err.data.error,"error");
            })
        },
        buildIndexDropdown : function() {
            var self = this;
            axios.get(axios.defaults.baseURL + `/index_manager`)
            .then(response => {
                this.environments = Object.keys(response.data.result.env);
                var envs = [];
                var cnt = 0;
                for(var e in this.environments) {
                    var d = {"name" : this.environments[e], "value" : this.environments[e]}
                    if(cnt == 0)
                        d["selected"] = true;
                    envs.push(d);
                    cnt++;
                }
                $(`.ui.${self.map_id}.dropdown`).dropdown({
                    values: envs,
                    onChange: function(value, text, $selectedItem) {
                        $(`.${self.map_id}.test-on`).text(`${value}`);
                    }
                });
            })
            .catch(err => {
                console.log("Error getting index environments: " + err);
            })
        },
    },
}
</script>

<style>
.list-label {
    padding-left: 1.5em;
}
.json {
    width: 100%;
    color: #4183c4;
    font-family: monospace,monospace;
    font-size: 0.8em;
}
.non-leaf {
  font-weight: bold;
}
.leaf {
  font-weight: bold;
  background-color: #ececec;
}
</style>
