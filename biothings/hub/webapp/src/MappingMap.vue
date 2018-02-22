<template>
    <span>
        <div class="ui red basic label" v-if="dirty">Edited</div>
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
                        </div>
                    </div>
                </div>
            </div>
            <div class="actions">
                <div class="ui red basic cancel button">
                    <i class="remove icon"></i>
                    Cancel
                </div>
                <div class="ui green ok button">
                    <i class="checkmark icon"></i>
                    OK
                </div>
            </div>
        </div>

    </span>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'
import Vue from 'vue';

export default {
    name: 'mapping-map',
    props: ['map_id','map','name','read_only'],
    mounted () {
        console.log("MappingMap mounted");
        this.htmlmap();
        $('.ui.checkbox').checkbox();
                this.$forceUpdate();
    },
    created() {
        bus.$on("reload_mapping_map",this.$forceUpdate);
        bus.$on(`${this.name}-${this.map_id}-mapping_saved`,this.cleaned);
    },
    beforeDestroy() {
        bus.$off("reload_mapping_map",this.$forceUpdate);
        bus.$off(`${this.name}-${this.map_id}-mapping_saved`,this.cleaned);
    },
    data () {
        return {
            path : [],
            submap : {},
            dirty : false,
        }
    },
    computed: {
        strmap: function () {
            return JSON.stringify(this.map);
        },
        indexed: {
            cache : false,
            get : function() {
                if("index" in this.submap && this.submap["index"] === false) {
                    //console.log("indexed: false");
                    return false;
                } else {
                    //console.log("indexed: true");
                    return true;
                }
            },
            set : function(val) {
                //console.log(`dummy indexed setter ${val}`);
                if(val)
                    $(`#modal_${this.name}-${this.map_id} .copy_to_all`).removeClass("disabled");
                else
                    $(`#modal_${this.name}-${this.map_id} .copy_to_all`).addClass("disabled");
            }
        },
        copied_to_all: {
            cache : false,
            get : function() {
                if("copy_to" in this.submap 
                    //&& typeof this.submap["copy_to"] == "array"
                    &&  this.submap["copy_to"].indexOf("all") != -1) {
                    //console.log("copied_to_all: true");
                    return true;
                } else {
                    //console.log("copied_to_all: false");
                    return false;
                }
            },
            set : function(val) {
                //console.log(`dummy copied_to_all setter ${val}`);
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
        generateField: function(map_id,name,path) {
            return `<a class='mapkey' id='${path}/${name}' map_id='${map_id}'>${name}</a>`;
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
        modifyMapKey: function(event) {
            var key = event.currentTarget.innerText;
            var path = event.currentTarget.id;
            var map_id = event.currentTarget.getAttribute("map_id");
            if(`${this.name}-${this.map_id}` != map_id)
                return;
            console.log(`in modifyMapKey ${this.name}-${this.map_id}`);
            console.log(map_id);
            $(`#modal_${this.name}-${this.map_id} span.key`).html(key);
            $(`#modal_${this.name}-${this.map_id} input.path`).val(path);
            $(`#modal_${this.name}-${this.map_id} span.path`).html(path.slice(1).replace(/\//g,'.'));
            // retrieve actual mapping rules
            var keys = path.split("/").slice(1)
            // position submap to explored key
            this.walkSubMap(this.map,keys);
            var self = this;
            $(`#modal_${this.name}-${this.map_id}`)
            .modal("setting", {
                onApprove: function () {
                    var index = $(`#modal_${self.name}-${self.map_id} #index_checkbox`).is(":checked");
                    var copy_to_all = $(`#modal_${self.name}-${self.map_id} #copy_to_all_checkbox`).is(":checked");
                    var path = $(`#modal_${self.name}-${self.map_id} input.path`).val()
                    console.log(`#modal_${self.name}-${self.map_id} input.path`);
                    console.log(`index form ${index}`);
                    console.log(`copy_to_all form ${copy_to_all}`);
                    console.log(`path form ${path}`);
                    // TODO: for now we only support outter keys to be modified
                    if("properties" in self.submap)
                        throw new Error("Only 'leaf' keys can be modified");
                    // copy to all ?
                    if(copy_to_all)
                        Vue.set(self.submap,"copy_to",["all"]);
                    else
                        Vue.delete(self.submap,"copy_to");
                    // index or not
                    if(index)
                        Vue.delete(self.submap,"index");
                    else {
                        Vue.set(self.submap,"index",false);
                        // make sure to delete this one even if selected, makes no senses when index is false
                        Vue.delete(self.submap,"copy_to");
                    }
                    self.walkSubMap(self.map,path.split("/").slice(1),self.submap)
                    self.dirty = true;
                    self.$forceUpdate();
                }
            })
            .modal(`show`);
        },
        htmlmap: function() {
            // deep copy
            var html = JSON.parse(JSON.stringify(this.map));
            console.log(`for ${this.map_id}`);
            console.log(this.map);
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
                        var field = self.generateField(`${self.name}-${self.map_id}`,key,path);
                        obj[field] = val;
                        // this key should later be deleted as it's been replaced.
                        // we can't do it there though as traversal would stop
                        return true;
                    }
                    return false;
                });
            }
            console.log(`#${this.name}-${this.map_id}`);
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
        }
    },
}
</script>

<style>
.list-label {
    padding-left: 1.5em;
}
</style>
