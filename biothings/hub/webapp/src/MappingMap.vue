<template>
    <span>
        <pre v-if="map" id="htmlmap">
        </pre>
        <div class="description" v-else>No mapping data inspection</div>

        <div v-bind:id="'modal_' + name" class="ui modal">
            <div class="header">Modify index rules</div>
            <input class="path" type="hidden">
            <div class="content">
                <div class="ui centered grid">
                    <div class="six wide column">
                        <h5>Key: <b class="key"></b></h5>
                        <p>
                        <div class="index ui checkbox">
                            <input type="checkbox" name="index" id="index_checkbox" v-model="indexed">
                            <label>Index this field (searchable)</label>
                        </div>
                        </p>
                        <p>
                            <div :class="['copy_to_all ui checkbox', indexed ? '' : 'disabled']">
                            <input type="checkbox" name="copy_to_all" id="copy_to_all_checkbox" v-model="copied_to_all">
                            <label>Search this field by default</label>
                        </div>
                        </p>
                    </div>
                    <div class="six wide column">
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

export default {
    name: 'mapping-map',
    props: ['map','name'],
    mounted () {
        console.log("MappingMap mounted");
        this.htmlmap();
        $('.ui.checkbox').checkbox();
    },
    data () {
        return {
            path : [],
            submap : {},
        }
    },
    computed: {
        strmap: function () {
            return JSON.stringify(this.map);
        },
        indexed: { 
            get : function() {
                console.log(this.submap);
                if("index" in this.submap && this.submap["index"] === false) {
                    console.log("indexed: false");
                    return false;
                } else {
                    console.log("indexed: true");
                    return true;
                }
            },
            set : function(val) {
                console.log(`dummy indexed setter ${val}`);
                if(val)
                    $(`#modal_${this.name} .copy_to_all`).removeClass("disabled");
                else
                    $(`#modal_${this.name} .copy_to_all`).addClass("disabled");
            }
        },
        copied_to_all: {
            get : function() {
                console.log(this.submap);
                if("copy_to" in this.submap && this.submap["copy_to"] === ["all"]) {
                    //console.log("copied_to_all: true");
                    return true;
                } else {
                    console.log("copied_to_all: false");
                    return false;
                }
            },
            set : function(val) {
                //console.log(`dummy copied_to_all setter ${val}`);
            }
        }
    },
    watch: {
        submap : function(newv,oldv) {
            if(newv != oldv) {
                //console.log("watched go refresh");
                // we need to refresh component when submap is modified
                // so computed data related to explored key can be updated
                // propagate status
                this.$forceUpdate();
            }
        }
    },
    components: { },
    methods: {
        generateField: function(name,path) {
            return `<a class='mapkey' id='${path}/${name}'>${name}</a>`;
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
            $(`#modal_${this.name} b.key`).html(key);
            $(`#modal_${this.name} input.path`).val(path);
            // retrieve actual mapping rules
            var keys = path.split("/").slice(1)
            // position submap to explored key
            this.walkSubMap(this.map,keys);
            console.log(this.submap);
            var self = this;
            $(`#modal_${this.name}`)
            .modal("setting", {
                onApprove: function () {
                    console.log(`#modal_${self.name} input.index:checked`);
                    var index = $(`#modal_${self.name} #index_checkbox`).is(":checked");
                    var copy_to_all = $(`#modal_${self.name} #copy_to_all_checkbox`).is(":checked");
                    var path = $(`#modal_${self.name} input.path`).val()
                    console.log(`index form ${index}`);
                    console.log(`copy_to_all form ${copy_to_all}`);
                    console.log(`path form ${path}`);
                    console.log(self.submap);
                    // TODO: for now we only support outter keys to be modified
                    if("properties" in self.submap)
                        throw new Error("Only 'leaf' keys can be modified");
                    // index or not
                    if(index)
                        delete self.submap["index"];
                    else
                        self.submap["index"] = false;
                    // copy to all ?
                    if(copy_to_all)
                        self.submap["copy_to"] = ["all"];
                    else
                        delete self.submap["copy_to"];
                    self.walkSubMap(self.map,path.split("/").slice(1),self.submap)
                    console.log(self.map);
                    self.$forceUpdate();
                }
            })
            .modal(`show`);
        },
        htmlmap: function() {
            // deep copy
            var html = JSON.parse(JSON.stringify(this.map));
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
            traverse(html, function(key,val,obj,path){
                if(typeof val == "object" && ["properties","copy_to"].indexOf(key) == -1) {
                    var field = self.generateField(key,path);
                    obj[field] = val;
                    // this key should later be deleted as it's been replaced.
                    // we can't do it there though as traversal would stop
                    return true;
                }
                return false;
            });
            $("#htmlmap").html(JSON.stringify(html,null,4));
            $(".mapkey").bind('click',this.modifyMapKey);
        },
    },
}
</script>

<style>
.list-label {
    padding-left: 1.5em;
}
</style>
