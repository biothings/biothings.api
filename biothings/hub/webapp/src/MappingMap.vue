<template>
    <span>
        <pre v-if="map" id="htmlmap">
        </pre>
        <div class="description" v-else>No mapping data inspection</div>
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
    },
    created() {
        bus.$on('save_mapping',this.saveMapping);
    },
    beforeDestroy() {
        bus.$off('save_mapping',this.saveMapping);
    },
    data () {
        return {
            i : 0
        }
    },
    computed: {
        strmap: function () {
            return JSON.stringify(this.map);
        },
    },
    components: { },
    methods: {
        generateField: function(name) {
            return `<a class='mapkey'>${name}</a>`;
        },
        modifyMapKey: function(o) {
            delete this.map["clinvar"]["properties"]["gene"];
            this.htmlmap();
        },
        htmlmap: function() {
            // deep copy
            var html = jQuery.extend(true, {}, this.map);
            var self = this;
            var traverse = function(o, fn) {
                for (var i in o) {
                    var to_delete = fn.apply(this,[i,o[i],o]);  
                    if (o[i] !== null && typeof(o[i])=="object") {
                        traverse(o[i], fn);
                        // now we can delete the fully explored key
                        if(to_delete) {
                            delete o[i];
                        }
                    }
                }
            }
            // usage
            traverse(html, function(key,val,o){
                if(typeof val == "object" && key != "properties") {
                    var field = self.generateField(key);
                    o[field] = val;
                    // this key should later be deleted as it's been replaced.
                    // we can't do it there though as traversal would stop
                    return true;
                }
                return false;
            });
            $("#htmlmap").html(JSON.stringify(html,null,4));
            $(".mapkey").bind('click',this.modifyMapKey);
        },
        saveMapping: function() {
            var html = $("#htmlmap").html();
            // remove html tags to get a clean json doc
            html = html.replace(/<.*>(\w+)<.*>/g,"$1");
            var json = null;
            try {
                json = JSON.parse(html);
            } catch(err) {
                console.log(`Error parsing mapping: ${err}`);
            }
            axios.put(axios.defaults.baseURL + `/source/${this.name}/mapping`,
                        {"mapping" : json})
            .then(response => {
                console.log(response.data.result)
            })
            .catch(err => {
                console.log("Error : " + err);
            })
        }
    },
}
</script>

<style>
.list-label {
    padding-left: 1.5em;
}
</style>
