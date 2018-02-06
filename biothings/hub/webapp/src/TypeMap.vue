<template>
    <span>
        <pre v-if="map">
{{map}}
        </pre>
        <div class="description" v-else>No type data inspection</div>
    </span>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'

export default {
    name: 'type-map',
    props: ['map'],
    mounted () {
    },
    /*// this is a terrible design, it's to avoid the component to update
    // and delete "map"
    beforeUpdate() {
        if(this._map)
            this.map = this._map;
    },
    // here we store the current value for later restore
    updated() {
        this._map = this.map;
    },*/
    created() {
        bus.$on('type_map',this.update);
    },
    data () {
        return {
        }
    },
    components: { },
    methods: {
        extractType (val) {
            var types = [];
            var re = /<class '(.*)'>/;
            var self = this;
            function check(k,v) {
                var match = re.exec(k);
                if(match) {
                    if(match[1] == "list") {
                        var m = self.extractType(v);
                        if(m)
                            // it's a list of scalar type
                            types.push(`list[${m}]`);
                        else
                            // it's a list of complex types, return "list" so it can be explored further
                            types.push("list");
                    } else {
                        types.push(match[1]);
                    }
                }
            }
            if(typeof val == "object") {
                for(var k in val) {
                    check(k,val[k]);
                }
            } else if(typeof val == "string") {
                    check(val,null);
            }
            if(types.length)
                var res = types.join(",");
            else
                var res = null;
            //console.log(`val: ${val} => ${res}`);
            return res;
        },
    },
}
</script>

<style>
.list-label {
    padding-left: 1.5em;
}
</style>
