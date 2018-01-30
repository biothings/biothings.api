<template>
    <div class="ui bulleted list" v-if="map">
        <div class="item" v-for="field in Object.keys(map).sort()">
            <span v-if="extractType(field) == 'list'">
                <i class="list layout icon"></i><code class="list-label">list</code>
            </span>
            <b v-else>{{ field }}</b>
            <!-- check key: if 'list' needs to explore -->
            <span v-if="extractType(field) == 'list'">
                <type-map v-bind:map="map[field]"></type-map>
            </span>
            <!-- check value: if leaf, print type -->
            <span v-else-if="extractType(map[field]) && extractType(map[field]) != 'list'"><i class="long arrow right icon"></i><code>{{extractType(map[field])}}</code></span>
            <!-- if none of above, it's an object, needs explore -->
            <span v-else>
                <type-map v-bind:map="map[field]"></type-map>
            </span>
        </div>
    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'

export default {
    name: 'type-map',
    props: ['map','mainsrc','subsrc'],
    mounted () {
        bus.$on('type_map',this.update);
    },
    // this is a terrible design, it's to avoid the component to update
    // and delete "map"
    beforeUpdate() {
        if(this._map)
            this.map = this._map;
    },
    // here we store the current value for later restore
    updated() {
        this._map = this.map;
    },
    created() {
    },
    data () {
        return {
            _map : null,
        }
    },
    components: { },
    methods: {
        update (mainsrc,subsrc,map) {
            if(mainsrc == this.mainsrc && subsrc == this.subsrc) {
            console.log("onela");
            console.log(`${mainsrc} ${subsrc}`);
            console.log(map);
            // This sets a props, which is forbidden...
            // the problem is we need "map" to be a prop because
            // template is called recursively and we need to v-bind:map
            this.map = map;
            }
        },
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
