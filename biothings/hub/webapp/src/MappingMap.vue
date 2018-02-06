<template>
    <span>
        <pre v-if="map">
{{map}}
        </pre>
        <div class="description" v-else>No mapping data inspection</div>
    </span>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'

export default {
    name: 'mapping-map',
    props: ['map'],
    mounted () {
        console.log(`in mappingmap ${this.map}`);
    },
    created() {
        bus.$on('mapping_map',this.update);
    },
    data () {
        return {
        }
    },
    components: { },
    methods: {
    },
}
</script>

<style>
.list-label {
    padding-left: 1.5em;
}
</style>
