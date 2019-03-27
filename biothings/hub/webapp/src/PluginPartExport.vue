<template>
    <span v-if="part">
        <span v-if="ptype == 'dumper'">
            <span class="ui green tiny smallmsg message" v-if="part.status == 'ok'">
                Dumper <b>{{part.class}}</b> exported in file <code>{{part.file}}</code>
            </span>
            <span class="ui red tiny smallmsg message" v-else>
                {{part.message}}
            </span>
        </span>
        <span v-if="ptype =='uploader'">
            <span class="ui green tiny smallmsg message" v-if="part.status == 'ok'">
                Uploader <b>{{part.class}}</b> exported in file <code>{{part.file}}</code>
            </span>
            <span class="ui red tiny smallmsg message" v-else>
                {{part.message}}
            </span>
        </span>
        <span v-if="ptype == 'mapping'">
            <span class="ui green tiny smallmsg message" v-if="part.status == 'ok'">
                Mapping (origin <b>{{part.origin}}</b>) exported in file <code>{{part.file}}</code>
            </span>
            <span class="ui yellow tiny smallmsg message" v-else-if="part.status == 'warning'">
                {{part.message}}
            </span>
            <span class="ui red tiny smallmsg message" v-else>
                {{part.message}}
            </span>
        </span>
    </span>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'

export default {
    name: 'plugin-part-export',
    props: ['part','ptype'],
    mounted () {
    },
    beforeDestroy() {
    },
    components: { },
    data () {
      return {
      }
    },
    methods: {
    },
}
</script>

<style scoped>
.white {color:white !important;}
.smallmsg {padding: 0.4em;}
</style>
