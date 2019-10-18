<template>
</template>

<script>

import axios from 'axios'
import bus from './bus.js'

export default {
    name: 'async-command-launcher',
    // Note: we don't declare "source", it must be defined in subclass/mixed
    // (sometimes it's a prop, sometimes it's a data field
    mounted () {
        $.tablesort.DEBUG = true;
        this.watchdog();
    },
    components: { },
    created() {
      bus.$on('change_command',this.onCommandChanged);
    },
    beforeDestroy() {
      bus.$off('change_command',this.onCommandChanged);
      if(this.interval_ref) {
          clearInterval(this.interval_ref);
      }
    },
    data() {
        return {
            running : {},
            watchdog_running: false,
            watchdog_interval: 10000, // ms
            interval_ref : null,
        }
    },
    computed: {
    },
    methods: {
        watchdog: function() {
            // this produces polling to check commands. While this is managed
            // through event (onCommandChanged), if we miss the event (no ws connection)
            // we could get stuck, so at least we have that watchdoc constantly checking
            // for stuck commands
            this.interval_ref = setInterval(() => {
                console.log("Watchdog is watching");
                for(var cmd_id in this.running) {
                    if(this.running.hasOwnProperty(cmd_id)) {
                        console.log(`Watchdog fetch result for ${cmd_id}`);
                        // this will clear cmd_id from this.running if command is done
                        this.fetchResult(cmd_id);
                    }
                }
            },this.watchdog_interval);
        },
        launchAsyncCommand: function(cmd,callback,errback) {
            var self = this;
            self.loading();
            cmd()
            .then(response => {
                if(response.data.status != "ok") {
                    throw new Error(`Couldn't launch async command ${cmd}`);
                }
                if(response.data.result.is_done) {
                    // not async, but we can still propagate results to callback
                    callback(response);
                }
                // if we ge there, command is being launch, now need to wait.
                self.running[response.data.result.id] = {"cb" : callback, "eb" : errback}
                self.loaded();
            })
            .catch(err => {
                self.loaderror(err);
            });
        },
        onCommandChanged: function(cmd_id) {
            if(this.running.hasOwnProperty(cmd_id)) {
                this.fetchResult(cmd_id);
            }
        },
        fetchResult: function(cmd_id) {
            var self = this;
            axios.get(axios.defaults.baseURL + `/command/${cmd_id}`)
            .then(response => {
                if(response.data && response.data.result && response.data.result.is_done) {
                    if(response.data.result.failed) {
                        self.running[cmd_id]["eb"](response);
                    } else {
                        self.running[cmd_id]["cb"](response);
                    }
                    delete self.running[cmd_id];
                }
            })
            .catch(err => {
            });
        },
        extractAsyncError: function(err) {
            if(err.data && err.data.result && err.data.result.failed) {
                return err.data.result.results[0];
            } else {
                console.log("Can't extract async error, it's not an error")
                console.log(err);
            }
        }
    },
}
</script>

<style>
</style>

