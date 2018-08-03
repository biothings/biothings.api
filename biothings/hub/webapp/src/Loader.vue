<template>
    <span>
        <div class="ui active inverted dimmer loading">
            <div class="ui text loader">Loading</div>
        </div>
        <div class="ui inverted dimmer error">
            <div class="ui indeterminate text loader error">
                <div class="ui warning message">
                    <div class="header">
                        Error while loading page
                    </div>
                    <span class="loader error message">message</span>
                </div>
            </div>
        </div>
    </span>
</template>

<script>

export default {
    name: 'loader',
    // Note: we don't declare "source", it must be defined in subclass/mixed
    // (sometimes it's a prop, sometimes it's a data field
    mounted () {
    },
    components: { },
    created() {
    },
    beforeDestroy() {
    },
    computed: {
    },
    methods: {
        loaded: function() {
            $(".ui.dimmer.loading").removeClass("active");
        },
        loading: function() {
            $(".ui.dimmer.loading").addClass("active");
        },
        loaderror: function(err) {
            var msg = "";
            console.log(err);
            if(err.status != undefined)
                msg += "Status code: " + err.status + "<br>";
            if(err.data != undefined && err.data.error != undefined)
                msg += "Error: " + err.data.error + "<br>";
            if(err.config) {
                msg += "Request: " + err.config.method.toUpperCase() + " <a href='" + err.config.url + "'>" + err.config.url + "</a>";
            }
            this.loaded();
            $(".ui.dimmer.error").addClass("active");
            $(".loader.error.message").html(msg);
        }
    },
}
</script>

<style>
a {
    color: #0b0089;
}

.ui.inverted.dimmer .ui.loader.error {
    color: red;
}
.ui.inverted.dimmer .ui.loader.error:before {
    border-color: red;
}


</style>

