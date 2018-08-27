<template>
    <div id="loader" :data-html="'<div>Page loaded</div>'" data-position="bottom center">
            <i id="loading" class="big circle icon studiogrey"></i>
    </div>

</template>

<script>

export default {
    name: 'loader',
    // Note: we don't declare "source", it must be defined in subclass/mixed
    // (sometimes it's a prop, sometimes it's a data field
    mounted () {
        $('#loader')
      .popup({
        on: 'hover'
      });
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
            $("#loading").removeClass("flashing");
            $("#loading").removeClass("blinking");
            $("#loading").addClass("studiogrey");
            $("#loader").attr("data-html",'<div>Page loaded</div>');
        },
        loadhide: function() {
            $("#loading").removeClass("red");
            this.loaded();
        },
        loading: function() {
            this.loadhide(); // reset
            $("#loading").addClass("flashing");
            $("#loading").removeClass("studiogrey");
            $("#loader").attr("data-html",'<div>Loading</div>');
        },
        loaderror: function(err) {
            var msg = "<div><b>Error while loading page</b><br>";
            console.log(err);
            if(err.status != undefined)
                msg += "XHR code: " + err.status + "<br>";
            if(err.data != undefined && err.data.error != undefined)
                msg += "Error: " + err.data.error + "<br>";
            if(err.config) {
                msg += "Request: " + err.config.method.toUpperCase() + " <a href='" + err.config.url + "'>" + err.config.url + "</a>";
            }
            msg += "</div>";
            this.loaded();
            $("#loading").removeClass("studiogrey");
            $("#loading").addClass("red");
            $("#loading").addClass("blinking");
            $("#loader").attr("data-html",msg);
        }
    },
}
</script>

<style>

#loader.error {
    color: red;
}
#loader.error:before {
    border-color: red;
}

/* Flash class and keyframe animation */
.flashing{
  color:#208CBC;
	-webkit-animation: flash linear 0.4s infinite;
	animation: flash linear 0.4s infinite;
}
@-webkit-keyframes flash {
	0% { opacity: 1; } 
	50% { opacity: .1; } 
	100% { opacity: 1; }
}
@keyframes flash {
	0% { opacity: 1; } 
	50% { opacity: .1; } 
	100% { opacity: 1; }
}

.blinking {
  animation: blinker 0.8s linear infinite;
}

@keyframes blinker {
  10% { opacity: 0; }
}

.studiogrey {
  color: #808080;
}

</style>

