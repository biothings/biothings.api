<template>
</template>

<script>

export default {
    methods: {
        html2json: function(html) {
            // remove html tags to get a clean json doc
            html = html ? html : "{}";
            html = html.replace(/<.*?>/g,""); // non-greedy to keep content between tags
            try {
                return JSON.parse(html);
            } catch(err) {
                console.log(`Error parsing mapping: ${err}`);
                console.log(html);
                throw err;
            }
        },
    },
}

</script>
