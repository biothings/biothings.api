<template>
  <div id="data-source" class="ui card">
    <div class="content">

      <i class="right floated lock icon orange"
        v-if="source.locked"></i>
      <i class="right floated database icon uploading"
        v-if="source.upload && source.upload.status == 'uploading'"></i>
      <i class="right floated cloud download icon downloading"
        v-if="source.download && source.download.status == 'downloading'"></i>

      <div class="left aligned header" v-if="source.name">{{ source.name | splitjoin | capitalize }}</div>
      <div class="meta">
        <span class="right floated time" v-if="source.download">{{ source.download.started_at | moment("from", "now") }}</span>
        <span class="right floated time" v-else>Never</span>
        <span class="left floated category">{{ source.release }}</span>
      </div>
      <div class="left aligned description">
        <p>
          <div class="ui clearing divider"></div>
          <div>
            <i class="file outline icon"></i>
            {{ source.count | currency('',0) }} document{{ source.count &gt; 1 ? "s" : "" }}
          </div>
        </p>
      </div>
    </div>
    <!--div class="extra content">
      <span class="left floated like">
        <i class="like icon"></i>
        Like
      </span>
      <span class="right floated star">
        <i class="star icon"></i>
        Favorite
      </span>
    </div-->
  </div>
</template>

<script>
export default {
  name: 'data-source',
  props: ['source'],
}
/*$('.bla')
// if a direction if specified it will be obeyed
.transition('horizontal flip in')
.transition('vertical flip in')
;*/
</script>

<style>
  @keyframes pulse {
    0% {transform: scale(1, 1);}
    50% {transform: scale(1.2, 1.2);}
    100% {transform: scale(1, 1);}
  }

  .downloading {
    animation: pulse 1s linear infinite;
  }
  .uploading {
    animation: pulse 1s linear infinite;
  }
</style>
