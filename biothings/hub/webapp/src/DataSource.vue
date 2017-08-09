<template>
  <div id="data-source" class="ui green card">
    <div class="content">
      <a class="header">{{ source._id | capitalize}}</a>
      <div class="meta">
        <span class="date">Release {{ source.release }}</span>
      </div>
      <!--div class="description">
      Information about taxonomy names and nodes
      </div-->
    </div>
    <div class="extra content">
      <i class="file text icon"></i>
      <div >{{ source.count | currency('',0) }} document{{ source.count &gt; 1 ? "s" : "" }}</div>
    </div>
    <div class="ui icon buttons">
      <div class="mini ui animated button" tabindex="0">
        <div class="visible content">
          <i class="cloud download icon"></i>
        </div>
        <div class="hidden content">Download</div>
      </div>
      <div class="mini ui animated button" tabindex="0">
        <div class="visible content">
          <i class="database icon"></i>
        </div>
        <div class="hidden content">Integrate</div>
      </div>
      <div class="mini ui animated button" tabindex="0">
        <div class="visible content">
          <i class="configure icon"></i>
        </div>
        <div class="hidden content">Configure</div>
      </div>
    </div>
  </div>
</template>

<script>

export default {
  name: 'data-source',
  props: ['source'],
}
</script>

<style>
</style>
