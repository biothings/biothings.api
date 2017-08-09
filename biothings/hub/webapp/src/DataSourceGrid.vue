<template>
  <div class="ui main container" id="list_sources">
    <div class="ui segment">
      <h2 class="ui right floated header">Data sources</h2>
      <div class="ui clearing divider"></div>
      <div id="data-source-grid" class="ui grid">
        <div class="four wide column" v-for="source in sources">
          <data-source v-bind:source="source"></data-source>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import DataSource from './DataSource.vue';
export default {
  name: 'data-source-grid',
  props: ['sources'],
  components: { DataSource, },
  }
</script>

<style>
</style>
