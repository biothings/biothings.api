<template>
    <div class="ui card">
        <div class="content">
            <!-- in progress -->
            <i class="right floated database icon pulsing"
                v-if="build.status == 'building'"></i>

            <!-- error -->
            <div class="ui"
                v-bind:data-tooltip="displayError()">
                <i class="right floated red alarm icon pulsing"
                    v-if="build.status == 'failed'"></i>
            </div>

            <div class="left aligned header" v-model="build">{{ build.target_name }}</div>
            <div class="meta">
                <span class="right floated time" v-model="build">Built {{ build.started_at | moment("from","now") }}</span>
                <span class="left floated category" v-model="build">{{ build._meta.build_version }}</span>
            </div>
            <div class="left aligned description">
                <p>
                    <div>
                        <i class="file outline icon"></i>
                        {{ build.count | currency('',0) }} document{{ build.count &gt; 1 ? "s" : "" }}
                    </div>
                </p>
            </div>

            <div class="ui clearing divider"></div>

            <div class="left aligned description">
                <p>
                    <div class="ui top attached pointing secondary menu">
                        <a class="item active" data-tab="sources">Sources</a>
                        <a class="item" data-tab="stats">Stats</a>
                        <a class="item" data-tab="logs">Logs</a>
                    </div>
                    <div class="ui bottom attached tab segment active" data-tab="sources">
                        <!-- TODO: as a component -->
                        <table class="ui small very compact blue table">
                            <thead>
                                <tr>
                                    <th>Datasource</th>
                                    <th>Version</th>
                                </tr>
                            </thead>
                            <tbody v-if="build._meta.src">
                                <tr v-for="(info,src) in build._meta.src">
                                    <td v-if="info.url"><a :href="info.url">{{src}}</a></td>
                                    <td v-else>{{src}}</td>
                                    <td>{{info.version}}</td>
                                </tr>
                            </tbody>
                            <tfoot v-else>
                                <tr><th>No data</th>
                                    <th></th>
                                </tr>
                            </tfoot>
                        </table>
                    </div>

                    <div class="ui bottom attached tab segment" data-tab="stats">
                        <!-- TODO: as a component -->
                        <table class="ui small very compact grey table">
                            <thead>
                                <tr>
                                    <th>Stats</th>
                                    <th>Count</th>
                                </tr>
                            </thead>
                            <tbody v-if="build._meta.stats">
                                <tr v-for="(count,stat) in build._meta.stats">
                                    <td >{{stat}}</td>
                                    <td>{{count}}</td>
                                </tr>
                            </tbody>
                            <tfoot v-else>
                                <tr><th>No data</th>
                                    <th></th>
                                </tr>
                            </tfoot>
                        </table>
                    </div>

                    <div class="ui bottom attached tab segment" data-tab="logs">
                        <!-- TODO: as a component -->
                        <div class="ui small feed" v-if="build.jobs">
                            <div class="event">
                                <i class="ui hourglass start icon"></i>
                                <div class="content">
                                    <div class="summary">
                                        Build starts
                                        <div class="date">
                                        {{build.started_at | moment('MMM Do YYYY, h:mm:ss a')}}
                                        </div>
                                    </div>
                                </div>
                            </div>

                            <div class="event" v-for="job in build.jobs">
                                    <i class="ui green checkmark icon" v-if="job.status == 'success'"></i>
                                    <i class="ui red warning sign icon" v-else-if="job.status == 'failed'"></i>
                                    <i class="ui pulsing cube icon" v-else></i>
                                <div class="content">
                                    <div class="summary">
                                        {{job.step}}
                                        <div class="date">
                                            {{job.time}}
                                        </div>
                                    </div>
                                    <div class="meta" v-if="job.sources">
                                        <i class="database icon"></i>{{job.sources.join(", ")}}
                                    </div>
                                    <div class="meta" v-if="job.err">
                                        <i class="warning icon"></i>{{job.err}}
                                    </div>
                                </div>

                            </div>
                        </div>

                    </div>
                </p>
            </div>
        </div>

        <div class="extra content">
            <div class="ui icon buttons left floated mini">
                <button class="ui disabled button">
                    <i class="download cloud icon"></i>
                </button>
                <button class="ui disabled button">
                    <i class="database icon"></i>
                </button>
            </div>
            <div class="ui icon buttons right floated mini">
                <button class="ui button"><i class="angle double right icon"></i></button>
            </div>
        </div>
    </div>
</template>

<script>
import axios from 'axios'
import bus from './bus.js'

export default {
  name: 'build',
  props: ['build'],
  mounted() {
      $('.menu .item')
        .tab()
        ;
  },
  methods: {
    displayError : function() {
      var errs = [];
      if (this.build.jobs) {
          var last = this.build.jobs[this.build.jobs.length-1];
          if(last.err)
              errs.push(`Step '${last.step}' failed: ${last.err}`);
      }
      return errs.join("<br>");
    },
  },
}
</script>

<style>
  @keyframes pulse {
    0% {transform: scale(1, 1);}
    50% {transform: scale(1.2, 1.2);}
    100% {transform: scale(1, 1);}
  }

  .pulsing {
    animation: pulse 1s linear infinite;
  }

</style>
