/**
 * Service for providing access to the backend API via HTTP.
 */

angular.module('pipelineAgentApp.common')
  .factory('api', function($rootScope, $http, $q) {
    var apiBase = 'rest/v1',
      api = {events: {}};

    api.pipelineAgent = {

      /**
       * Fetches JVM Metrics
       * @returns {*}
       */
      getJVMMetrics: function() {
        var url = apiBase + '/admin/jvm-metrics';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetches Server Log
       */
      getServerLog: function() {
        var url = apiBase + '/admin/log';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetches UI Configuration from dist/src/main/etc/pipeline.properties
       *
       * @returns {*}
       */
      getConfiguration: function() {
        var url = apiBase + '/configuration/ui';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetches all configuration definitions of Pipeline and Stage Configuration.
       *
       * @returns {*}
       */
      getDefinitions: function() {
        var url = apiBase + '/definitions';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetches all Pipeline Configuration Info.
       *
       * @returns {*}
       */
      getPipelines: function() {
        var url = apiBase + '/pipeline-library';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetches Pipeline Configuration.
       *
       * @param name
       * @returns {*}
       */
      getPipelineConfig: function(name) {
        var url;

        if(!name) {
          name = 'xyz';
        }

        url = apiBase + '/pipeline-library/' + name;
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Fetches Pipeline Configuration Information
       *
       * @param name
       * @returns {*}
       */
      getPipelineConfigInfo: function(name) {
        var url;

        if(!name) {
          name = 'xyz';
        }

        url = apiBase + '/pipeline-library/' + name + '?get=info';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Sends updated Pipeline configuration to server for update.
       *
       * @param name - Pipeline Name
       * @param config - Modified Pipeline Configuration
       * @returns Updated Pipeline Configuration
       */
      savePipelineConfig: function(name, config) {
        var url;

        if(!name) {
          name = 'xyz';
        }

        url = apiBase + '/pipeline-library/' + name;
        return $http({
          method: 'POST',
          url: url,
          data: config
        });
      },

      /**
       * Create new Pipeline Configuration.
       *
       * @param name
       * @param description
       */
      createNewPipelineConfig: function(name, description) {
        var url = apiBase + '/pipeline-library/' + name + '?description=' + description;

        return $http({
          method: 'PUT',
          url: url
        });
      },

      /**
       * Delete Pipeline Cofiguration.
       *
       * @param name
       * @returns {*}
       */
      deletePipelineConfig: function(name) {
        var url = apiBase + '/pipeline-library/' + name;

        return $http({
          method: 'DELETE',
          url: url
        });
      },


      duplicatePipelineConfig: function(name, description, pipelineInfo) {
        var deferred = $q.defer(),
          pipelineObject,
          duplicatePipelineObject;

        // Fetch the pipelineInfo full object
        // then Create new config object
        // then copy the configuration from pipelineInfo to new Object.
        api.pipelineAgent.getPipelineConfig(pipelineInfo.name)
          .then(function(res) {
            pipelineObject = res.data;
            return api.pipelineAgent.createNewPipelineConfig(name, description);
          })
          .then(function(res) {
            duplicatePipelineObject = res.data;
            duplicatePipelineObject.configuration = pipelineObject.configuration;
            duplicatePipelineObject.uiInfo = pipelineObject.uiInfo;
            duplicatePipelineObject.stages = pipelineObject.stages;
            return api.pipelineAgent.savePipelineConfig(name, duplicatePipelineObject);
          })
          .then(function(res) {
            deferred.resolve(res.data);
          });

        return deferred.promise;
      },


      /**
       * Export Pipeline Configuration.
       *
       * @param name
       */
      exportPipelineConfig: function(name) {
        var url;

        if(!name) {
          name = 'xyz';
        }

        url = apiBase + '/pipeline-library/' + name + '?attachment=true';

        window.open(url, '_blank', '');
      },

      /**
       * Fetches Preview Data for Pipeline
       *
       * @param name
       * @param sourceOffset
       * @param batchSize
       * @param rev
       * @returns {*}
       */
      previewPipeline: function(name, sourceOffset, batchSize, rev) {
        var url;

        if(!name) {
          name = 'xyz';
        }

        if(!sourceOffset) {
          sourceOffset = 0;
        }

        if(!batchSize) {
          batchSize = 10;
        }

        url = apiBase + '/pipeline-library/' + name + '/preview?sourceOffset=' + sourceOffset +
          '&batchSize=' + batchSize + '&rev=' + rev;

        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Preview Stage by running with passed input records.
       *
       * @param name
       * @param stageInstanceName
       * @param inputRecords
       * @returns {*}
       */
      previewPipelineRunStage: function(name, stageInstanceName, inputRecords) {
        var url;

        if(!name) {
          name = 'xyz';
        }

        url = apiBase + '/pipeline-library/' + name + '/preview?stageInstance=' + stageInstanceName + '&rev=0';

        return $http({
          method: 'POST',
          url: url,
          data: inputRecords
        });
      },

      /**
       * Fetch the Pipeline Status
       *
       * @returns {*}
       */
      getPipelineStatus: function() {
        var url = apiBase + '/pipeline/status';
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Start the Pipeline
       *
       * @param name
       * @param rev
       * @returns {*}
       */
      startPipeline: function(name, rev) {
        var url = apiBase + '/pipeline/start?name=' + name + '&rev=' + rev ;
        return $http({
          method: 'POST',
          url: url
        });
      },

      /**
       * Stop the Pipeline
       *
       * @returns {*}
       */
      stopPipeline: function() {
        var url = apiBase + '/pipeline/stop';
        return $http({
          method: 'POST',
          url: url
        });
      },

      /**
       * Fetch the Pipeline Metrics
       *
       * @returns {*}
       */
      getPipelineMetrics: function() {
        var url = apiBase + '/pipeline/metrics';
        return $http({
          method: 'GET',
          url: url
        });
      },


      /**
       * Capture Snapshot of running pipeline.
       *
       * @param batchSize
       * @returns {*}
       */
      captureSnapshot: function(batchSize) {
        var url = apiBase + '/pipeline/snapshot?batchSize=' + batchSize ;
        return $http({
          method: 'PUT',
          url: url
        });
      },

      /**
       * Get Status of Snapshot.
       *
       * @returns {*}
       */
      getSnapshotStatus: function() {
        var url = apiBase + '/pipeline/snapshot' ;
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Get captured snapshot for given pipeline name.
       *
       * @param name
       * @returns {*}
       */
      getSnapshot: function(name) {
        var url = apiBase + '/pipeline/snapshot/' + name ;
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Delete captured snapshot for given pipeline name.
       *
       * @param name
       * @returns {*}
       */
      deleteSnapshot: function(name) {
        var url = apiBase + '/pipeline/snapshot/' + name ;
        return $http({
          method: 'DELETE',
          url: url
        });
      },


      /**
       * Get error records for the given stage instance name of running pipeline if it is provided otherwise
       * return error records for the pipeline.
       *
       * @param stageInstanceName
       * @returns {*}
       */
      getErrorRecords: function(stageInstanceName) {
        var url = apiBase + '/pipeline/errorRecords?stageInstanceName=' + stageInstanceName;
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Get error messages for the given stage instance name of running pipeline if is provided otherwise
       * return error messages for the pipeline.
       *
       * @param stageInstanceName
       * @returns {*}
       */
      getErrorMessages: function(stageInstanceName) {
        var url = apiBase + '/pipeline/errorMessages?stageInstanceName=' + stageInstanceName;
        return $http({
          method: 'GET',
          url: url
        });
      },

      /**
       * Raw Source Preview
       *
       * @param name
       * @param rev
       * @param configurations
       * @returns {*}
       */
      rawSourcePreview: function(name, rev, configurations) {
        var url = apiBase + '/pipeline-library/' + name + '/rawSourcePreview?rev=' + rev;

        angular.forEach(configurations, function(config) {
          if(config.name && config.value) {
            url+= '&' + config.name + '=' + config.value;
          }
        });

        return $http({
          method: 'GET',
          url: url
        });
      },

      shutdownCollector: function(secret) {
        var url = apiBase + '/admin/shutdown?secret=' + secret;
        return $http({
          method: 'POST',
          url: url
        });
      }
    };

    return api;
  });