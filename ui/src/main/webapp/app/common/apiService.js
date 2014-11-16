/**
 * Service for providing access to the backend API via HTTP.
 */

angular.module('pipelineAgentApp.common')
  .factory('api', function($rootScope, $http) {
    var apiBase = 'rest/v1',
      api = {events: {}};

    api.pipelineAgent = {

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

        url = apiBase + '/pipelines/' + name;
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

        url = apiBase + '/pipelines/' + name + '?get=info';
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

        url = apiBase + '/pipelines/' + name;
        return $http({
          method: 'POST',
          url: url,
          data: config
        });
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

        url = apiBase + '/pipelines/' + name + '/preview?sourceOffset=' + sourceOffset +
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

        url = apiBase + '/pipelines/' + name + '/preview?stageInstance=' + stageInstanceName + '&rev=0';

        return $http({
          method: 'POST',
          url: url,
          data: inputRecords
        });
      }

    };

    return api;
  });