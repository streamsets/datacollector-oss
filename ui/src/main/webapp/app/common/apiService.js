/**
 * Service for providing access to the backend API via HTTP.
 */

angular.module('pipelineAgentApp.common')
  .factory('api', function($rootScope, $http, $q) {
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


      duplicatePipelineConfig: function(name, description, originalConfig) {
        var deferred = $q.defer();
        var url = apiBase + '/pipeline-library/' + name + '?description=' + description;

        //First create new config object and then copy the configuration from original configuration.
        $http({
          method: 'PUT',
          url: url
        }).then(function(res) {
          var newConfigObect = res.data;
          newConfigObect.configuration = originalConfig.configuration;
          newConfigObect.uiInfo = originalConfig.uiInfo;
          newConfigObect.stages = originalConfig.stages;

          return $http({
            method: 'POST',
            url: url,
            data: newConfigObect
          });
        }).then(function(res) {
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
      }

    };

    return api;
  });