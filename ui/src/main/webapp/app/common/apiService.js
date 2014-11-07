/**
 * Service for providing access to the backend API via HTTP.
 */

angular.module('pipelineAgentApp.common')
  .factory('api', function($rootScope, $http) {
    var apiBase = 'rest/v1',
      api = {events: {}};

    api.pipelineAgent = {

      getDefinitions: function() {
        var url = apiBase + '/definitions';
        return $http({
          method: 'GET',
          url: url
        });
      },

      getPipelineConfig: function() {
        var url = apiBase + '/pipelines/xyz';
        return $http({
          method: 'GET',
          url: url
        });
      },

      getPipelineConfigInfo: function() {
        var url = apiBase + '/pipelines/xyz?get=info';
        return $http({
          method: 'GET',
          url: url
        });
      },

      savePipelineConfig: function(config) {
        var url = apiBase + '/pipelines/xyz';
        return $http({
          method: 'POST',
          url: url,
          data: config
        });
      },

      previewPipeline: function() {
        var url = apiBase + '/pipelines/xyz/preview';
        return $http({
          method: 'GET',
          url: url
        });
      }

    };

    return api;
  });