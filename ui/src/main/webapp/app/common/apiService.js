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
      }

    };

    return api;
  });